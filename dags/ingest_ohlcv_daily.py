"""Daily OHLCV ingestion into the date-partitioned raw zone.

Contract
--------
For a given logical date D, this DAG writes exactly one file:

    {RAW_ZONE_PATH}/ohlcv/dt=D/ohlcv.parquet

The DAG is *idempotent*: running it twice for D produces a byte-equivalent
partition, never a duplicated one. It is *backfillable*: D is derived from the
run's data interval, so a run for a date three months ago behaves identically
to today's run.

Raw-zone rules observed here:
  - Land data as close to source-shape as possible. Cleaning belongs in dbt,
    downstream. If a transform is wrong, we re-derive from raw instead of
    re-fetching from a vendor who may no longer serve that history.
  - One partition = one atomic unit of work. Never append to an existing file.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Variable, BaseHook, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable
import requests

log = logging.getLogger(__name__)

RAW_ROOT = Path(os.getenv("RAW_ZONE_PATH", "/opt/airflow/data/raw"))
# Fixed column order pinned here rather than inferred from the vendor response.
# If vendor adds, removes, or reorders a column, the reindex below normalizes
# it instead of silently changing the parquet schema under downstream readers.
OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trade_count"]


def _partition_dir(logical_date: pendulum.DateTime) -> Path:
    """Hive-style partition path: raw/ohlcv/dt=2026-08-26/.

    The `dt=` prefix is not cosmetic. DuckDB, Spark, Athena, and BigQuery all
    parse it as a virtual column, so `WHERE dt = '2026-08-26'` prunes entire
    directories before reading a single byte. Date-first partitioning also makes
    a re-run target a deterministic path, which is what makes overwrite-based
    idempotency possible.
    """
    return RAW_ROOT / "ohlcv" / f"dt={logical_date.date().isoformat()}"


@dag(
    dag_id="ingest_ohlcv_daily",
    description="Pull daily OHLCV bars for the configured tickers into raw/ohlcv/dt=YYYY-MM-DD/",
    # Runs every day at 06:00 UTC, after the US session has settled.
    # Why daily rather than a weekdays-only "0 6 * * 2-6" cron: Airflow derives
    # data_interval_start from the *previous* scheduled fire time. Under a Tue-Sat
    # cron, Tuesday's run would have an interval starting the previous Saturday,
    # so we would fetch the wrong session date. A daily cron keeps the interval
    # exactly one day wide; non-trading days simply skip.
    schedule=CronTriggerTimetable("0 6 * * *", timezone="UTC", interval=timedelta(days=1)),

    # Anchors the schedule. Never use a dynamic value such as days_ago() or
    # datetime.now() here — the schedule would shift on every parse.
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),

    # Off until the Days 3-4 backfill exercise. Flipping this to True would
    # immediately queue every missed interval since start_date.
    catchup=True,

    # Bounds scheduler-created runs only; backfills carry their own limit (default 10).
    max_active_runs=1,
    default_args={

        # Market data APIs fail transiently: rate limits, 5xx, connection resets.
        # Retries are safe *because* the task is idempotent — this is the payoff
        # for the write-then-rename design below.
        "retries": 3,
        "retry_delay": timedelta(minutes=2),

        # Backs off 2m, 4m, 8m rather than retrying into an active rate limit.
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["ingestion", "raw", "market-data"],
)
def ingest_ohlcv_daily():
    #pool="polygon" serializes vendor calls since max_active_runs doesnt apply to backfills.
    # execution_timeout bounds a hung HTTP call. Without it a stuck task holds a
    # worker slot indefinitely and blocks the pool — a failed task is recoverable,
    # a hung one silently starves the whole scheduler.
    @task(pool="polygon", execution_timeout=timedelta(minutes=15))
    def fetch_ohlcv() -> str:

        # Imported inside the task, not at module top level. Top-level imports of
        # heavy third-party libraries run on every DAG-file parse in the
        # dag-processor, not just at execution time.
        import pandas as pd

        # Both reads happen here rather than at module scope: in Airflow 3 task
        # code has no DB access and resolves these over the Task Execution API,
        # so a top-level read would cost the dag-processor a round trip per parse.
        tickers = Variable.get("tickers", deserialize_json=True)   
        conn = BaseHook.get_connection("polygon_default")
        base_url = f"{conn.schema}://{conn.host}"

        # Fail loudly at the boundary rather than writing an empty partition that
        # looks like "market closed" to every downstream consumer.
        if not tickers:
            raise ValueError("tickers is empty")

        ctx = get_current_context()

        # THE critical line for idempotency and backfill. The session date comes
        # from the run's data interval, never from datetime.now(). A run for
        # 2026-06-01 fetches 2026-06-01 whether it executes on time or three
        # months late, which is precisely what makes the task a pure function of
        # its interval.
        session_date = ctx["data_interval_start"].date()

        #Create url, params, and headers for the api request to the vendor. Session date is passed in the url path to ensure that the correct date is returned from the vendor.
        url = f"{base_url}/v2/aggs/grouped/locale/us/market/stocks/{session_date}"
        params = {"adjusted": "false"}
        headers = {"Authorization": f"Bearer {conn.password}"}

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response_code = response.status_code

        #Validate the status code recieved from the vendors response
        if response_code in [401, 403]:
            raise AirflowFailException(f"Response code {response_code}; Authentication/Permission Issue")

        elif response_code == 429:
            raise requests.HTTPError(f"Response code {response_code}; retrying")
        
        elif 500 <= response_code < 600:
            raise requests.HTTPError(f"Response code {response_code}; retrying")

        else:
            if response_code != 200:
                raise AirflowFailException(f"Response code {response_code}; Unexpected error, check the vendor's status page or try again later.")

        payload = response.json()
        results = payload.get("results")

        #Validate that the json payload is not empty, and contains a reasonable number of tickers in the "results" key (>12,000 tickers expected so less than 80000 is very unusual). Skip if empty (market holiday/weekend likely); Fail if less than 8000.
        if not results:
            raise AirflowSkipException(f"No bars returned for {session_date}; market likely closed due to being a weekend or market holiday")

        if len(results) < 8000:
            raise AirflowFailException(f"Vendor returned {len(results)} rows; Possible vendor issue, Check the vendor's status page or try again later.")

        #convert vendor json response to pandas dataframe, contains all tickers from the market.  
        df_market = pd.DataFrame(results)

        #filter df_market for rows where "T" is in our tickerlist, 
        df_tick = df_market[df_market['T'].isin(tickers)].copy()

        # Rename vendor columns
        df_tick = df_tick.rename(columns={"T": "ticker", "v": "volume", "vw":"vwap","o":"open","c":"close","h":"high","l":"low","t": "date", "n":"trade_count"})

        #validate that the date in every row matches session_date. Fail if there are any mismatches.
        df_tick["date"] = pd.to_datetime(df_tick["date"], unit="ms").dt.date
        mismatched = df_tick["date"] != session_date
        if mismatched.any():
            mismatches = df_tick.loc[mismatched, ["date","ticker"]].drop_duplicates()
            raise AirflowFailException(f"Some rows returned have a mismatched date; {session_date} was requested but below dates are being returned instead: {mismatches}")

        # Enforce the pinned schema. reindex adds missing columns as null and drops
        # unexpected ones, so the parquet schema is stable across vendor changes.
        df_tick = df_tick.reindex(columns=OHLCV_COLUMNS)
        
        #validate that all requested tickers were returned from vendor. fail if missing ticker percentage >= 20%, alert otherwise.
        missing = set(tickers) - set(df_tick["ticker"].unique())
        if len(missing) > 0:
            count_missing = len(missing)
            percent_missing = (100 * count_missing) / len(tickers)
            log.warning(f"{percent_missing:.1f}% of requested tickers ({count_missing} of {len(tickers)}) are missing from the vendor response. missing tickers: {missing}")
            if percent_missing >= 20:
                raise ValueError(f"{percent_missing:.1f}% of requested tickers are missing ({count_missing} of {len(tickers)}) from the vendor response. missing tickers: {missing}")

        partition = _partition_dir(ctx["data_interval_start"])

        # exist_ok=True keeps a re-run from failing on its own prior directory.
        partition.mkdir(parents=True, exist_ok=True)
        target = partition / "ohlcv.parquet"

        # Write-then-rename. Path.replace() is an atomic rename within a single
        # filesystem, so a reader either sees the previous complete file or the new
        # complete file, never a partial one. Two consequences worth stating in an
        # interview:
        #   1. Crash safety — a container killed mid-write leaves only a .tmp file.
        #   2. Idempotency — a re-run overwrites a deterministic path instead of
        #      appending, so N runs of the same interval yield one partition.
        # This is why the retry policy above is safe to be aggressive.
        staging = target.with_suffix(".parquet.tmp")
        df_tick.to_parquet(staging, index=False)
        staging.replace(target)

        log.info("Wrote %s rows for %s tickers to %s", len(df_tick), df_tick["ticker"].nunique(), target)
        # Return the path, not the DataFrame. TaskFlow return values become XComs,
        # which are stored in the Airflow metadata DB — a small string reference is
        # appropriate, a serialized DataFrame is the classic XCom antipattern.
        return str(target)

    @task
    def validate_partition(path: str) -> None:
        """
        validate the artifact.

        Kept as a separate task so a data-quality failure surfaces as its own red
        node in the UI. Folding these checks into fetch_ohlcv would make "the API
        was down" and "the data was wrong" indistinguishable at a glance, and would
        force a re-fetch just to re-check.
        """
        import pandas as pd

        df_part = pd.read_parquet(path)

        if df_part.empty:
            raise ValueError(f"{path} is empty")
        
        #Validate the file on disk to make sure partition date and the row dates within are matching.
        part_date = Path(path).parent.name.removeprefix("dt=")
        mismatch = df_part["date"].astype(str) != part_date
        if mismatch.any():
            mismatches = df_part.loc[mismatch, 'date'].unique()
            raise AirflowFailException(f"Partition date is for {part_date} but dates for {mismatch.sum()} of {len(df_part)} rows are mismatched: {mismatches}")
            
        # (date, ticker) is the declared grain of this table. A duplicate means the
        # reshape logic double-counted, which would silently inflate every
        # downstream aggregate — exactly the failure mode that produces a "the
        # numbers look wrong" ticket weeks later.
        duplicates = df_part.duplicated(subset=["date", "ticker"]).sum()
        if duplicates:
            raise ValueError(f"{duplicates} duplicate (date, ticker) rows in {path}")

        # close is the one column with no valid reason to be null on a trading day.
        nulls = df_part["close"].isna().sum()
        if nulls:
            raise ValueError(f"{nulls} rows with null close in {path}")

        log.info("Validated %s rows across %s tickers", len(df_part), df_part["ticker"].nunique())

    # Passing the return value establishes the dependency implicitly. Under the
    # TaskFlow API this is both the data flow (the XCom'd path) and the execution
    # order — no separate `>>` needed, and the two can never drift apart.
    validate_partition(fetch_ohlcv())


# Calling the decorated function is what registers the DAG. Without this line the
# file parses cleanly and the DAG simply never appears in the UI.
ingest_ohlcv_daily()

