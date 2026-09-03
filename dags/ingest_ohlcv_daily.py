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

log = logging.getLogger(__name__)

RAW_ROOT = Path(os.getenv("RAW_ZONE_PATH", "/opt/airflow/data/raw"))
# Fixed column order pinned here rather than inferred from the vendor response.
# If yfinance adds, removes, or reorders a column, the reindex below normalizes
# it instead of silently changing the parquet schema under downstream readers.
OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]


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
    catchup=False,

    # Bounds scheduler-created runs only; backfills carry their own limit (default 10).
    max_active_runs=1,
    default_args={

        # Market data APIs fail transiently: rate limits, 5xx, connection resets.
        # Retries are safe *because* the task is idempotent — this is the payoff
        # for the write-then-rename design below.
        "retries": 0,
        "retry_delay": timedelta(minutes=2),

        # Backs off 2m, 4m, 8m rather than retrying into an active rate limit.
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["ingestion", "raw", "market-data"],
)
def ingest_ohlcv_daily():
    #pool="yfinance" serializes vendor calls since max_active_runs doesnt apply to backfills.
    # execution_timeout bounds a hung HTTP call. Without it a stuck task holds a
    # worker slot indefinitely and blocks the pool — a failed task is recoverable,
    # a hung one silently starves the whole scheduler.
    @task(pool="yfinance", execution_timeout=timedelta(minutes=15))
    def fetch_ohlcv() -> str:

        # Imported inside the task, not at module top level. Top-level imports of
        # heavy third-party libraries run on every DAG-file parse in the
        # dag-processor, not just at execution time.
        import pandas as pd
        import yfinance as yf

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

        raw = yf.download(
            tickers,
            start=session_date.isoformat(),
            # yfinance treats `end` as exclusive, so +1 day yields a single session.
            end=(session_date + timedelta(days=1)).isoformat(),
            group_by="ticker",

            # Keep both close and adj_close. Adjusted prices are retroactively            
            # rewritten by splits and dividends; storing the unadjusted close means
            # we can always reconstruct what the vendor said on the day.
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )

        # Reshape vendor output (wide, one column block per ticker) into long
        # format (one row per date/ticker). Done with explicit column slicing
        # rather than DataFrame.stack() because stack()'s MultiIndex semantics
        # changed in pandas 2.1 and again in 3.0; slicing is version-stable.
        frames = []
        for ticker in tickers:

            # yfinance returns flat columns for a single ticker, MultiIndex for many.
            if isinstance(raw.columns, pd.MultiIndex):
                # A delisted or misspelled ticker is absent entirely. Skip it here
                # and let validate_partition report it, so one bad symbol cannot
                # fail the whole batch.
                if ticker not in raw.columns.get_level_values(0):
                    continue
                sub = raw[ticker]
            else:
                sub = raw
            sub = sub.dropna(how="all").reset_index()
            if sub.empty:
                continue
            sub["ticker"] = ticker
            frames.append(sub)

        # Weekends and market holidays legitimately return nothing. Skipped is the
        # honest state: not success (no data was produced) and not failure (nothing
        # is broken). Marking these as failures would train you to ignore red DAGs.
        if not frames:
            raise AirflowSkipException(f"No bars returned for {session_date}; market likely closed due to being a weekend or market holiday")

        frame = pd.concat(frames, ignore_index=True)
        # Normalize vendor casing/spacing: "Adj Close" -> "adj_close".
        frame.columns = [str(c).strip().lower().replace(" ", "_") for c in frame.columns]
        frame = frame.rename(columns={"index": "date", "datetime": "date"})

        # Store a plain date, not a timestamp. The bar's grain is one day; keeping
        # a tz-aware midnight timestamp invites off-by-one joins across timezones.
        frame["date"] = pd.to_datetime(frame["date"]).dt.date

        # Enforce the pinned schema. reindex adds missing columns as null and drops
        # unexpected ones, so the parquet schema is stable across vendor changes.
        frame = frame.reindex(columns=OHLCV_COLUMNS)
        
        # Validate that the vendor returned the requested session date. Skip and succeed if all dates are mismatched (weekend/holiday) but fail if only some are mismatched (Vendor issue).
        mismatched = frame["date"] != session_date
        if mismatched.all():
            raise AirflowSkipException(f"All rows returned have a mismatched date; {session_date} was requested but {frame['date'].unique()} is being returned instead. market likely closed due to being a weekend or market holiday")
        elif mismatched.any():
            raise AirflowFailException(f"{mismatched.sum()} of {len(frame)} rows returned have a mismatched date. Mismatched tickers:{frame.loc[mismatched, 'ticker'].unique()}")

        #validate that all requested tickers were returned from vendor. fail if missing ticker percentage >= 15%, alert otherwise.
        missing = set(tickers) - set(frame["ticker"].unique())
        if len(missing) > 0:
            count_missing = len(missing)
            percent_missing = (100 * count_missing) / len(tickers)
            log.warning(f"{percent_missing:.1f}% of requested tickers ({count_missing} of {len(tickers)}) are missing from the vendor response. missing tickers: {missing}")
            if percent_missing >= 15:
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
        frame.to_parquet(staging, index=False)
        staging.replace(target)

        log.info("Wrote %s rows for %s tickers to %s", len(frame), frame["ticker"].nunique(), target)
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

        frame = pd.read_parquet(path)

        if frame.empty:
            raise ValueError(f"{path} is empty")
        
        #Validate the file on disk to make sure partition date and the row dates within are matching.
        part_date = Path(path).parent.name.removeprefix("dt=")
        mismatch = frame["date"].astype(str) != part_date
        if mismatch.any():
            raise AirflowFailException(f"Partition date is for {part_date} but dates for {mismatch.sum()} of {len(frame)} rows are mismatched: {frame.loc[mismatch, 'date'].unique()}")
            
        # (date, ticker) is the declared grain of this table. A duplicate means the
        # reshape logic double-counted, which would silently inflate every
        # downstream aggregate — exactly the failure mode that produces a "the
        # numbers look wrong" ticket weeks later.
        duplicates = frame.duplicated(subset=["date", "ticker"]).sum()
        if duplicates:
            raise ValueError(f"{duplicates} duplicate (date, ticker) rows in {path}")

        # close is the one column with no valid reason to be null on a trading day.
        nulls = frame["close"].isna().sum()
        if nulls:
            raise ValueError(f"{nulls} rows with null close in {path}")

        log.info("Validated %s rows across %s tickers", len(frame), frame["ticker"].nunique())

    # Passing the return value establishes the dependency implicitly. Under the
    # TaskFlow API this is both the data flow (the XCom'd path) and the execution
    # order — no separate `>>` needed, and the two can never drift apart.
    validate_partition(fetch_ohlcv())


# Calling the decorated function is what registers the DAG. Without this line the
# file parses cleanly and the DAG simply never appears in the UI.
ingest_ohlcv_daily()

