"""Data-quality checks for the OHLCV raw zone, as pure functions.

Every rule here already exists inline in dags/ingest_ohlcv_daily.py and was
earned by a measurement or an incident — the ``Why:`` in each docstring cites
the ROADMAP decision-log entry. Extracting them changes *where* the rules live,
not what they are, so each test is a statement of current DAG behaviour.

Nothing in this module imports Airflow. Functions return verdicts; the DAG maps
verdicts to exceptions (see this package's ``__init__`` for the table).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any

import pandas as pd

from market_ops._scaffold import todo

# Pinned column order for the raw zone, copied from the DAG. The DAG should
# import this constant once wired; until then tests/quality/test_schema_parity.py
# fails if the two copies drift.
OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trade_count"]

# The declared grain of the raw table: one row per ticker per session.
GRAIN = ("date", "ticker")

# Polygon grouped-daily returns ~12,500 rows on a normal session. Anything under
# 8,000 is a truncated response, not a quiet day (decision log 2026-09-03).
MIN_MARKET_ROWS = 8000

# Under grouped-daily, a missing ticker means it didn't trade or the symbol is
# dead, so this gate measures ticker-list drift, not vendor reliability
# (decision log 2026-09-03).
COMPLETENESS_FAIL_PCT = 20.0

# In-task throttle loop bounds (decision log 2026-09-08).
DEFAULT_WAIT_SECONDS = 60
MAX_WAIT_SECONDS = 90


class VendorAction(StrEnum):
    PROCEED = "proceed"  # 200: parse the payload
    THROTTLED = "throttled"  # 429: wait in-task, then re-request
    RETRY = "retry"  # 5xx: transient; let Airflow retry with backoff
    FAIL_FAST = "fail_fast"  # 401/403/other: retrying cannot help


class PayloadVerdict(StrEnum):
    OK = "ok"
    MARKET_CLOSED = "market_closed"  # nothing to do: weekend or holiday
    TRUNCATED = "truncated"  # vendor defect: fail, don't write


@dataclass(frozen=True)
class Completeness:
    """How much of the requested ticker universe came back.

    ``requested`` is the count of *distinct* requested tickers, the denominator
    of ``pct_missing``.
    """

    requested: int
    missing: frozenset[str]
    pct_missing: float
    should_fail: bool


def classify_status(status_code: int) -> VendorAction:
    """Map a vendor HTTP status to what the task should do next.

    Rules:
        200        -> PROCEED
        429        -> THROTTLED
        401, 403   -> FAIL_FAST
        500-599    -> RETRY
        anything else (including other 2xx/3xx/4xx) -> FAIL_FAST

    Why: the retry decision belongs at the raise site, made from the status code
    rather than inferred from an error string (decision log 2026-09-02, "explicit
    HTTP status taxonomy"). A bad key stays bad, so 401/403 retries are pure
    waste; throttling clears within seconds, so it is handled inside the task
    rather than by a 2-minute Airflow retry (2026-09-08); 5xx is the vendor's
    problem and earns Airflow's exponential backoff.
    """
    todo("classify_status: map 200/429/401/403/5xx/other to VendorAction per the docstring table")


def throttle_wait_seconds(
    retry_after: str | None,
    *,
    fallback: int = DEFAULT_WAIT_SECONDS,
    cap: int = MAX_WAIT_SECONDS,
) -> int:
    """Seconds to sleep before re-requesting after a 429.

    Rules:
        - ``retry_after`` made only of digits (after stripping surrounding
          whitespace) -> that integer.
        - ``None``, empty, or anything non-numeric (an HTTP-date, "soon",
          "-5", "1.5") -> ``fallback``.
        - The result is never greater than ``cap``.

    Why: honour the vendor's ``Retry-After`` when it sends one, fall back to the
    60-second window Polygon's free tier resets on, and cap a single sleep at 90s
    so one bad header can't park a worker slot for an hour (decision log
    2026-09-08). Matches the DAG's
    ``min(int(retry_after) if retry_after.isdigit() else 60, 90)``, plus
    whitespace tolerance.
    """
    todo("throttle_wait_seconds: digits -> int, else fallback; cap the result")


def classify_payload(results: Sequence[Any] | None, *, min_rows: int = MIN_MARKET_ROWS) -> PayloadVerdict:
    """Classify the ``results`` list of a grouped-daily response.

    Rules:
        - ``None`` or empty -> MARKET_CLOSED
        - fewer than ``min_rows`` rows -> TRUNCATED
        - otherwise -> OK   (exactly ``min_rows`` is OK)

    Why: a weekend returns ``{"resultsCount": 0, "status": "OK"}`` with **no
    ``results`` key at all**, so the caller passes ``payload.get("results")`` and
    ``None`` must mean "market closed", not a crash (decision log 2026-09-03). The
    row floor catches a truncated response that still happens to contain every
    tracked ticker — the ticker-level gate can't see that; a market-size gate can.
    """
    todo("classify_payload: None/empty -> MARKET_CLOSED, < min_rows -> TRUNCATED, else OK")


def find_date_mismatches(df: pd.DataFrame, session_date: date) -> pd.DataFrame:
    """Rows whose ``date`` is not the requested session.

    Args:
        df: frame with at least ``date`` and ``ticker`` columns. ``date`` holds
            ``datetime.date`` objects (what the DAG produces from Polygon's
            epoch-ms ``t``) or ISO ``'YYYY-MM-DD'`` strings (what a parquet
            round-trip or a CSV may give back); both must work.
        session_date: the session the task requested.

    Returns:
        A frame with exactly the columns ``["date", "ticker"]`` holding the
        distinct mismatched pairs, index reset. Empty (same two columns) when
        every row matches.

    Why: this is the check that did not exist on 2026-08-30, when ``dt=2026-08-29``
    (a Saturday) was written with 73 rows dated 2026-08-28 and every other check
    passed. Nothing compared the response to the *request*. Returning the
    offending pairs rather than a bool lets the error name exactly what came back.
    """
    todo("find_date_mismatches: distinct (date, ticker) rows where date != session_date")


def assess_completeness(
    requested: Iterable[str],
    returned: Iterable[str],
    *,
    fail_pct: float = COMPLETENESS_FAIL_PCT,
) -> Completeness:
    """Compare the requested ticker universe with what came back.

    Rules:
        - Duplicates in either input are ignored (set semantics).
        - Tickers returned but not requested are ignored.
        - ``pct_missing = 100 * len(missing) / len(set(requested))``.
        - ``should_fail`` is ``pct_missing >= fail_pct`` (>=, as in the DAG).
        - Empty ``requested`` raises ``ValueError``.

    Why: on 2026-09-01 a throttled response returned 28 of 73 tickers, the task
    went green, and a complete partition was overwritten — the incident that
    added this gate. Under grouped-daily a partial *response* can no longer
    happen, so the gate now measures list drift: warn on any miss, fail only when
    a fifth of the universe is gone (decision log 2026-09-03). An empty request
    list raises because writing an empty partition looks exactly like "market
    closed" to every consumer — fail loudly at the boundary instead.
    """
    todo("assess_completeness: set difference, pct of distinct requested, should_fail at >= fail_pct")


def validate_partition_frame(df: pd.DataFrame, partition_date: date | str) -> list[str]:
    """Check a partition's contents. Returns problems; ``[]`` means valid.

    A pure function of the partition: it needs nothing but the frame and the
    ``dt=`` date the file is filed under. Problems reported, one message each:

        1. the frame is empty
        2. any row's ``date`` differs from ``partition_date`` (message includes
           how many rows)
        3. duplicate ``(date, ticker)`` rows — the declared grain
        4. null ``close`` values

    When the frame is empty, report only problem 1.
    ``partition_date`` accepts a ``date`` or an ISO ``'YYYY-MM-DD'`` string;
    ``df["date"]`` may hold ``date`` objects or ISO strings.

    Why: a different threat model from the checks in ``fetch_ohlcv``. Those
    validate the vendor exchange; this validates the artifact on disk, however it
    got there — which is why it must not check completeness (that needs the
    ticker list, i.e. external state, and belongs to fetch — decision log
    2026-09-03). A duplicate on the grain silently inflates every downstream
    aggregate; a null close on a trading day has no valid explanation.
    """
    todo("validate_partition_frame: empty / date != partition / duplicate grain / null close -> messages")


def partition_date_from_path(path: str | Path) -> date:
    """The session date a raw-zone path is filed under.

    Accepts the file (``.../dt=2026-09-02/ohlcv.parquet``) or the directory
    (``.../dt=2026-09-02``). Raises ``ValueError`` when neither the path's own
    name nor its parent's is ``dt=YYYY-MM-DD`` with a real calendar date.

    Why: the partition key is the contract downstream readers prune on. Parsing
    it strictly — ``dt=2026-02-30`` is an error, not a date — is how the
    date-invariant check knows what the file *claims* to hold.
    """
    todo("partition_date_from_path: parse dt=YYYY-MM-DD from the path or its parent; ValueError otherwise")
