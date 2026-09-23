"""Spec for market_ops.quality.checks.

Each table row is a statement of how the ingestion DAG behaves today. While a
function is still a ``todo()`` stub, its tests report as TODO skips.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from market_ops.quality.checks import (
    COMPLETENESS_FAIL_PCT,
    MIN_MARKET_ROWS,
    PayloadVerdict,
    VendorAction,
    assess_completeness,
    classify_payload,
    classify_status,
    find_date_mismatches,
    partition_date_from_path,
    throttle_wait_seconds,
    validate_partition_frame,
)

UNIVERSE_73 = [f"T{i:02d}" for i in range(73)]


# --- classify_status -------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (200, VendorAction.PROCEED),
        (429, VendorAction.THROTTLED),
        (401, VendorAction.FAIL_FAST),
        (403, VendorAction.FAIL_FAST),
        (500, VendorAction.RETRY),
        (502, VendorAction.RETRY),
        (503, VendorAction.RETRY),
        (599, VendorAction.RETRY),
        # Everything outside the taxonomy fails fast: retrying an unexpected
        # status just burns the retry budget on a request that won't change.
        (201, VendorAction.FAIL_FAST),
        (302, VendorAction.FAIL_FAST),
        (400, VendorAction.FAIL_FAST),
        (404, VendorAction.FAIL_FAST),
        (499, VendorAction.FAIL_FAST),
        (600, VendorAction.FAIL_FAST),
    ],
)
def test_classify_status(code, expected):
    assert classify_status(code) is expected


# --- throttle_wait_seconds -------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("30", 30),
        ("90", 90),
        ("120", 90),  # capped: one header can't park a worker for minutes
        ("0", 0),
        (" 45 ", 45),  # surrounding whitespace tolerated
        (None, 60),  # no header -> the 60s window Polygon's free tier resets on
        ("", 60),
        ("soon", 60),
        ("Wed, 21 Oct 2026 07:28:00 GMT", 60),  # HTTP-date form: not honoured
        ("-5", 60),
        ("1.5", 60),
    ],
)
def test_throttle_wait_seconds(header, expected):
    assert throttle_wait_seconds(header) == expected


def test_throttle_wait_respects_custom_cap_and_fallback():
    assert throttle_wait_seconds("30", cap=20) == 20
    assert throttle_wait_seconds(None, fallback=10) == 10
    # The fallback is capped too.
    assert throttle_wait_seconds(None, fallback=120, cap=90) == 90


# --- classify_payload ------------------------------------------------------


def _rows(n: int) -> list[dict]:
    return [{"T": f"X{i}"} for i in range(n)]


def test_weekend_payload_has_no_results_key():
    # Captured weekend response: {"resultsCount": 0, "status": "OK"} and nothing
    # else. The caller passes payload.get("results"), i.e. None.
    payload = {"resultsCount": 0, "status": "OK"}
    assert classify_payload(payload.get("results")) is PayloadVerdict.MARKET_CLOSED


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (0, PayloadVerdict.MARKET_CLOSED),
        (1, PayloadVerdict.TRUNCATED),
        (MIN_MARKET_ROWS - 1, PayloadVerdict.TRUNCATED),
        (MIN_MARKET_ROWS, PayloadVerdict.OK),
        (12_541, PayloadVerdict.OK),  # the captured normal trading-day response
    ],
)
def test_classify_payload_by_size(n, expected):
    assert classify_payload(_rows(n)) is expected


def test_classify_payload_custom_floor():
    assert classify_payload(_rows(2), min_rows=2) is PayloadVerdict.OK
    assert classify_payload(_rows(1), min_rows=2) is PayloadVerdict.TRUNCATED


# --- find_date_mismatches --------------------------------------------------


def test_find_date_mismatches_all_match_returns_empty_frame(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL", "MSFT"])
    out = find_date_mismatches(df, date(2026, 9, 2))
    assert out.empty
    assert list(out.columns) == ["date", "ticker"]


def test_find_date_mismatches_partial(make_frame):
    df = make_frame(
        date(2026, 9, 2),
        ["AAPL", "MSFT", "NVDA"],
        date=[date(2026, 9, 2), date(2026, 9, 1), date(2026, 9, 1)],
    )
    out = find_date_mismatches(df, date(2026, 9, 2))
    assert sorted(out["ticker"]) == ["MSFT", "NVDA"]
    assert set(out["date"].astype(str)) == {"2026-09-01"}


def test_find_date_mismatches_returns_distinct_pairs(make_frame):
    df = make_frame(date(2026, 9, 1), ["AAPL", "AAPL"])
    out = find_date_mismatches(df, date(2026, 9, 2))
    assert len(out) == 1


def test_find_date_mismatches_accepts_iso_strings(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL", "MSFT"], date=["2026-09-02", "2026-09-01"])
    out = find_date_mismatches(df, date(2026, 9, 2))
    assert list(out["ticker"]) == ["MSFT"]


# --- assess_completeness ---------------------------------------------------


def test_complete_universe():
    c = assess_completeness(UNIVERSE_73, UNIVERSE_73)
    assert c.missing == frozenset()
    assert c.pct_missing == 0.0
    assert c.should_fail is False
    assert c.requested == 73


def test_one_missing_warns_but_does_not_fail():
    c = assess_completeness(UNIVERSE_73, UNIVERSE_73[1:])
    assert c.missing == {UNIVERSE_73[0]}
    assert c.pct_missing == pytest.approx(100 / 73)  # 1.37%
    assert c.should_fail is False


def test_threshold_is_inclusive():
    # 1 of 5 missing is exactly 20.0% -> fails, because the DAG uses >=.
    c = assess_completeness(list("ABCDE"), list("ABCD"))
    assert c.pct_missing == pytest.approx(COMPLETENESS_FAIL_PCT)
    assert c.should_fail is True


def test_duplicates_and_extras_are_ignored():
    c = assess_completeness(["A", "A", "B"], ["A", "Z"])
    assert c.requested == 2
    assert c.missing == {"B"}
    assert c.pct_missing == pytest.approx(50.0)


def test_custom_fail_pct():
    c = assess_completeness(list("ABCD"), list("ABC"), fail_pct=50.0)
    assert c.pct_missing == pytest.approx(25.0)
    assert c.should_fail is False


def test_empty_request_list_raises():
    with pytest.raises(ValueError):
        assess_completeness([], ["AAPL"])


# --- validate_partition_frame ----------------------------------------------


def test_valid_partition_has_no_problems(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL", "MSFT"])
    assert validate_partition_frame(df, date(2026, 9, 2)) == []
    assert validate_partition_frame(df, "2026-09-02") == []


def test_valid_partition_with_string_dates(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL"], date=["2026-09-02"])
    assert validate_partition_frame(df, "2026-09-02") == []


def test_empty_partition_reports_exactly_one_problem(make_frame):
    df = make_frame(date(2026, 9, 2), [])
    assert len(validate_partition_frame(df, "2026-09-02")) == 1


def test_duplicate_grain(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL", "AAPL", "MSFT"])
    assert len(validate_partition_frame(df, "2026-09-02")) == 1


def test_null_close(make_frame):
    df = make_frame(date(2026, 9, 2), ["AAPL", "MSFT"], close=[100.5, None])
    assert len(validate_partition_frame(df, "2026-09-02")) == 1


def test_every_problem_is_reported(make_frame):
    # Wrong date on every row + a duplicate + a null close: three messages.
    df = make_frame(date(2026, 9, 1), ["AAPL", "AAPL", "MSFT"], close=[1.0, 1.0, None])
    assert len(validate_partition_frame(df, "2026-09-02")) == 3


# --- partition_date_from_path ----------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        Path("data/raw/ohlcv/dt=2026-09-02/ohlcv.parquet"),
        Path("data/raw/ohlcv/dt=2026-09-02"),
        "data/raw/ohlcv/dt=2026-09-02/ohlcv.parquet",
        Path("/opt/airflow/data/raw/ohlcv/dt=2026-09-02/ohlcv.parquet"),
    ],
)
def test_partition_date_from_path(path):
    assert partition_date_from_path(path) == date(2026, 9, 2)


@pytest.mark.parametrize(
    "path",
    [
        "data/raw/ohlcv/2026-09-02/ohlcv.parquet",  # no dt= prefix
        "data/raw/ohlcv/dt=2026-02-30/ohlcv.parquet",  # not a real date
        "data/raw/ohlcv/dt=20260902/ohlcv.parquet",  # not ISO
        "ohlcv.parquet",
    ],
)
def test_partition_date_from_bad_path_raises(path):
    with pytest.raises(ValueError):
        partition_date_from_path(path)


# --- regressions, named for the incidents they guard -----------------------


def test_incident_2026_08_30_saturday_partition_with_friday_rows(make_frame):
    """dt=2026-08-29 (a Saturday) held 73 rows dated 2026-08-28 and every check
    passed, because every check compared the data to itself."""
    df = make_frame(date(2026, 8, 28), UNIVERSE_73)

    problems = validate_partition_frame(df, "2026-08-29")
    assert len(problems) == 1
    assert "73" in problems[0]  # the message says how many rows are wrong

    mismatches = find_date_mismatches(df, date(2026, 8, 29))
    assert len(mismatches) == 73


def test_incident_2026_09_01_partial_response_28_of_73():
    """A throttled response returned 28 of 73 tickers and overwrote a complete
    partition while the task stayed green."""
    c = assess_completeness(UNIVERSE_73, UNIVERSE_73[:28])
    assert len(c.missing) == 45
    assert c.pct_missing == pytest.approx(61.64, abs=0.01)  # 45 / 73
    assert c.should_fail is True
