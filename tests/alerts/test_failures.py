"""Spec for market_ops.alerts.failures."""

from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from market_ops.agent.schemas import RunSignal, SignalKind
from market_ops.alerts.failures import build_failure_signal, on_task_failure
from market_ops.alerts.signals_io import read_signals

NOW = datetime(2026, 9, 18, 6, 2, tzinfo=UTC)
RUN_ID = "scheduled__2026-09-18T06:00:00+00:00"


class AirflowFailException(Exception):
    """Stand-in with the real class name; the signal records the name only."""


def failure_context(exception=None, **overrides) -> dict:
    ti = SimpleNamespace(dag_id="ingest_ohlcv_daily", task_id="fetch_ohlcv", run_id=RUN_ID, try_number=1)
    ctx = {
        "ti": ti,
        "task_instance": ti,
        "run_id": RUN_ID,
        "data_interval_start": datetime(2026, 9, 17, 6, 0, tzinfo=UTC),
        "exception": exception,
    }
    ctx.update(overrides)
    return ctx


def test_build_failure_signal_from_exception_object():
    exc = AirflowFailException("Response code 401; Authentication/Permission Issue")
    s = build_failure_signal(failure_context(exc), now=NOW)
    assert s.kind is SignalKind.TASK_FAILED
    assert (s.dag_id, s.task_id, s.run_id, s.try_number) == ("ingest_ohlcv_daily", "fetch_ohlcv", RUN_ID, 1)
    assert s.session_date == date(2026, 9, 17)
    assert s.message == "Response code 401; Authentication/Permission Issue"
    assert s.attributes["exception_type"] == "AirflowFailException"
    assert s.signal_id == RunSignal.stable_id(
        SignalKind.TASK_FAILED, "ingest_ohlcv_daily", RUN_ID, "fetch_ohlcv", 1
    )


def test_string_exception_has_no_type_attribute():
    s = build_failure_signal(failure_context("boom"), now=NOW)
    assert s.message == "boom"
    assert "exception_type" not in s.attributes


def test_missing_exception_gives_empty_message():
    s = build_failure_signal(failure_context(None), now=NOW)
    assert s.message == ""


def test_long_exception_text_is_truncated_to_fit():
    s = build_failure_signal(failure_context(ValueError("x" * 10_000)), now=NOW)
    assert 0 < len(s.message) <= 4000


def test_each_try_is_its_own_signal():
    ctx2 = failure_context("boom")
    ctx2["ti"] = SimpleNamespace(**{**vars(ctx2["ti"]), "try_number": 2})
    a = build_failure_signal(failure_context("boom"), now=NOW)
    b = build_failure_signal(ctx2, now=NOW)
    assert a.signal_id != b.signal_id


def test_on_task_failure_writes_a_signal(tmp_path, monkeypatch):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    on_task_failure(failure_context(ValueError("45 of 73 tickers missing")))
    [signal] = read_signals(tmp_path / "signals")
    assert signal.task_id == "fetch_ohlcv"


def test_on_task_failure_never_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    assert on_task_failure({}) is None


@pytest.mark.parametrize("bad", [{"ti": None}, {"ti": SimpleNamespace()}])
def test_on_task_failure_swallows_malformed_contexts(tmp_path, monkeypatch, bad):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    assert on_task_failure(bad) is None
