"""Spec for market_ops.alerts.deadlines."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import pytest

from market_ops.agent.schemas import RunSignal, SignalKind
from market_ops.alerts.deadlines import (
    CALLBACK_PATH,
    DEADLINE_INTERVAL,
    build_deadline_signal,
    make_deadline_alert,
    on_deadline_missed,
)
from market_ops.alerts.signals_io import read_signals

NOW = datetime(2026, 9, 18, 6, 40, tzinfo=UTC)
RUN_ID = "scheduled__2026-09-18T06:00:00+00:00"


def deadline_context(**dag_run_overrides) -> dict:
    """Shaped like models/deadline.py::handle_miss builds it: the DagRun's REST
    representation (ISO strings) plus the deadline row."""
    dag_run = {
        "dag_run_id": RUN_ID,
        "dag_id": "ingest_ohlcv_daily",
        "logical_date": "2026-09-18T06:00:00Z",
        "queued_at": "2026-09-18T06:00:03.512Z",
        "data_interval_start": "2026-09-17T06:00:00Z",
        "data_interval_end": "2026-09-18T06:00:00Z",
        "state": "running",
    }
    dag_run.update(dag_run_overrides)
    return {"dag_run": dag_run, "deadline": {"id": "0199", "deadline_time": "2026-09-18T06:30:03.512Z"}}


def test_build_deadline_signal():
    s = build_deadline_signal(deadline_context(), now=NOW)
    assert s.kind is SignalKind.DEADLINE_MISSED
    assert s.dag_id == "ingest_ohlcv_daily"
    assert s.run_id == RUN_ID
    assert s.task_id is None
    # Same derivation as the DAG's partition key: data_interval_start's date.
    assert s.session_date == date(2026, 9, 17)
    assert s.detected_at == NOW
    assert s.message
    assert "deadline_time" in s.attributes


def test_deadline_signal_id_is_stable_per_run():
    first = build_deadline_signal(deadline_context(), now=NOW)
    again = build_deadline_signal(deadline_context(), now=datetime(2026, 9, 18, 7, 0, tzinfo=UTC))
    assert first.signal_id == again.signal_id
    assert first.signal_id == RunSignal.stable_id(SignalKind.DEADLINE_MISSED, "ingest_ohlcv_daily", RUN_ID)


def test_deadline_signal_without_interval_has_no_session_date():
    s = build_deadline_signal(deadline_context(data_interval_start=None), now=NOW)
    assert s.session_date is None


def test_on_deadline_missed_writes_a_signal(tmp_path, monkeypatch):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    asyncio.run(on_deadline_missed(context=deadline_context()))
    [signal] = read_signals(tmp_path / "signals")
    assert signal.kind is SignalKind.DEADLINE_MISSED


def test_on_deadline_missed_never_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    # A context missing everything must be logged and swallowed, not raised.
    assert asyncio.run(on_deadline_missed(context={})) is None
    assert read_signals(tmp_path / "signals") == []


@pytest.mark.airflow
def test_make_deadline_alert_anchors_on_queued_at():
    pytest.importorskip("airflow.sdk")
    from airflow.sdk import AsyncCallback

    alert = make_deadline_alert()
    assert type(alert.reference).__name__ == "DagRunQueuedAtDeadline"
    assert alert.interval == DEADLINE_INTERVAL
    assert isinstance(alert.callback, AsyncCallback)
    assert alert.callback.path == CALLBACK_PATH
