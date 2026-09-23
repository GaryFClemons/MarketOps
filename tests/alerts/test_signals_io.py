"""signals_io is implemented plumbing; these tests pass today."""

from __future__ import annotations

from datetime import UTC, datetime

from market_ops.agent.schemas import RunSignal, SignalKind
from market_ops.alerts.signals_io import read_signals, write_signal


def _signal(run_id: str, minute: int) -> RunSignal:
    kind = SignalKind.TASK_FAILED
    return RunSignal(
        signal_id=RunSignal.stable_id(kind, "ingest_ohlcv_daily", run_id, "fetch_ohlcv", 1),
        kind=kind,
        dag_id="ingest_ohlcv_daily",
        run_id=run_id,
        task_id="fetch_ohlcv",
        try_number=1,
        detected_at=datetime(2026, 9, 18, 6, minute, tzinfo=UTC),
        message="Response code 401; Authentication/Permission Issue",
    )


def test_round_trip(tmp_path):
    s = _signal("scheduled__2026-09-18T06:00:00+00:00", 5)
    path = write_signal(s, tmp_path / "signals")
    assert path.name == f"{s.signal_id}.json"
    assert read_signals(tmp_path / "signals") == [s]


def test_same_event_twice_is_one_file(tmp_path):
    s = _signal("scheduled__2026-09-18T06:00:00+00:00", 5)
    write_signal(s, tmp_path)
    write_signal(s, tmp_path)
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_sorted_oldest_first_and_tmp_ignored(tmp_path):
    late = _signal("run-b", 9)
    early = _signal("run-a", 1)
    write_signal(late, tmp_path)
    write_signal(early, tmp_path)
    (tmp_path / "half-written.json.tmp").write_text("{", encoding="utf-8")
    assert [s.run_id for s in read_signals(tmp_path)] == ["run-a", "run-b"]


def test_missing_directory_is_empty(tmp_path):
    assert read_signals(tmp_path / "nope") == []
