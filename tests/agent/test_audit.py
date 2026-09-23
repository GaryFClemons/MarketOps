"""Spec for AuditLog.append (read is implemented)."""

from __future__ import annotations

from datetime import UTC, datetime

from market_ops.agent.audit import AuditLog, AuditRecord


def record(step: int, trace_id: str = "t1", event: str = "llm_call") -> AuditRecord:
    return AuditRecord(
        ts=datetime(2026, 9, 18, 6, 2, step, tzinfo=UTC),
        trace_id=trace_id,
        signal_id="b101d2cad2508c70",
        step=step,
        event=event,
        model="scripted",
        input_tokens=100,
        output_tokens=20,
    )


def test_append_creates_parents_and_writes_one_line(tmp_path):
    log = AuditLog(tmp_path / "audit" / "nested" / "triage.jsonl")
    log.append(record(1))
    assert len(log.path.read_text(encoding="utf-8").splitlines()) == 1


def test_append_never_rewrites_earlier_lines(tmp_path):
    log = AuditLog(tmp_path / "triage.jsonl")
    log.append(record(1))
    first = log.path.read_bytes()
    log.append(record(2))
    after = log.path.read_bytes()
    assert after.startswith(first)
    assert len(after.splitlines()) == 2


def test_append_preserves_existing_content(tmp_path):
    path = tmp_path / "triage.jsonl"
    path.write_text(record(1, trace_id="old").model_dump_json() + "\n", encoding="utf-8")
    AuditLog(path).append(record(1, trace_id="new"))
    assert [r.trace_id for r in AuditLog(path).read()] == ["old", "new"]


def test_round_trip_and_filter(tmp_path):
    log = AuditLog(tmp_path / "triage.jsonl")
    for r in (record(1, "a"), record(1, "b"), record(2, "a", "brief")):
        log.append(r)
    assert [r.step for r in log.read("a")] == [1, 2]
    assert log.read("a")[1].event == "brief"
    assert len(log.read()) == 3


def test_read_missing_file_is_empty(tmp_path):
    assert AuditLog(tmp_path / "none.jsonl").read() == []
