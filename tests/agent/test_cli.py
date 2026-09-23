"""The agent CLI. demo-signal and error handling are implemented; the full
scripted run needs the whole core backlog."""

from __future__ import annotations

import argparse
import json

from market_ops._scaffold import NotBuiltYet
from market_ops.agent.__main__ import demo_signal, main
from market_ops.alerts.signals_io import read_signals


def test_demo_signal_is_written(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    assert main(["demo-signal"]) == 0
    assert read_signals(tmp_path / "signals") == [demo_signal()]


def test_demo_transcript_matches_the_demo_signal(repo_root):
    fixture = json.loads((repo_root / "tests/agent/fixtures/demo_transcript.json").read_text(encoding="utf-8"))
    assert fixture["signal_id"] == demo_signal().signal_id
    submit = fixture["responses"][-1]["tool_calls"][0]
    assert submit["name"] == "submit_brief" and submit["arguments"]["signal_id"] == demo_signal().signal_id


def test_unbuilt_backlog_exits_2_with_an_explanation(tmp_path, monkeypatch, capsys, repo_root):
    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    main(["demo-signal"])

    def unbuilt(*args, **kwargs):
        raise NotBuiltYet("[core] sentinel")

    monkeypatch.setattr("market_ops.retrieval.service.build_retriever", unbuilt)
    fixture = str(repo_root / "tests/agent/fixtures/demo_transcript.json")
    assert main(["triage", "--latest", "--scripted", fixture]) == 2
    assert "not built yet: [core] sentinel" in capsys.readouterr().err


def test_scripted_demo_end_to_end(tmp_path, monkeypatch, repo_root):
    """The offline demo: demo signal -> real loop, tools, BM25 over the real
    docs, guardrails, audit -> a submitted brief. Needs the core backlog."""
    from market_ops.agent.__main__ import _triage

    monkeypatch.setenv("OPS_DIR", str(tmp_path))
    main(["demo-signal"])
    args = argparse.Namespace(
        signal=None, latest=True, scripted=str(repo_root / "tests/agent/fixtures/demo_transcript.json"),
        runs=None, retriever="bm25", out=str(tmp_path / "briefs"),
    )
    assert _triage(args) == 0
    result = json.loads((tmp_path / "briefs" / f"{demo_signal().signal_id}.json").read_text(encoding="utf-8"))
    assert result["stop_reason"] == "submitted"
    assert (tmp_path / "audit" / "triage.jsonl").exists()
