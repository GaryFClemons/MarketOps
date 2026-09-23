"""``python -m market_ops.agent`` — triage signals from the command line. Implemented CLI.

    python -m market_ops.agent demo-signal
    python -m market_ops.agent triage --latest --scripted tests/agent/fixtures/demo_transcript.json
    python -m market_ops.agent triage --signal data/ops/signals/<id>.json

``--scripted`` replays a recorded model transcript through the real loop,
tools, guardrails and audit log: an offline, deterministic, free demo. Without
it, the client comes from ``LLM_PROVIDER`` / ``LLM_MODEL``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from market_ops._scaffold import NotBuiltYet
from market_ops.agent.llm import LLMResponse, ScriptedLLM
from market_ops.agent.schemas import RunSignal, SignalKind
from market_ops.config import Settings

DEMO_RUN_ID = "manual__demo"


def demo_signal() -> RunSignal:
    """A realistic 401 failure: the message is the DAG's exact exception text.

    Fixed fields -> a fixed ``signal_id``, so the scripted demo transcript can
    cite ``signal:<id>`` and pass validation on any machine.
    """
    kind = SignalKind.TASK_FAILED
    return RunSignal(
        signal_id=RunSignal.stable_id(kind, "ingest_ohlcv_daily", DEMO_RUN_ID, "fetch_ohlcv", 1),
        kind=kind,
        dag_id="ingest_ohlcv_daily",
        run_id=DEMO_RUN_ID,
        task_id="fetch_ohlcv",
        try_number=1,
        session_date=datetime(2026, 9, 17).date(),
        detected_at=datetime(2026, 9, 18, 6, 2, tzinfo=UTC),
        message="Response code 401; Authentication/Permission Issue",
        attributes={"exception_type": "AirflowFailException"},
    )


def _load_signal(args: argparse.Namespace, settings: Settings) -> RunSignal:
    from market_ops.alerts.signals_io import read_signals

    if args.signal:
        return RunSignal.model_validate_json(Path(args.signal).read_text(encoding="utf-8"))
    signals = read_signals(settings.signals_dir)
    if not signals:
        raise SystemExit(f"no signals in {settings.signals_dir}; try: python -m market_ops.agent demo-signal")
    return signals[-1]


def _demo_signal(args: argparse.Namespace) -> int:
    from market_ops.alerts.signals_io import write_signal

    path = write_signal(demo_signal(), Settings.from_env().signals_dir)
    print(path)
    return 0


def _triage(args: argparse.Namespace) -> int:
    from market_ops.agent.audit import AuditLog
    from market_ops.agent.providers import make_client
    from market_ops.agent.render import brief_to_markdown
    from market_ops.agent.tools import FixtureRunSource, ToolContext
    from market_ops.agent.triage import run_triage
    from market_ops.retrieval.service import build_retriever

    settings = Settings.from_env()
    signal = _load_signal(args, settings)

    if args.scripted:
        data = json.loads(Path(args.scripted).read_text(encoding="utf-8"))
        llm = ScriptedLLM([LLMResponse.model_validate(r) for r in data["responses"]], model="scripted")
    else:
        llm = make_client(settings)

    # Run state: a fixture file for now; AirflowApiRunSource is the next tier.
    run_source = FixtureRunSource.from_json(args.runs) if args.runs else FixtureRunSource()
    ctx = ToolContext(run_source=run_source, retriever=build_retriever(args.retriever), raw_zone=settings.raw_zone)

    result = run_triage(signal, llm=llm, ctx=ctx, audit=AuditLog(settings.audit_log))

    out_dir = Path(args.out) if args.out else settings.ops_dir / "briefs"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{signal.signal_id}.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

    print(brief_to_markdown(result.brief))
    print(
        f"stop_reason={result.stop_reason} fallback={result.fallback_used} turns={result.turns} "
        f"tool_calls={result.tool_calls} tokens={result.usage.input_tokens}+{result.usage.output_tokens} "
        f"trace_id={result.trace_id}",
        file=sys.stderr,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m market_ops.agent")
    sub = parser.add_subparsers(dest="command", required=True)

    d = sub.add_parser("demo-signal", help="write a sample 401 failure signal to the signals directory")
    d.set_defaults(func=_demo_signal)

    t = sub.add_parser("triage", help="triage one signal and print the incident brief")
    which = t.add_mutually_exclusive_group(required=True)
    which.add_argument("--signal", help="path to a RunSignal JSON file")
    which.add_argument("--latest", action="store_true", help="the newest signal in the signals directory")
    t.add_argument("--scripted", help="replay a recorded transcript instead of calling a model")
    t.add_argument("--runs", help="FixtureRunSource JSON with DagRun/TaskInstance state")
    t.add_argument("--retriever", choices=["bm25", "dense", "hybrid"], default="hybrid")
    t.add_argument("--out", help="directory for the brief JSON (default: <OPS_DIR>/briefs)")
    t.set_defaults(func=_triage)

    args = parser.parse_args(argv)
    # A Windows console may not encode every character a brief or report
    # contains; replace the odd glyph rather than crash mid-output.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    try:
        return args.func(args)
    except NotBuiltYet as exc:
        # The backlog is expected; explain it instead of printing a traceback.
        print(f"not built yet: {exc}  (see docs/build-order.md)", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
