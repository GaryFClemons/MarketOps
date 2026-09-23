"""``python -m market_ops.evals`` — run the golden sets. Implemented CLI.

    python -m market_ops.evals retrieval --retriever bm25
    python -m market_ops.evals retrieval --retriever hybrid --k 5

Each run prints a markdown report and writes the full JSON (every case, every
hit) to evals/results/, which is gitignored. Curated before/after numbers go in
docs/eval_results.md by hand, with a sentence on what changed between runs.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from market_ops._scaffold import NotBuiltYet
from market_ops.config import REPO_ROOT

DEFAULT_DATASET = REPO_ROOT / "evals" / "golden" / "retrieval.jsonl"
DEFAULT_OUT = REPO_ROOT / "evals" / "results"


def _retrieval(args: argparse.Namespace) -> int:
    from market_ops.evals.dataset import RetrievalCase, load_jsonl
    from market_ops.evals.harness import run_retrieval_eval
    from market_ops.retrieval.service import build_retriever

    cases = load_jsonl(args.dataset, RetrievalCase)
    retriever = build_retriever(args.retriever)
    label = f"{args.retriever} ({args.dataset.name})"
    report = run_retrieval_eval(cases, retriever, k=args.k, label=label)
    print(report.to_markdown())

    args.out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = args.out / f"retrieval-{args.retriever}-k{args.k}-{stamp}.json"
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    print(f"\nwrote {path}", file=sys.stderr)
    return 0


def _briefs(args: argparse.Namespace) -> int:
    print(
        "not built yet: brief evals need the triage agent (market_ops.agent.triage.run_triage) "
        "and score_brief; see docs/build-order.md",
        file=sys.stderr,
    )
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m market_ops.evals")
    sub = parser.add_subparsers(dest="command", required=True)

    r = sub.add_parser("retrieval", help="score a retriever against the retrieval golden set")
    r.add_argument("--retriever", choices=["bm25", "dense", "hybrid"], default="hybrid")
    r.add_argument("--k", type=int, default=5)
    r.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    r.add_argument("--out", type=Path, default=DEFAULT_OUT)
    r.set_defaults(func=_retrieval)

    b = sub.add_parser("briefs", help="score triage briefs against the brief golden set")
    b.set_defaults(func=_briefs)

    args = parser.parse_args(argv)
    # A Windows console may not encode every character a brief or report
    # contains; replace the odd glyph rather than crash mid-output.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    try:
        return args.func(args)
    except NotBuiltYet as exc:
        # The backlog is expected; a traceback for it would be noise.
        print(f"not built yet: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
