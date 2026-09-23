"""Run the golden sets and aggregate the results.

Report models and their markdown rendering are implemented; the evaluation
loops and the brief scorer are core.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

from pydantic import BaseModel, Field

from market_ops._scaffold import todo
from market_ops.agent.schemas import IncidentBrief
from market_ops.evals.dataset import BriefCase, RetrievalCase
from market_ops.retrieval.types import Retriever


class CaseResult(BaseModel):
    case_id: str
    tags: list[str]
    precision: float
    recall: float
    reciprocal_rank: float
    latency_ms: float
    hit_chunk_ids: list[str]


class TagStats(BaseModel):
    n: int
    mean_precision: float
    mean_recall: float
    mrr: float


class RetrievalReport(BaseModel):
    """One retriever, one dataset, one ``k``. Serialized to evals/results/ per run."""

    label: str
    k: int
    n_cases: int
    mean_precision: float
    mean_recall: float
    mrr: float
    latency_p50_ms: float
    latency_p95_ms: float
    by_tag: dict[str, TagStats] = Field(default_factory=dict)
    cases: list[CaseResult] = Field(default_factory=list)

    def to_markdown(self) -> str:
        """Summary + per-tag table, ready to paste into docs/eval_results.md."""
        lines = [
            f"**{self.label}** — k={self.k}, {self.n_cases} cases",
            "",
            f"| Metric | Value |",
            f"|---|---|",
            f"| P@{self.k} | {self.mean_precision:.3f} |",
            f"| R@{self.k} | {self.mean_recall:.3f} |",
            f"| MRR | {self.mrr:.3f} |",
            f"| Latency p50 / p95 (ms) | {self.latency_p50_ms:.1f} / {self.latency_p95_ms:.1f} |",
            "",
            f"| Tag | n | P@{self.k} | R@{self.k} | MRR |",
            "|---|---|---|---|---|",
        ]
        for tag, s in sorted(self.by_tag.items()):
            lines.append(f"| {tag} | {s.n} | {s.mean_precision:.3f} | {s.mean_recall:.3f} | {s.mrr:.3f} |")
        return "\n".join(lines)


def run_retrieval_eval(
    cases: Sequence[RetrievalCase],
    retriever: Retriever,
    *,
    k: int = 5,
    label: str,
    clock: Callable[[], float] = time.perf_counter,
) -> RetrievalReport:
    """Search every case's question and score the hits.

    Per case: call ``clock()`` immediately before and after
    ``retriever.search(case.question, k)``; ``latency_ms = (after - before) *
    1000``. Score with ``precision_at_k`` / ``recall_at_k`` /
    ``reciprocal_rank``; record the hit chunk ids in rank order.

    Aggregate: plain means over cases for precision, recall and reciprocal rank
    (the last is MRR); ``latency_p50_ms`` / ``latency_p95_ms`` via
    ``numpy.percentile`` (its default linear interpolation); ``by_tag`` — the
    same three means per tag, where a case with two tags counts in both.
    Empty ``cases`` -> ``ValueError``.

    Why ``clock`` is injectable: latency is a first-class metric, and a fake
    clock makes the latency math testable exactly.

    Why per-tag: "hybrid beats BM25" averaged over everything can hide "hybrid
    loses on exact identifiers". The tags make that visible.
    """
    todo("run_retrieval_eval: time each search, score it, aggregate means, p50/p95, per-tag stats")


class BriefScore(BaseModel):
    case_id: str
    severity_ok: bool
    requires_human_ok: bool
    cite_recall: float
    missing_citations: list[str]
    missing_mentions: list[str]
    forbidden_mentions: list[str]
    passed: bool


def score_brief(brief: IncidentBrief, case: BriefCase) -> BriefScore:
    """Deterministic checks of one brief against its golden case.

    - ``severity_ok``: ``brief.severity == case.expected_severity``
    - ``requires_human_ok``: ``brief.requires_human == case.expected_requires_human``
    - Cited docs: the doc part (text before ``"::"``) of every evidence ``ref``
      whose ``source`` is DOC. ``missing_citations`` = ``must_cite_docs`` not
      cited, in case order; ``cite_recall`` = fraction cited (1.0 when
      ``must_cite_docs`` is empty).
    - Mentions: case-insensitive substring search over ``summary``,
      ``probable_cause`` and every action ``step``. ``missing_mentions`` =
      ``must_mention`` not found; ``forbidden_mentions`` = ``must_not_mention``
      found. Both in case order.
    - ``passed`` = severity_ok and requires_human_ok and no missing citations,
      no missing mentions, no forbidden mentions.

    Why substring checks and not a judge: they're crude, but they're exact,
    free, and never drift. "Did the 401 brief tell someone to add retries?" is
    a substring question. Save the judge for faithfulness and tone.
    """
    todo("score_brief: severity, requires_human, cited-doc recall, must/must-not mentions -> BriefScore")


def compare_reports(before: RetrievalReport, after: RetrievalReport) -> str:
    """Markdown delta table (overall and per tag) between two runs of one dataset.

    The before/after record in docs/eval_results.md: change one thing, run
    both, keep the diff.
    """
    todo("compare_reports: overall and per-tag deltas as a markdown table", tier="next")
