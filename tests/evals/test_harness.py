"""Spec for the harness (report rendering is implemented; the loops are core)."""

from __future__ import annotations

import argparse
import json

import pytest

from market_ops.agent.schemas import ActionKind, RecommendedAction, Severity
from market_ops.evals.dataset import BriefCase, RelevanceLabel, RetrievalCase
from market_ops.evals.harness import RetrievalReport, TagStats, run_retrieval_eval, score_brief
from market_ops.retrieval.types import Chunk, SearchHit
from tests.factories import DOC_REF, LOG_REF, make_brief, make_signal

V = "docs/runbooks/vendor.md"


def _chunk(doc_id: str, n: int, section: str = "") -> Chunk:
    return Chunk(chunk_id=Chunk.make_id(doc_id, n), doc_id=doc_id, section=section, text="", ordinal=n)


A = _chunk(V, 0, "Vendor > Symptoms")
C = _chunk("ROADMAP.md", 9, "Roadmap > Decision log")
D = _chunk("codebase-map.md", 1, "Map > Topology")


class DictRetriever:
    def __init__(self, table: dict[str, list[Chunk]]) -> None:
        self.table = table

    def search(self, query: str, k: int = 5) -> list[SearchHit]:
        return [SearchHit(chunk=c, score=1.0, rank=i + 1, retriever="bm25") for i, c in enumerate(self.table[query][:k])]


CASES = [
    RetrievalCase(id="c1", question="q1", relevant=[RelevanceLabel(doc_id=V)], tags=["identifier"]),
    RetrievalCase(
        id="c2",
        question="q2",
        relevant=[RelevanceLabel(doc_id="ROADMAP.md", heading="Decision log")],
        tags=["paraphrase", "why"],
    ),
]
RETRIEVER = DictRetriever({"q1": [A, D], "q2": [D, C]})


def fake_clock(*times: float):
    it = iter(times)
    return lambda: next(it)


def test_run_retrieval_eval_aggregates():
    # c1: hits A D, k=2 -> P 1/2, R 1, RR 1     latency (0.010 - 0.000) s = 10 ms
    # c2: hits D C, k=2 -> P 1/2, R 1, RR 1/2   latency (1.030 - 1.000) s = 30 ms
    report = run_retrieval_eval(CASES, RETRIEVER, k=2, label="stub", clock=fake_clock(0.0, 0.010, 1.0, 1.030))
    assert report.n_cases == 2 and report.k == 2 and report.label == "stub"
    assert report.mean_precision == pytest.approx(0.5)
    assert report.mean_recall == pytest.approx(1.0)
    assert report.mrr == pytest.approx(0.75)
    assert report.latency_p50_ms == pytest.approx(20.0)  # linear interpolation of [10, 30]
    assert report.latency_p95_ms == pytest.approx(29.0)
    assert report.cases[0].hit_chunk_ids == [A.chunk_id, D.chunk_id]
    assert report.cases[1].latency_ms == pytest.approx(30.0)


def test_per_tag_breakdown_counts_multi_tag_cases_in_each():
    report = run_retrieval_eval(CASES, RETRIEVER, k=2, label="stub", clock=fake_clock(0, 0, 0, 0))
    assert set(report.by_tag) == {"identifier", "paraphrase", "why"}
    assert report.by_tag["identifier"].mrr == pytest.approx(1.0)
    assert report.by_tag["why"].mrr == pytest.approx(0.5)
    assert report.by_tag["paraphrase"].n == 1


def test_empty_case_list_raises():
    with pytest.raises(ValueError):
        run_retrieval_eval([], RETRIEVER, label="stub")


def test_report_markdown_renders():
    report = RetrievalReport(
        label="bm25", k=5, n_cases=1, mean_precision=0.4, mean_recall=1.0, mrr=1.0,
        latency_p50_ms=1.0, latency_p95_ms=2.0,
        by_tag={"identifier": TagStats(n=1, mean_precision=0.4, mean_recall=1.0, mrr=1.0)},
    )
    md = report.to_markdown()
    assert "| P@5 | 0.400 |" in md
    assert "| identifier | 1 |" in md


# --- score_brief -----------------------------------------------------------


def _case(**kw) -> BriefCase:
    base = dict(id="brief-001", signal=make_signal(), expected_severity=Severity.HIGH, expected_requires_human=True)
    base.update(kw)
    return BriefCase(**base)


def test_score_brief_all_good():
    doc = DOC_REF.split("::")[0]
    score = score_brief(make_brief(), _case(must_cite_docs=[doc], must_mention=["POLYGON_DEFAULT"]))
    assert score.passed
    assert score.cite_recall == 1.0
    assert score.missing_mentions == [] and score.forbidden_mentions == []


def test_score_brief_reports_each_failure():
    brief = make_brief(
        severity=Severity.MEDIUM,
        recommended_actions=[RecommendedAction(kind=ActionKind.RERUN, step="Increase retries and rerun")],
    )
    case = _case(
        must_cite_docs=[DOC_REF.split("::")[0], "docs/runbooks/config-and-secrets.md"],
        must_mention=["polygon_default", "Fernet"],
        must_not_mention=["increase retries"],
    )
    score = score_brief(brief, case)
    assert not score.passed
    assert not score.severity_ok
    assert score.requires_human_ok
    assert score.cite_recall == pytest.approx(0.5)
    assert score.missing_citations == ["docs/runbooks/config-and-secrets.md"]
    assert score.missing_mentions == ["Fernet"]
    assert score.forbidden_mentions == ["increase retries"]


def test_task_log_refs_are_not_doc_citations():
    # Only DOC evidence counts toward must_cite_docs. The factory brief also
    # carries a TASK_LOG ref; an implementation that treated every ref as a
    # document would "cite" LOG_REF here and score 1.0.
    score = score_brief(make_brief(), _case(must_cite_docs=[LOG_REF]))
    assert score.cite_recall == 0.0


# --- CLI -------------------------------------------------------------------


def test_cli_retrieval_writes_a_report(tmp_path, monkeypatch):
    from market_ops.evals import __main__ as cli

    dataset = tmp_path / "golden.jsonl"
    dataset.write_text("\n".join(c.model_dump_json() for c in CASES), encoding="utf-8")
    monkeypatch.setattr("market_ops.retrieval.service.build_retriever", lambda mode: RETRIEVER)
    args = argparse.Namespace(retriever="bm25", k=2, dataset=dataset, out=tmp_path / "out")
    assert cli._retrieval(args) == 0
    [written] = (tmp_path / "out").glob("retrieval-bm25-k2-*.json")
    assert json.loads(written.read_text())["n_cases"] == 2


def test_cli_briefs_explains_it_is_not_built(capsys):
    # Documents the current stub; replace when the briefs command is built.
    from market_ops.evals.__main__ import main

    assert main(["briefs"]) == 2
    assert "not built yet" in capsys.readouterr().err
