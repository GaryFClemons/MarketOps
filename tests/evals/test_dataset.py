"""Golden-set schemas and loader are implemented; these pass today."""

from __future__ import annotations

import pytest

from market_ops.evals.dataset import BriefCase, RelevanceLabel, RetrievalCase, load_jsonl

GOOD = '{"id": "ret-001", "question": "q?", "relevant": [{"doc_id": "ROADMAP.md", "heading": "Decision log"}], "tags": ["why"]}'


def test_loads_and_skips_comments_and_blanks(tmp_path):
    path = tmp_path / "g.jsonl"
    path.write_text(f"// a comment\n\n{GOOD}\n", encoding="utf-8")
    [case] = load_jsonl(path, RetrievalCase)
    assert case.relevant == [RelevanceLabel(doc_id="ROADMAP.md", heading="Decision log")]


def test_bad_line_names_file_and_line(tmp_path):
    path = tmp_path / "g.jsonl"
    path.write_text(GOOD + '\n{"id": "ret-002", "question": "no labels", "relevant": [], "tags": ["why"]}\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"g\.jsonl:2"):
        load_jsonl(path, RetrievalCase)


def test_duplicate_ids_rejected(tmp_path):
    path = tmp_path / "g.jsonl"
    path.write_text(GOOD + "\n" + GOOD + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_jsonl(path, RetrievalCase)


def test_unknown_fields_rejected(tmp_path):
    path = tmp_path / "g.jsonl"
    path.write_text(GOOD[:-1] + ', "relevance": 1}\n', encoding="utf-8")
    with pytest.raises(ValueError):
        load_jsonl(path, RetrievalCase)


def test_brief_case_embeds_a_real_signal():
    from tests.factories import make_signal

    case = BriefCase(id="brief-x", signal=make_signal(), expected_severity="high", expected_requires_human=True)
    assert case.signal.dag_id == "ingest_ohlcv_daily"
