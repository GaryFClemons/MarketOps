from __future__ import annotations

import pytest

from market_ops.retrieval.service import build_retriever


def test_unknown_mode_is_rejected_before_any_work(tmp_path):
    with pytest.raises(ValueError):
        build_retriever("vector", root=tmp_path)


@pytest.mark.parametrize("mode", ["bm25", "dense", "hybrid"])
def test_every_mode_answers_over_the_real_corpus(repo_root, mode):
    # End to end over the repo's own docs: loads, chunks, indexes, searches.
    hits = build_retriever(mode, root=repo_root).search("partition", k=3)
    assert 0 < len(hits) <= 3
    assert [h.rank for h in hits] == list(range(1, len(hits) + 1))
