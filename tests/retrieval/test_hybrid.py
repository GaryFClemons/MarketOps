"""Spec for reciprocal_rank_fusion and the HybridRetriever wiring."""

from __future__ import annotations

import pytest

from market_ops.retrieval.hybrid import HybridRetriever, reciprocal_rank_fusion

from tests.retrieval.helpers import StubRetriever, mk_chunk, mk_hits

X, Y, Z = mk_chunk(0, "x"), mk_chunk(1, "y"), mk_chunk(2, "z")


def test_agreement_wins():
    # A: x1 y2    B: y1 z2
    # x = 1/61, y = 1/62 + 1/61, z = 1/62  ->  y, x, z
    fused = reciprocal_rank_fusion([mk_hits([X, Y]), mk_hits([Y, Z], "dense")])
    assert [h.chunk.chunk_id for h in fused] == [Y.chunk_id, X.chunk_id, Z.chunk_id]
    assert fused[0].score == pytest.approx(1 / 61 + 1 / 62)
    assert [h.rank for h in fused] == [1, 2, 3]
    assert {h.retriever for h in fused} == {"hybrid"}


def test_third_in_both_beats_first_in_one():
    p1, p2, q1, q2 = (mk_chunk(i, "") for i in range(10, 14))
    fused = reciprocal_rank_fusion([mk_hits([p1, p2, Z]), mk_hits([q1, q2, Z])])
    assert fused[0].chunk.chunk_id == Z.chunk_id  # 2/63 ~ 0.0317 > 1/61 ~ 0.0164


def test_top_n():
    fused = reciprocal_rank_fusion([mk_hits([X, Y]), mk_hits([Y, Z])], top_n=2)
    assert [h.chunk.chunk_id for h in fused] == [Y.chunk_id, X.chunk_id]


def test_ties_break_on_chunk_id():
    fused = reciprocal_rank_fusion([mk_hits([Y]), mk_hits([X])])
    assert [h.chunk.chunk_id for h in fused] == [X.chunk_id, Y.chunk_id]


def test_k_controls_how_much_the_top_rank_dominates():
    others = [mk_chunk(i, "") for i in range(20, 28)]
    a = mk_hits([X, *others[:3], Y])  # x rank 1, y rank 5
    b = mk_hits([*others[3:7], Y])  # y rank 5
    # k=0: x = 1/1 = 1.0, y = 1/5 + 1/5 = 0.4          -> x first
    assert reciprocal_rank_fusion([a, b], k=0)[0].chunk.chunk_id == X.chunk_id
    # k=60: x = 1/61 ~ 0.0164, y = 2/65 ~ 0.0308       -> y first
    assert reciprocal_rank_fusion([a, b], k=60)[0].chunk.chunk_id == Y.chunk_id


def test_empty_inputs():
    assert reciprocal_rank_fusion([]) == []
    assert reciprocal_rank_fusion([[], []]) == []


def test_hybrid_retriever_pulls_candidates_then_fuses_to_k():
    bm25 = StubRetriever(mk_hits([X, Y]))
    dense = StubRetriever(mk_hits([Y, Z], "dense"))
    hits = HybridRetriever(bm25, dense, candidates=20).search("q", k=2)
    assert bm25.calls == [("q", 20)] and dense.calls == [("q", 20)]
    assert [h.chunk.chunk_id for h in hits] == [Y.chunk_id, X.chunk_id]
