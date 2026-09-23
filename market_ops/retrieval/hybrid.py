"""Hybrid retrieval: BM25 and dense rankings fused with Reciprocal Rank Fusion.

Each retriever fails differently. BM25 misses paraphrase ("the vendor keeps
telling us to slow down" shares no tokens with "429 Too Many Requests"); dense
blurs exact identifiers. Fusing their rankings recovers most of both — and the
retrieval eval is what says whether that is true on *this* corpus, per query
type, rather than as a belief.
"""

from __future__ import annotations

from collections.abc import Sequence

from market_ops._scaffold import todo
from market_ops.retrieval.types import Retriever, SearchHit


def reciprocal_rank_fusion(
    rankings: Sequence[Sequence[SearchHit]],
    *,
    k: int = 60,
    top_n: int = 10,
) -> list[SearchHit]:
    """Fuse several ranked lists into one.

    For every chunk appearing in any input list:
        fused(chunk) = sum over the lists containing it of 1 / (k + rank)
    using each list's own 1-based ``rank``.

    Returns at most ``top_n`` hits: one per ``chunk_id`` (deduplicated),
    ``score`` = the fused score, ``retriever="hybrid"``, ranks re-assigned 1..n
    by descending fused score, ties broken by ``chunk_id`` ascending. Empty
    input (or all-empty lists) -> ``[]``.

    Why ranks and not scores: a BM25 score of 12.3 and a cosine of 0.81 are on
    unrelated scales. Normalizing them means choosing and tuning a scheme per
    corpus; RRF uses only positions, so there's nothing to tune.

    Why k = 60: the constant from the original RRF paper (Cormack, Clarke &
    Büttcher, 2009). It damps the top of each list — rank 1 vs rank 2 is
    1/61 vs 1/62, not 1 vs 1/2 — so one retriever's confident #1 can't outvote
    agreement between both. A chunk ranked 3rd by both beats one ranked 1st by
    only one: 2/63 ≈ 0.0317 > 1/61 ≈ 0.0164.
    """
    todo("reciprocal_rank_fusion: sum 1/(k+rank) per chunk_id, sort (-score, chunk_id), re-rank, top_n")


class HybridRetriever:
    """BM25 + dense, fused. Implemented wiring over the core functions.

    Pulls ``candidates`` hits from each retriever (more than the final ``k``: a
    chunk ranked 12th by BM25 and 4th by dense should still get to compete),
    then fuses down to ``k``.
    """

    def __init__(self, bm25: Retriever, dense: Retriever, *, candidates: int = 20, rrf_k: int = 60) -> None:
        self.bm25 = bm25
        self.dense = dense
        self.candidates = candidates
        self.rrf_k = rrf_k

    def search(self, query: str, k: int = 5) -> list[SearchHit]:
        rankings = [self.bm25.search(query, self.candidates), self.dense.search(query, self.candidates)]
        return reciprocal_rank_fusion(rankings, k=self.rrf_k, top_n=k)
