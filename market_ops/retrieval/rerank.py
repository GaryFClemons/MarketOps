"""Reranking — a second, more expensive pass over the fused candidates.

BM25 and bi-encoder embeddings score the query and each passage
*independently*. A reranker (a cross-encoder, or an LLM prompted to grade
relevance) reads query and passage *together*, so it can tell that "429 after
five waits" answers "why did the retries give up" even with little word
overlap. That costs one model call per candidate, which is why it runs only on
the ~20 fused candidates, never the whole corpus.

Not built. Add it only if the retrieval eval shows ranking errors that fusion
doesn't fix — a reranker without a before/after number is cost without evidence.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from market_ops._scaffold import todo
from market_ops.retrieval.types import SearchHit


class Reranker(Protocol):
    def rerank(self, query: str, hits: Sequence[SearchHit], k: int) -> list[SearchHit]:
        """Reorder ``hits`` for ``query``; return the top ``k`` with ``retriever="rerank"``."""
        ...


class LLMReranker:
    """Grade each candidate 0–3 for relevance with a small model, sort by grade.

    Keep the original fused rank as the tie-breaker, so a reranker that returns
    all zeros degrades to the fused order instead of scrambling it.
    """

    def __init__(self, llm) -> None:
        self.llm = llm

    def rerank(self, query: str, hits: Sequence[SearchHit], k: int) -> list[SearchHit]:
        todo("LLMReranker.rerank: grade (query, passage) pairs, stable-sort by grade, top k", tier="next")
