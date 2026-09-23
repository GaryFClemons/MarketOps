"""Builders shared by the retrieval tests (imported explicitly; not a conftest)."""

from __future__ import annotations

from market_ops.retrieval.types import Chunk, SearchHit


def mk_chunk(n: int, text: str = "", doc_id: str = "docs/runbooks/x.md", section: str = "") -> Chunk:
    return Chunk(chunk_id=Chunk.make_id(doc_id, n), doc_id=doc_id, section=section, text=text, ordinal=n)


def mk_hits(chunks: list[Chunk], retriever: str = "bm25") -> list[SearchHit]:
    """Hits in the given order, ranks 1..n, descending dummy scores."""
    return [SearchHit(chunk=c, score=float(len(chunks) - i), rank=i + 1, retriever=retriever) for i, c in enumerate(chunks)]


class StubRetriever:
    """Returns fixed hits and records the ``k`` it was asked for."""

    def __init__(self, hits: list[SearchHit]) -> None:
        self.hits = hits
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, k: int = 5) -> list[SearchHit]:
        self.calls.append((query, k))
        return self.hits[:k]
