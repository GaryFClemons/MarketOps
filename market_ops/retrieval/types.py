"""Shared retrieval types: Document -> Chunk -> SearchHit.

Fixed up front because three packages depend on their exact shape: retrieval/
produces them, agent/ cites them in incident briefs, and evals/ scores them
against a golden set. Changing a field here is a contract change.

The one format that matters most is ``Chunk.section``. Golden-set labels are
written at the *section* level — "doc X, heading Y is relevant" — rather than
against chunk ids, so re-chunking with a different size or overlap does not
invalidate the dataset. That only works if every chunker renders the heading
path identically, which is why the format is pinned here and not left to the
chunker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

SourceType = Literal["runbook", "incident", "decision_log", "codebase_map", "doc", "sql"]
RetrieverName = Literal["bm25", "dense", "hybrid", "rerank"]

SECTION_SEP = " > "


@dataclass(frozen=True)
class Document:
    """One source file, whole.

    ``doc_id`` is the repo-relative POSIX path (``docs/runbooks/x.md``) — stable,
    human-readable, and directly citable in a brief. A UUID would be none of those.
    """

    doc_id: str
    title: str
    text: str
    source_type: SourceType


@dataclass(frozen=True)
class Chunk:
    """One retrievable unit.

    Attributes:
        chunk_id: ``f"{doc_id}::{ordinal:03d}"``. Deterministic, so re-indexing the
            same corpus yields the same ids and audit records stay resolvable.
        doc_id: The parent ``Document.doc_id``.
        section: Heading path from the top of the document down to this chunk,
            heading text only (no ``#`` markers), joined with ``SECTION_SEP``:
            ``"Vendor throttling > Diagnosis"``. Empty string for text before the
            first heading.
        text: The chunk body. Contextual retrieval (prepending title/section before
            embedding) is done at index time, not stored here, so ``text`` stays a
            faithful, quotable excerpt of the source.
        ordinal: 0-based position within the document.
    """

    chunk_id: str
    doc_id: str
    section: str
    text: str
    ordinal: int
    metadata: dict[str, str] = field(default_factory=dict, hash=False, compare=False)

    @property
    def headings(self) -> tuple[str, ...]:
        """The section path as a tuple; what golden-set matching compares against."""
        return tuple(self.section.split(SECTION_SEP)) if self.section else ()

    @staticmethod
    def make_id(doc_id: str, ordinal: int) -> str:
        return f"{doc_id}::{ordinal:03d}"


@dataclass(frozen=True)
class SearchHit:
    """One ranked result. ``rank`` is 1-based, because MRR and every human are."""

    chunk: Chunk
    score: float
    rank: int
    retriever: RetrieverName


class Retriever(Protocol):
    """Anything that ranks chunks for a query: BM25Index, DenseIndex, HybridRetriever.

    The agent's ``search_docs`` tool and the eval harness both take a
    ``Retriever``, so "BM25 only vs hybrid" is a constructor argument, not a code
    change — which is what makes a before/after measurement one command each.
    """

    def search(self, query: str, k: int = 5) -> list[SearchHit]: ...
