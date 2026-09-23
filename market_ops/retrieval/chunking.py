"""Structure-aware markdown chunking.

The corpus is hand-written markdown with meaningful headings: a runbook's
``## Diagnosis > ### 429 Too Many Requests`` section *is* the answer to "we're
being throttled". So chunks follow the heading structure first and only fall
back to size-based splitting when a section is too big. Chunking by a fixed
window would cut answers in half and glue unrelated sections together.

Sizes are in characters, not tokens: no tokenizer dependency, and at ~4
characters per token the 1,200-character default is ~300 tokens — big enough
for a whole runbook section, small enough that ten hits fit in a prompt.
"""

from __future__ import annotations

from collections.abc import Iterable

from market_ops._scaffold import todo
from market_ops.retrieval.types import Chunk, Document


def chunk_document(doc: Document, *, max_chars: int = 1200, overlap_chars: int = 150) -> list[Chunk]:
    """Split one markdown document into section-aligned chunks.

    Rules:

    1. **Headings.** A line matching ``#`` to ``######`` + space + text starts a
       new section. ``Chunk.section`` is the heading-text path (no ``#``
       markers) joined with ``SECTION_SEP``: ``### Diagnosis`` under
       ``## Vendor 429`` under ``# Runbook`` -> ``"Runbook > Vendor 429 >
       Diagnosis"``. A heading closes every open heading at its level or
       deeper. Text before the first heading has section ``""``.
    2. **Code fences.** Inside a fenced block (opened and closed by a line
       starting with ```` ``` ```` or ``~~~``) nothing is a heading — a
       ``# comment`` in a code sample is code.
    3. **Chunk text** is the section's heading line followed by its body, with
       leading/trailing whitespace stripped. A section whose body is empty (a
       heading immediately followed by another heading) yields no chunk. A
       whitespace-only preamble yields no chunk.
    4. **Oversize sections.** If the text exceeds ``max_chars``, split it on
       paragraph boundaries (blank lines; a fenced block counts as one
       paragraph) by packing whole paragraphs greedily. A paragraph of at most
       ``max_chars - overlap_chars`` characters is never split. A longer one is
       hard-split into windows.
    5. **Overlap.** Each piece after the first begins with a suffix of the
       previous piece: at most ``overlap_chars`` characters, starting at a word
       boundary. ``overlap_chars=0`` disables overlap.
    6. **Limit.** Every chunk's text, overlap included, is at most ``max_chars``.
       ``overlap_chars >= max_chars`` raises ``ValueError``.
    7. **Identity.** Ordinals are 0, 1, 2, ... across the whole document in
       reading order; ``chunk_id = Chunk.make_id(doc.doc_id, ordinal)``;
       ``metadata = {"source_type": doc.source_type, "title": doc.title}``.

    Why the heading line stays in the text: it is part of what makes the chunk
    quotable in an incident brief, and it gives BM25 the section's own name as
    a term — "Retry-After" in a heading is strong evidence.

    Why overlap only when splitting: section boundaries are real semantic
    boundaries, so overlapping across them would just duplicate text. Inside an
    oversize section the boundary is arbitrary, and overlap stops a sentence
    that straddles it from being unfindable.

    Hints: walk lines once, tracking a fence flag and a heading stack; collect
    (section_path, lines) blocks; then size-split each block.
    """
    todo("chunk_document: heading-path sections, fence-aware, paragraph packing with overlap, <= max_chars")


def chunk_corpus(docs: Iterable[Document], **kwargs) -> list[Chunk]:
    """Chunk every document; ``kwargs`` pass through to ``chunk_document``."""
    return [chunk for doc in docs for chunk in chunk_document(doc, **kwargs)]


def split_markdown_table(table: str, *, max_chars: int) -> list[str]:
    """Split one markdown table into pieces that each repeat the header.

    Input: a table as text (header row, separator row ``|---|...``, body rows).

    Returns pieces where each piece is the header row + separator row + one or
    more *whole* body rows, in original order, every body row appearing exactly
    once overall. Pieces are at most ``max_chars`` unless a single row plus the
    header is already longer (that row then forms its own piece). A table that
    already fits is returned as a single piece, unchanged.

    Why: ROADMAP.md's decision log is one ~50-row table in one section, far over
    ``max_chars``. Paragraph packing can't split it (no blank lines) and hard
    splits would cut decisions in half and orphan rows from their column
    names. Row-aligned pieces that repeat the header keep every decision whole
    and self-describing — the "why" questions land on exactly one row.
    """
    todo("split_markdown_table: row-aligned pieces, header + separator repeated, <= max_chars", tier="next")
