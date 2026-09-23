"""Spec for market_ops.retrieval.chunking."""

from __future__ import annotations

import pytest

from market_ops.retrieval.chunking import chunk_corpus, chunk_document, split_markdown_table
from market_ops.retrieval.types import Document

RUNBOOK = """Preamble line.

# Runbook

## Symptoms

Task fetch_ohlcv is red.

## Diagnosis

### 429 Too Many Requests

The vendor throttled us.

```python
# not a heading
x = 1
```

### 401 Unauthorized

Bad key.

## Empty

## References

See ROADMAP.md.
"""


def _doc(text: str, doc_id: str = "docs/runbooks/test.md") -> Document:
    return Document(doc_id=doc_id, title="Runbook", text=text, source_type="runbook")


def _max_overlap(a: str, b: str, limit: int) -> int:
    """Length of the longest suffix of ``a`` (<= limit) that ``b`` starts with."""
    return max((n for n in range(1, min(limit, len(a), len(b)) + 1) if a[-n:] == b[:n]), default=0)


def test_sections_follow_the_heading_path():
    chunks = chunk_document(_doc(RUNBOOK))
    assert [c.section for c in chunks] == [
        "",
        "Runbook > Symptoms",
        "Runbook > Diagnosis > 429 Too Many Requests",
        "Runbook > Diagnosis > 401 Unauthorized",
        "Runbook > References",
    ]


def test_heading_only_sections_produce_no_chunk():
    sections = {c.section for c in chunk_document(_doc(RUNBOOK))}
    assert "Runbook" not in sections  # H1 immediately followed by an H2
    assert "Runbook > Diagnosis" not in sections  # H2 immediately followed by an H3
    assert "Runbook > Empty" not in sections


def test_chunk_text_is_heading_line_plus_body():
    chunks = chunk_document(_doc(RUNBOOK))
    symptoms = chunks[1]
    assert symptoms.text.startswith("## Symptoms")
    assert "Task fetch_ohlcv is red." in symptoms.text
    assert "## Diagnosis" not in symptoms.text
    assert chunks[0].text == "Preamble line."


def test_code_fence_lines_are_not_headings():
    chunks = chunk_document(_doc(RUNBOOK))
    assert "# not a heading" in chunks[2].text
    assert not any("not a heading" in c.section for c in chunks)


def test_identity_ordinals_ids_and_metadata():
    chunks = chunk_document(_doc(RUNBOOK))
    assert [c.ordinal for c in chunks] == list(range(len(chunks)))
    assert chunks[0].chunk_id == "docs/runbooks/test.md::000"
    assert all(c.doc_id == "docs/runbooks/test.md" for c in chunks)
    assert chunks[0].metadata == {"source_type": "runbook", "title": "Runbook"}


def test_whitespace_only_preamble_yields_nothing():
    chunks = chunk_document(_doc("\n\n# Title\n\nBody.\n"))
    assert [c.section for c in chunks] == ["Title"]


def test_oversize_section_packs_paragraphs_with_overlap():
    paragraphs = [f"Paragraph {i} " + "word " * 38 for i in range(10)]  # ~200 chars each
    text = "# Big\n\n" + "\n\n".join(p.strip() for p in paragraphs) + "\n"
    chunks = chunk_document(_doc(text), max_chars=500, overlap_chars=100)

    assert len(chunks) > 1
    assert all(c.section == "Big" for c in chunks)
    assert all(len(c.text) <= 500 for c in chunks)
    # A paragraph that fits in max_chars - overlap_chars is never split.
    for p in paragraphs:
        assert any(p.strip() in c.text for c in chunks)
    # Every later piece starts with a suffix of the previous one.
    for prev, nxt in zip(chunks, chunks[1:]):
        assert 0 < _max_overlap(prev.text, nxt.text, 100) <= 100


def test_zero_overlap_still_respects_the_limit():
    text = "# Big\n\n" + "\n\n".join("word " * 40 for _ in range(6))
    chunks = chunk_document(_doc(text), max_chars=300, overlap_chars=0)
    assert len(chunks) > 1
    assert all(len(c.text) <= 300 for c in chunks)


def test_single_huge_paragraph_is_hard_split():
    body = " ".join(f"token{i}" for i in range(500))  # ~4,400 chars, no blank lines
    chunks = chunk_document(_doc("# Wall\n\n" + body), max_chars=1000, overlap_chars=100)
    assert len(chunks) >= 5
    assert all(len(c.text) <= 1000 for c in chunks)
    assert chunks[0].text.startswith("# Wall")
    assert chunks[-1].text.endswith("token499")


def test_overlap_must_be_smaller_than_max():
    with pytest.raises(ValueError):
        chunk_document(_doc(RUNBOOK), max_chars=100, overlap_chars=100)


def test_chunk_corpus_flattens_in_order():
    docs = [_doc("# A\n\nx\n", "a.md"), _doc("# B\n\ny\n", "b.md")]
    assert [c.chunk_id for c in chunk_corpus(docs)] == ["a.md::000", "b.md::000"]


# --- split_markdown_table (tier: next) -------------------------------------

HEADER = "| Date | Decision | Why |"
SEP = "|---|---|---|"
ROWS = [f"| 2026-09-{i:02d} | Decision {i} | " + "because " * 10 + "|" for i in range(1, 11)]
TABLE = "\n".join([HEADER, SEP, *ROWS])


def test_table_that_fits_is_unchanged():
    assert split_markdown_table(TABLE, max_chars=10_000) == [TABLE]


def test_table_pieces_repeat_header_and_keep_rows_whole():
    pieces = split_markdown_table(TABLE, max_chars=400)
    assert len(pieces) > 1
    seen_rows = []
    for piece in pieces:
        lines = piece.splitlines()
        assert lines[:2] == [HEADER, SEP]
        assert len(lines) >= 3
        assert len(piece) <= 400
        seen_rows.extend(lines[2:])
    assert seen_rows == ROWS  # every row exactly once, in order
