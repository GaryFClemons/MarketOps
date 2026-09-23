"""Spec for market_ops.evals.metrics (is_relevant is implemented)."""

from __future__ import annotations

import pytest

from market_ops.evals.dataset import RelevanceLabel
from market_ops.evals.metrics import is_relevant, precision_at_k, recall_at_k, reciprocal_rank
from market_ops.retrieval.types import Chunk, SearchHit

V = "docs/runbooks/vendor.md"


def chunk(doc_id: str, n: int, section: str) -> Chunk:
    return Chunk(chunk_id=Chunk.make_id(doc_id, n), doc_id=doc_id, section=section, text="", ordinal=n)


A = chunk(V, 2, "Vendor > Diagnosis > 429 Too Many Requests")
A2 = chunk(V, 3, "Vendor > Diagnosis > 429 Too Many Requests")  # same section, second chunk
B = chunk(V, 5, "Vendor > Resolution")
C = chunk("ROADMAP.md", 40, "Roadmap > Decision log")
D = chunk("codebase-map.md", 1, "Map > Topology")

LABELS = [RelevanceLabel(doc_id=V, heading="429 Too Many Requests"), RelevanceLabel(doc_id="ROADMAP.md", heading="Decision log")]
# Ranked: D (irrelevant), A (L1), A2 (L1 again), C (L2), B (irrelevant)
HITS = [SearchHit(chunk=c, score=1.0, rank=i + 1, retriever="hybrid") for i, c in enumerate([D, A, A2, C, B])]


def test_is_relevant():
    assert is_relevant(A, LABELS[0])
    assert not is_relevant(B, LABELS[0])
    assert is_relevant(B, RelevanceLabel(doc_id=V))  # heading None -> any chunk of the doc
    assert is_relevant(A, RelevanceLabel(doc_id=V, heading="Diagnosis"))  # any level of the path
    assert not is_relevant(A, RelevanceLabel(doc_id=V, heading="429"))  # exact heading text only


@pytest.mark.parametrize(
    ("k", "expected"),
    [
        (1, 0.0),  # D
        (2, 0.5),  # D A
        (5, 0.6),  # D A A2 C B -> 3 relevant / 5
        (10, 0.3),  # only 5 hits, still divided by 10
    ],
)
def test_precision_at_k(k, expected):
    assert precision_at_k(HITS, LABELS, k) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("k", "expected"),
    [
        (1, 0.0),
        (2, 0.5),  # L1 found
        (3, 0.5),  # A2 is L1 again: counts once
        (4, 1.0),  # C finds L2
    ],
)
def test_recall_at_k_is_label_level(k, expected):
    assert recall_at_k(HITS, LABELS, k) == pytest.approx(expected)


def test_reciprocal_rank():
    assert reciprocal_rank(HITS, LABELS) == pytest.approx(0.5)  # first relevant is rank 2
    assert reciprocal_rank(HITS[:1], LABELS) == 0.0
    assert reciprocal_rank([], LABELS) == 0.0


@pytest.mark.parametrize(
    "call",
    [
        lambda: precision_at_k(HITS, LABELS, 0),
        lambda: recall_at_k(HITS, LABELS, -1),
        lambda: precision_at_k(HITS, [], 5),
        lambda: recall_at_k(HITS, [], 5),
        lambda: reciprocal_rank(HITS, []),
    ],
)
def test_invalid_arguments_raise(call):
    with pytest.raises(ValueError):
        call()
