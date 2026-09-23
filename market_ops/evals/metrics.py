"""Retrieval metrics over section-level labels.

``hits`` is always a ranked ``list[SearchHit]`` (rank 1 first) and ``labels`` a
``list[RelevanceLabel]``. Relevance is decided by ``is_relevant``, which is part
of the dataset contract and therefore implemented; the metrics are core.
"""

from __future__ import annotations

from collections.abc import Sequence

from market_ops._scaffold import todo
from market_ops.evals.dataset import RelevanceLabel
from market_ops.retrieval.types import Chunk, SearchHit


def is_relevant(chunk: Chunk, label: RelevanceLabel) -> bool:
    """True when the chunk is from the labelled doc and, if the label names a
    heading, that heading appears anywhere in the chunk's heading path."""
    return chunk.doc_id == label.doc_id and (label.heading is None or label.heading in chunk.headings)


def precision_at_k(hits: Sequence[SearchHit], labels: Sequence[RelevanceLabel], k: int) -> float:
    """Fraction of the top-``k`` *slots* holding a relevant chunk.

    ``(number of hits[:k] relevant to ANY label) / k``.

    Divide by ``k`` even when fewer than ``k`` hits came back: returning two
    good hits when five were asked for is 0.4, not 1.0. Otherwise a retriever
    could look perfect by returning almost nothing.

    ``k <= 0`` or empty ``labels`` -> ``ValueError`` (a case with no labels is a
    dataset bug, not a zero).
    """
    todo("precision_at_k: relevant hits in the top k / k; ValueError on k <= 0 or no labels")


def recall_at_k(hits: Sequence[SearchHit], labels: Sequence[RelevanceLabel], k: int) -> float:
    """Fraction of *labels* found in the top ``k``.

    ``(number of labels matched by at least one of hits[:k]) / len(labels)``.

    Label-level, not chunk-level: five chunks from one relevant section count as
    finding one thing, not five. That's the question an on-call reader cares
    about — "did I get pointed at every section I need?".

    ``k <= 0`` or empty ``labels`` -> ``ValueError``.
    """
    todo("recall_at_k: labels covered by the top k / number of labels; ValueError on k <= 0 or no labels")


def reciprocal_rank(hits: Sequence[SearchHit], labels: Sequence[RelevanceLabel]) -> float:
    """``1 / rank`` of the first relevant hit; ``0.0`` if none is relevant.

    Uses each hit's ``rank`` field. Averaged over cases this is MRR: it rewards
    putting the right section *first*, which matters because an agent (and a
    tired human) reads the top hit hardest.

    Empty ``labels`` -> ``ValueError``.
    """
    todo("reciprocal_rank: 1/rank of the first relevant hit, else 0.0; ValueError on no labels")
