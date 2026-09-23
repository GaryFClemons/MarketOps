"""HashingEmbedder and DenseIndex construction are implemented; search is spec."""

from __future__ import annotations

import numpy as np
import pytest

from market_ops.retrieval.dense import DenseIndex, HashingEmbedder

from tests.retrieval.helpers import mk_chunk


def test_hashing_embedder_shape_norm_and_determinism():
    texts = ["vendor throttled the request", "partition date mismatch"]
    a = HashingEmbedder(dim=256).embed(texts)
    b = HashingEmbedder(dim=256).embed(texts)  # a fresh instance: no hidden state
    assert a.shape == (2, 256)
    assert np.allclose(np.linalg.norm(a, axis=1), 1.0)
    assert np.array_equal(a, b)


def test_hashing_embedder_empty_text_is_zero_vector():
    assert not HashingEmbedder(dim=64).embed([""]).any()


def test_dense_index_embeds_every_chunk():
    chunks = [mk_chunk(i, f"text {i}") for i in range(3)]
    assert DenseIndex(chunks, HashingEmbedder(dim=64)).matrix.shape == (3, 64)


CHUNKS = [
    mk_chunk(0, "vendor throttled the request with 429"),
    mk_chunk(1, "partition date mismatch on saturday"),
    mk_chunk(2, "scheduler catchup burst after downtime"),
]


def test_identical_text_ranks_first_with_cosine_one():
    hits = DenseIndex(CHUNKS, HashingEmbedder(dim=4096)).search("partition date mismatch on saturday")
    assert hits[0].chunk.ordinal == 1
    assert hits[0].score == pytest.approx(1.0)
    assert hits[0].rank == 1
    assert hits[0].retriever == "dense"


def test_no_shared_words_returns_nothing():
    assert DenseIndex(CHUNKS, HashingEmbedder(dim=4096)).search("zebra quantum") == []


def test_k_limits_results():
    index = DenseIndex(CHUNKS, HashingEmbedder(dim=4096))
    assert len(index.search("vendor partition scheduler", k=2)) == 2
