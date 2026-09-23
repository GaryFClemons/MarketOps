"""Spec for market_ops.retrieval.bm25."""

from __future__ import annotations

import math

import pytest

from market_ops.retrieval.bm25 import BM25Index, tokenize

from tests.retrieval.helpers import mk_chunk


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("AirflowFailException.", ["airflowfailexception"]),
        ("(429)", ["429"]),
        ("Response code 429", ["response", "code", "429"]),
        ("polygon_default", ["polygon_default", "polygon", "default"]),
        ("dt=2026-08-29", ["dt=2026-08-29", "dt", "2026-08-29"]),
        ("2026-08-29", ["2026-08-29"]),
        ("Retry-After: 60", ["retry-after", "retry", "after", "60"]),
        ("max_active_runs=1", ["max_active_runs=1", "max", "active", "runs", "1"]),
        ("requests.HTTPError", ["requests.httperror", "requests", "httperror"]),
        ("ingest_ohlcv_daily failed", ["ingest_ohlcv_daily", "ingest", "ohlcv", "daily", "failed"]),
        ("", []),
        ("--- !! ...", []),
    ],
)
def test_tokenize(text, expected):
    assert tokenize(text) == expected


CORPUS = [
    mk_chunk(0, "fetch_ohlcv raised AirflowFailException on a 401"),
    mk_chunk(1, "the vendor returned 429 Too Many Requests so wait and retry"),
    mk_chunk(2, "validate_partition found duplicate rows"),
    mk_chunk(3, "the task failed"),
]


def test_exact_identifier_ranks_its_chunk_first_and_only():
    hits = BM25Index(CORPUS).search("AirflowFailException")
    assert [h.chunk.ordinal for h in hits] == [0]  # zero-score chunks are excluded
    assert hits[0].rank == 1
    assert hits[0].retriever == "bm25"


def test_status_code_query():
    assert BM25Index(CORPUS).search("429")[0].chunk.ordinal == 1


def test_hand_computed_score():
    # N=2, "apple" in 1 chunk: idf = ln(1 + (2-1+0.5)/(1+0.5)) = ln 2.
    # tf=1, len=2, avgdl=2 -> tf part = 1*(1.5+1)/(1+1.5*1) = 1. Score = ln 2.
    index = BM25Index([mk_chunk(0, "apple banana"), mk_chunk(1, "banana cherry")])
    [hit] = index.search("apple")
    assert hit.score == pytest.approx(math.log(2))


def test_rare_term_outweighs_common_term():
    chunks = [mk_chunk(0, "the rare"), mk_chunk(1, "the common"), mk_chunk(2, "the other")]
    assert BM25Index(chunks).search("the rare")[0].chunk.ordinal == 0


def test_term_frequency_saturates():
    filler = " ".join(f"f{i}" for i in range(8))
    twice = mk_chunk(0, "throttle throttle " + filler)  # 10 tokens, tf=2
    ten = mk_chunk(1, " ".join(["throttle"] * 10))  # 10 tokens, tf=10
    others = [mk_chunk(i, f"unrelated words {i}") for i in range(2, 6)]
    hits = {h.chunk.ordinal: h.score for h in BM25Index([twice, ten, *others]).search("throttle")}
    assert hits[0] < hits[1] < 5 * hits[0]  # 5x the occurrences, well under 5x the score


def test_short_focused_chunk_beats_long_diluted_one():
    short = mk_chunk(0, "throttle vendor")
    long = mk_chunk(1, "throttle " + " ".join(f"pad{i}" for i in range(30)))
    other = mk_chunk(2, "nothing relevant here")
    assert BM25Index([short, long, other]).search("throttle")[0].chunk.ordinal == 0


def test_ties_break_on_chunk_id():
    a = mk_chunk(1, "same words here")
    b = mk_chunk(0, "same words here")
    c = mk_chunk(2, "different")
    hits = BM25Index([a, b, c]).search("same")
    assert [h.chunk.chunk_id for h in hits] == [b.chunk_id, a.chunk_id]
    assert [h.rank for h in hits] == [1, 2]


def test_k_and_empty_query():
    index = BM25Index(CORPUS)
    assert len(index.search("the", k=100)) == 2  # only chunks containing "the"
    assert len(index.search("the", k=1)) == 1
    assert index.search("") == []
    assert index.search("!!!") == []
