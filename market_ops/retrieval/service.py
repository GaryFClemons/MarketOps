"""One place that assembles a retriever from the repo's docs. Implemented wiring.

The agent's ``search_docs`` tool and the eval CLI both call ``build_retriever``,
so the thing being evaluated is exactly the thing the agent uses.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from market_ops.config import REPO_ROOT
from market_ops.retrieval.bm25 import BM25Index
from market_ops.retrieval.chunking import chunk_corpus
from market_ops.retrieval.corpus import load_corpus
from market_ops.retrieval.dense import DenseIndex, Embedder, HashingEmbedder
from market_ops.retrieval.hybrid import HybridRetriever
from market_ops.retrieval.types import Retriever

Mode = Literal["bm25", "dense", "hybrid"]


def build_retriever(
    mode: Mode = "hybrid",
    *,
    root: Path = REPO_ROOT,
    embedder: Embedder | None = None,
    **chunk_kwargs,
) -> Retriever:
    """Load the corpus, chunk it, and build the requested retriever.

    ``embedder`` defaults to ``HashingEmbedder`` — an offline stand-in with no
    semantics. Numbers measured with it say nothing about dense retrieval
    quality; pass a ``ProviderEmbedder`` for that, and say which one was used
    when recording results.
    """
    if mode not in ("bm25", "dense", "hybrid"):
        raise ValueError(f"unknown retriever mode {mode!r}; expected bm25, dense or hybrid")
    chunks = chunk_corpus(load_corpus(root), **chunk_kwargs)
    if mode == "bm25":
        return BM25Index(chunks)
    dense = DenseIndex(chunks, embedder or HashingEmbedder())
    if mode == "dense":
        return dense
    return HybridRetriever(BM25Index(chunks), dense)
