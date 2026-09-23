"""Dense (embedding) retrieval over a flat, exact index.

Why a flat numpy index and not pgvector / Chroma / HNSW: the corpus is a few
hundred chunks. Brute-force cosine over that is exact and sub-millisecond.
Approximate nearest-neighbour indexes trade recall for latency, a trade that
only pays at roughly 10^5–10^6 vectors. Adding a vector database here would add
a service, a failure mode, and a recall loss to solve a problem that doesn't
exist yet. The ``Embedder`` protocol is the seam where that changes.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from typing import Protocol

import numpy as np

from market_ops._scaffold import todo
from market_ops.retrieval.types import Chunk, SearchHit


class Embedder(Protocol):
    """Texts in, one L2-normalized row per text out: shape ``(len(texts), dim)``."""

    dim: int

    def embed(self, texts: Sequence[str]) -> np.ndarray: ...


class HashingEmbedder:
    """Deterministic hashed bag-of-words. **Not semantic.** Implemented.

    It exists so the dense path, the hybrid fusion, and the eval harness can
    all run offline, in tests, for free — and give the same vectors on every
    machine. It knows nothing about meaning: "throttled" and "rate limited"
    share no dimensions. Any claim about *semantic* retrieval quality needs a
    real model (``ProviderEmbedder``).

    ``hashlib`` rather than ``hash()``: Python salts ``hash()`` per process, so
    vectors would change between runs and the eval would stop being repeatable.
    """

    _WORD = re.compile(r"[a-z0-9]+")

    def __init__(self, dim: int = 512) -> None:
        self.dim = dim

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for row, text in enumerate(texts):
            for word in self._WORD.findall(text.lower()):
                digest = hashlib.blake2b(word.encode(), digest_size=8).digest()
                h = int.from_bytes(digest, "little")
                # Signed hashing trick: a second bit picks the sign so colliding
                # words tend to cancel instead of always adding up.
                sign = 1.0 if (h >> 63) & 1 else -1.0
                out[row, h % self.dim] += sign
            norm = np.linalg.norm(out[row])
            if norm > 0:
                out[row] /= norm
        return out


class ProviderEmbedder:
    """Embeddings from a hosted model named by ``Settings.embedding_model``.

    ``EMBEDDING_MODEL`` in .env.example names the model; the provider SDK is
    imported inside ``embed`` so nothing here needs it installed until used.
    Embed chunks once at index build and cache the matrix — re-embedding the
    corpus on every query would dominate both cost and latency.
    """

    def __init__(self, model: str, dim: int) -> None:
        self.model = model
        self.dim = dim

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        todo("ProviderEmbedder.embed: call the embedding API in batches, return L2-normalized rows", tier="next")


def contextualize(chunk: Chunk) -> str:
    """Text to embed for a chunk: ``"<title> > <section>\\n\\n<text>"``.

    Contextual retrieval: a chunk that reads "Wait 60 seconds, then retry" means
    nothing on its own; prefixed with "Vendor auth, throttling and outages >
    Diagnosis > 429 Too Many Requests" it is findable. Applied at index time
    only — ``Chunk.text`` stays a faithful quote of the source.
    """
    todo("contextualize: prefix title and section path to the chunk text for embedding", tier="next")


class DenseIndex:
    """Exact cosine search over embedded chunks."""

    def __init__(self, chunks: Sequence[Chunk], embedder: Embedder) -> None:
        self.chunks = list(chunks)
        self.embedder = embedder
        # Rows are L2-normalized, so cosine similarity is a plain dot product.
        # TODO(next): embed contextualize(c) instead of c.text once it exists,
        # then re-run the retrieval eval to see whether it earned its keep.
        self.matrix = embedder.embed([c.text for c in self.chunks]) if self.chunks else np.zeros(
            (0, embedder.dim), dtype=np.float32
        )

    def search(self, query: str, k: int = 5) -> list[SearchHit]:
        """Top-``k`` chunks by cosine similarity to ``query``.

        Returns ``SearchHit(retriever="dense")``, 1-based ranks, highest
        similarity first, ties broken by ``chunk_id`` ascending. Chunks with
        similarity <= 0 are excluded (no shared signal at all). ``k`` larger than
        the corpus returns every positive match.

        Hints: embed the query (one row), ``self.matrix @ q``, then sort.
        """
        todo("DenseIndex.search: embed query, dot with the matrix, drop <= 0, sort (-score, chunk_id), top k")
