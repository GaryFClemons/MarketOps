"""Retrieval over the platform's own operational docs.

Corpus: runbooks, incident postmortems, the ROADMAP decision log, the codebase
map. Small (hundreds of chunks), dense with exact identifiers — ``dt=2026-08-29``,
``AirflowFailException``, ``429``, ``polygon_default`` — which is why BM25 is a
first-class retriever here and not an afterthought to embeddings.

Pipeline: ``corpus.load_corpus`` -> ``chunking.chunk_document`` ->
``bm25.BM25Index`` + ``dense.DenseIndex`` -> ``hybrid.reciprocal_rank_fusion``.
"""
