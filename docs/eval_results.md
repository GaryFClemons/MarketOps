# Eval results

Measured results only. Every table here comes from a command someone ran, and names what changed between runs. Nothing is recorded yet: the metrics and the harness are still being written (docs/build-order.md).

## Method

- **Retrieval:** `python -m market_ops.evals retrieval --retriever <bm25|dense|hybrid> --k 5` over `evals/golden/retrieval.jsonl` (30 cases, section-level labels). Metrics: P@5, R@5, MRR, p50/p95 search latency, broken down by query tag.
- **Briefs:** deterministic scoring (severity, escalation, cited runbooks, required and forbidden content) over `evals/golden/incident_briefs.jsonl`, then LLM-as-judge for faithfulness, once the judge has been checked against hand-graded briefs.
- **Embedder:** state which one. `HashingEmbedder` is an offline stand-in with no semantics; numbers from it say nothing about semantic retrieval.

## Retrieval: BM25 vs hybrid

TBD. Run `python -m market_ops.evals retrieval --retriever bm25` and `--retriever hybrid`, then paste both tables.

| Retriever | Embedder | P@5 | R@5 | MRR | p50 ms | p95 ms |
|---|---|---|---|---|---|---|
| bm25 | n/a | | | | | |
| hybrid | | | | | | |

Per tag (MRR):

| Tag | bm25 | hybrid |
|---|---|---|
| identifier | | |
| paraphrase | | |
| why | | |
| procedure | | |
| multi_hop | | |

What changed, and did it earn its keep: TBD.

## Brief quality

TBD. Needs the triage agent and `score_brief`.

| Case | Severity | Human | Cite recall | Missing mentions | Forbidden | Pass |
|---|---|---|---|---|---|---|
| | | | | | | |

Model, prompt version, cost per brief, and latency per brief: TBD.
