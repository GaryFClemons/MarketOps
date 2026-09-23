# Evals

Golden sets for the AI layer, and how to run them. The harness code is in `market_ops/evals/`.

| File | Measures | Cases |
|---|---|---|
| `golden/retrieval.jsonl` | Did the right doc section come back? | 30 (seed) |
| `golden/incident_briefs.jsonl` | Is a triage brief's severity, escalation, citation, and content right? | 6 (seed) |

## How labels work

Retrieval labels are **section-level**: a document plus an exact heading, never a chunk id.

```json
{"doc_id": "docs/runbooks/vendor-auth-throttling-outage.md", "heading": "429 Too Many Requests"}
```

A retrieved chunk is relevant when it comes from that document and the heading appears anywhere in its heading path (`heading: null` means any chunk of the document). Chunk ids change whenever chunk size or overlap changes; headings don't. So the golden set survives exactly the experiments it exists to judge.

One known coarseness: `ROADMAP.md`'s decision log is a single section, so a label on `"Decision log"` accepts any decision row. Row-level labels would need row anchors; not worth it until the "why" questions need finer scoring.

## Tags

| Tag | Query shape | Why it's tracked separately |
|---|---|---|
| `identifier` | Pasted exception text, status codes, partition keys, config names | Where BM25 should win and pure embeddings lose |
| `paraphrase` | The same needs in plain words, with no identifiers | Where embeddings should win and BM25 loses |
| `why` | Design rationale, answered by the decision log or a postmortem | Tests whether history is findable, not only procedures |
| `procedure` | "How do I…" | The runbooks' Resolution sections |
| `multi_hop` | Needs two different sections | Recall@k matters more than MRR here |

The per-tag breakdown is what makes "hybrid beats BM25" a claim you can check: an average can hide "hybrid loses on identifiers".

## Running

```bash
python -m market_ops.evals retrieval --retriever bm25
python -m market_ops.evals retrieval --retriever hybrid
```

Each run prints a markdown table and writes the full report, with every case and every hit, to `evals/results/` (gitignored). The brief eval (`python -m market_ops.evals briefs`) needs the triage agent; see docs/build-order.md.

## Before/after protocol

1. Change one thing: chunk size, tokenizer, fusion constant, embedder, prompt.
2. Run the eval before and after, with the same dataset and the same `k`.
3. Record both tables in `docs/eval_results.md`, with one sentence saying what changed and whether it earned its keep.

If you can't name the change, the numbers can't be compared.

## Adding a case

- Retrieval: append a line to `golden/retrieval.jsonl` with a new `ret-NNN` id. The heading must exist verbatim in the document, so copy it; don't retype it. `load_jsonl` rejects duplicate ids and names the line of any malformed entry.
- Briefs: build the `signal` exactly as `market_ops.alerts` would emit it, including `signal_id = RunSignal.stable_id(...)`. `must_not_mention` should hold the plausible wrong answer, e.g. "increase retries" for a 401.

Lines starting with `//` are comments.
