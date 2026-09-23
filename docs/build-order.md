# Build order

The scaffold is in place: every module's contracts, docstrings and tests exist, and every piece of core logic is a `todo()` stub. This is the order to write them in. Each block ends with something runnable.

**How to tell where you are:**

```bash
pytest -rs                      # the backlog: every unwritten function is a "TODO [core] ..." skip
pytest --todo-fail tests/quality  # the same items as red failures, one area at a time
grep -rn "todo(" market_ops     # the backlog in code order
```

A stub's tests are already written against its docstring. Implement the body, and the same tests start running for real. "Done" means the listed tests pass.

Estimates are for writing the function against its docstring, not for reading about the topic first.

---

## 1. Quality checks, then wire them into the DAG (~3h)

Pure functions extracted from `dags/ingest_ohlcv_daily.py`: no Airflow imports, verdicts instead of exceptions. Tests: [tests/quality/test_checks.py](../tests/quality/test_checks.py).

| Function | Est. | Answers out loud |
|---|---|---|
| [`classify_status`](../market_ops/quality/checks.py) | 10m | "Which failures should retry, and who decides?" |
| [`throttle_wait_seconds`](../market_ops/quality/checks.py) | 10m | "How do you handle rate limits?" |
| [`classify_payload`](../market_ops/quality/checks.py) | 10m | "How do you tell 'market closed' from 'vendor broke'?" |
| [`find_date_mismatches`](../market_ops/quality/checks.py) | 20m | "Tell me about a time the data was wrong." (2026-08-30) |
| [`assess_completeness`](../market_ops/quality/checks.py) | 20m | "How do you detect partial loads?" (2026-09-01) |
| [`validate_partition_frame`](../market_ops/quality/checks.py) | 30m | "How do you test data quality in a pipeline?" |
| [`partition_date_from_path`](../market_ops/quality/checks.py) | 10m | "What's your partitioning contract?" |

Done when `pytest tests/quality` has no skips. Then wire the checks into the DAG; see [Wiring into the DAG](#wiring-into-the-dag-owner-writes-dag-code).

## 2. Alerts: failure and deadline callbacks (~2h)

The bridge from Airflow to the AI layer: callbacks write `RunSignal` JSON files. Tests: [tests/alerts/](../tests/alerts/).

| Function | Est. | Answers out loud |
|---|---|---|
| [`build_failure_signal`](../market_ops/alerts/failures.py) | 20m | "How does a failure reach whoever triages it?" |
| [`on_task_failure`](../market_ops/alerts/failures.py) | 10m | "What happens when the alerting itself breaks?" |
| [`build_deadline_signal`](../market_ops/alerts/deadlines.py) | 20m | "How do you monitor SLAs in Airflow 3?" |
| [`on_deadline_missed`](../market_ops/alerts/deadlines.py) | 15m | "Why must an async callback never block the event loop?" |
| [`make_deadline_alert`](../market_ops/alerts/deadlines.py) | 15m | "Control-M SLAs vs Airflow deadlines: what transfers?" |

Done when `pytest tests/alerts` has no skips. Then wire both into the DAG, run `docker compose up -d` to pick up the mount, and force one failure to see a real signal land in `data/ops/signals/`.

## 3. Retrieval (~3.5h)

Tests: [tests/retrieval/](../tests/retrieval/).

| Function | Est. | Answers out loud |
|---|---|---|
| [`tokenize`](../market_ops/retrieval/bm25.py) | 30m | "Why does pure vector search fail on error codes?" |
| [`BM25Index._build`](../market_ops/retrieval/bm25.py) | 30m | "Walk me through BM25: idf, k1, b." |
| [`BM25Index.search`](../market_ops/retrieval/bm25.py) | 30m | same |
| [`reciprocal_rank_fusion`](../market_ops/retrieval/hybrid.py) | 20m | "Why hybrid, and how do you combine scores on different scales?" |
| [`DenseIndex.search`](../market_ops/retrieval/dense.py) | 15m | "Flat index vs HNSW: when does ANN pay?" |
| [`chunk_document`](../market_ops/retrieval/chunking.py) | 1.5h | "Defend your chunk size and overlap." |

Done when `pytest tests/retrieval` has only `[next]` skips. `test_service.py` then searches the real docs end to end.

## 4. Retrieval evals: the first real numbers (~1.5h)

Tests: [tests/evals/](../tests/evals/).

| Function | Est. | Answers out loud |
|---|---|---|
| [`precision_at_k`](../market_ops/evals/metrics.py) | 10m | "How do you know retrieval is working?" |
| [`recall_at_k`](../market_ops/evals/metrics.py) | 10m | same |
| [`reciprocal_rank`](../market_ops/evals/metrics.py) | 10m | same |
| [`run_retrieval_eval`](../market_ops/evals/harness.py) | 45m | "What did you measure, and what changed?" |

Then run and record in [docs/eval_results.md](eval_results.md):

```bash
python -m market_ops.evals retrieval --retriever bm25
python -m market_ops.evals retrieval --retriever hybrid
```

Be precise about the embedder. `HashingEmbedder` has no semantics, so "hybrid" with it is BM25 plus noise. For a real BM25-vs-hybrid comparison, implement `ProviderEmbedder.embed` (next tier, ~30m) first. Otherwise record BM25 alone and say why.

## 5. The triage agent (~8h)

Tests: [tests/agent/](../tests/agent/). Order matters: the loop is last because it calls everything else.

| Function | Est. | Answers out loud |
|---|---|---|
| [`fence_untrusted`](../market_ops/agent/guardrails.py) | 45m | "How do you handle prompt injection from data?" |
| [`validate_brief`](../market_ops/agent/guardrails.py) | 1h | "How do you stop hallucinated citations?" |
| [`AuditLog.append`](../market_ops/agent/audit.py) | 15m | "How is this auditable?" |
| [`read_task_log`](../market_ops/agent/tools.py) | 1h | "How do you give an agent file access safely?" |
| [`check_partition`](../market_ops/agent/tools.py) | 30m | "What can the agent verify first-hand?" |
| [`search_docs`](../market_ops/agent/tools.py) | 20m | "How does retrieval feed the agent?" |
| [`get_run_summary`](../market_ops/agent/tools.py) | 20m | "What context does the agent get, and what does it not?" |
| [`dispatch`](../market_ops/agent/tools.py) | 45m | "What happens when the model calls a tool wrong?" |
| [`run_triage`](../market_ops/agent/triage.py) | 2h | "Walk me through your agent loop and its guardrails." |
| **one of** [`AnthropicClient.complete`](../market_ops/agent/providers.py) / [`OpenAIClient.complete`](../market_ops/agent/providers.py) | 1h | "How do you keep the vendor swappable?" |

`read_task_log` has a real gotcha documented in its docstring: on this Windows host, `:` in run_id directory names is stored as U+F03A.

Done when `pytest tests/agent` has no core skips.

## 6. Brief scoring and the end-to-end demo (~1.5h)

| Function | Est. | Answers out loud |
|---|---|---|
| [`score_brief`](../market_ops/evals/harness.py) | 30m | "Deterministic checks vs LLM-as-judge: when each?" |

The demo, offline and free:

```bash
python -m market_ops.agent demo-signal
python -m market_ops.agent triage --latest --scripted tests/agent/fixtures/demo_transcript.json
```

Then live: set `LLM_PROVIDER` and `LLM_MODEL`, install the SDK, and drop `--scripted`. Every step lands in `data/ops/audit/triage.jsonl`.

Cut line if time runs short: the scripted demo exercises the real loop, tools, guardrails and audit log. Present that, and say plainly which provider adapter is not yet written.

---

## Wiring into the DAG (owner writes DAG code)

The package is importable in every Airflow container once compose is recreated: `docker-compose.yaml` mounts `./market_ops` at `/opt/airflow/src/market_ops` and sets `PYTHONPATH=/opt/airflow/src`. Run `docker compose up -d` once after pulling this change.

**Imports.** `market_ops.quality.checks` imports pandas, so import it inside the task bodies, like the DAG's existing `import pandas as pd`. That keeps it off the dag-processor's parse path. `market_ops.alerts` is light and can be imported at module top.

**Checks** (`fetch_ohlcv`):

| DAG block today | Replace with | Verdict → exception |
|---|---|---|
| status-code `if/elif` chain | `classify_status(response.status_code)` | FAIL_FAST → `AirflowFailException`; RETRY → `requests.HTTPError`; THROTTLED → the wait loop |
| `Retry-After` parsing in the 429 loop | `throttle_wait_seconds(response.headers.get("Retry-After"))` | (a value) |
| `if not results` / `len(results) < 8000` | `classify_payload(payload.get("results"))` | MARKET_CLOSED → `AirflowSkipException`; TRUNCATED → `AirflowFailException` |
| `mismatched = ...` block | `find_date_mismatches(df_tick, session_date)` | non-empty → `AirflowFailException` |
| `missing = set(tickers) - ...` block | `assess_completeness(tickers, df_tick["ticker"])` | `should_fail` → `ValueError`; any miss → `log.warning` |
| `OHLCV_COLUMNS` constant | import it from `market_ops.quality.checks` | then delete `tests/quality/test_schema_parity.py` |

**Checks** (`validate_partition`): replace the body with `partition_date_from_path(path)` plus `validate_partition_frame(df, part_date)`, and raise when the returned list is non-empty.

**Alerts** (DAG definition):
- `default_args["on_failure_callback"] = on_task_failure`
- `@dag(..., deadline=make_deadline_alert())`

## Later design note: sensors, branching, dynamic task mapping

These don't belong in the ingestion DAG: grouped-daily removed the per-ticker fan-out. They have an honest home in a separate **quality-sweep DAG**:

- a sensor waits on the day's partition
- `.expand()` maps a check over each date in the spine (dynamic task mapping)
- a branch routes to "emit `partition_missing` signal" or "all clear"

That would also make `sql/date-spine-anti-join.sql` a scheduled control rather than a drill.

---

## Next tier (after the core works)

| Item | Why |
|---|---|
| [`ProviderEmbedder.embed`](../market_ops/retrieval/dense.py) | Real semantics; without it "hybrid" is BM25 plus noise |
| [`contextualize`](../market_ops/retrieval/dense.py) | Contextual retrieval: prefix title and section before embedding |
| [`split_markdown_table`](../market_ops/retrieval/chunking.py) | Keeps each decision-log row whole |
| [`LLMReranker.rerank`](../market_ops/retrieval/rerank.py) | Only if the eval shows ranking errors fusion doesn't fix |
| [`compare_reports`](../market_ops/evals/harness.py) | Before/after tables without hand copying |
| [`judge_brief`](../market_ops/evals/judge.py) | LLM-as-judge, calibrated against hand-graded briefs first |
| [`cost_usd`](../market_ops/evals/cost.py) | Cost per brief as a first-class metric (fill `PRICING` from the provider's page) |
| [`AirflowApiRunSource`](../market_ops/agent/tools.py) | Live run state instead of a fixture file |
| `python -m market_ops.evals briefs` | Brief evals over `evals/golden/incident_briefs.jsonl` |

## Later tier

- dbt: `daily_return`, lookback sizing, holiday vs gap in `dim_date`, SCD2 backdating ([dbt/README.md](../dbt/README.md), [docs/data_model.md](data_model.md)).
- One measured `EXPLAIN ANALYZE` improvement ([docs/query-cost.md](query-cost.md)).
- The quality-sweep DAG above.
