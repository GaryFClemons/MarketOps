---
status: active
last_updated: 2026-08-30
next_action: Week 1 / Days 3–4 — TaskFlow depth, retries/SLA, idempotency proof, 90-day backfill
---

# market-ops-platform — Roadmap

A single system with a **data layer** (ingest → orchestrate → transform → warehouse → quality checks) and an **AI layer** (retrieval over operational docs → triage agent → eval harness).

Legend: `[x]` done · `[~]` in progress · `[ ]` not started

---

## Current state

| Component | Status |
|---|---|
| Dockerized Airflow 3.3.1 (CeleryExecutor + Postgres + Redis) | Running |
| Custom image (`marketops/airflow:dev`) | Building |
| Raw-zone ingestion DAG | Verified end to end |
| dbt project | Not started |
| DuckDB warehouse | Not started |
| Retrieval layer | Not started |
| Triage agent | Not started |
| Eval harness | Not started |

---

## Week 1 — Orchestration, containers, SQL

**Objective:** Airflow and Docker are operated, not read about. SQL is fast under observation.

### Days 1–2 — Environment and ingestion `[x]` *(2026-08-27)*

- [x] Public repo, MIT license, README, `.gitignore`
- [x] `.env.example` aligned to the Airflow 3.3.1 compose contract
- [x] Docker fundamentals — images vs containers, layers, volumes, networks
- [x] Airflow stack up via Docker Compose, UI reachable
- [x] Custom `Dockerfile` extending `apache/airflow:3.3.1`, deps before source for layer caching
- [x] `dags/ingest_ohlcv_daily.py` — idempotent, date-partitioned OHLCV ingestion
- [x] Raw zone mounted to host at `data/raw/ohlcv/dt=YYYY-MM-DD/`
- [ ] Expand `TICKERS` from 5 to ~20

### Days 3–4 — Airflow for real `[ ]`

- [ ] TaskFlow API depth; minimize XComs and be able to say why
- [ ] Retries, exponential backoff, timeouts, SLA misses
- [ ] Prove idempotency — run the same interval twice, diff the partition
- [ ] Flip `catchup=True`, backfill 90 days, document what broke
- [ ] Sensors, branching, dynamic task mapping (per-ticker fan-out)
- [ ] Move `TICKERS` to an Airflow Variable; add a Connection for the data source

### Days 5–6 — SQL drills + partitioning `[ ]`

- [ ] 1 hour daily, timed, no autocomplete
- [ ] Window functions, multi-CTE decomposition, dedupe, gaps-and-islands, anti-joins, date spines
- [ ] `EXPLAIN` a slow query; make one measurably faster
- [ ] Document why partition layout dominates query cost

**Deliverable:** Working Dockerized Airflow + idempotent, backfillable ingestion DAG. README explains design decisions, not setup steps.

---

## Week 2 — Transformation, modeling, Python screen

- [ ] dbt Core: sources, models, refs, materializations
- [ ] Staging → intermediate → marts over raw market data
- [ ] At least one incremental model with late-arriving-data handling
- [ ] Tests: `unique`, `not_null`, `relationships`, `accepted_values`, plus one custom
- [ ] Star schema design; **state the grain of every fact table**
- [ ] SCD Type 2 implementation in dbt
- [ ] `docs/data_model.md` — schema and grain rationale
- [ ] Python drills, 1 hour daily, timed and spoken aloud
- [ ] `pytest` coverage on transformation helpers

**Deliverable:** Layered dbt project, tests passing, documented lineage, one incremental model.

---

## Week 3 — Retrieval

- [ ] Embeddings, cosine similarity, dimensionality
- [ ] Vector store (pgvector or Chroma); HNSW vs flat, recall/latency tradeoff
- [ ] Chunking strategy — defensible size and overlap, not defaults
- [ ] Index runbooks, schema docs, pipeline logs
- [ ] Hybrid search (BM25 + dense) — required for tickers and error codes
- [ ] Reranking over a wider candidate set
- [ ] Golden dataset, 30–50 question/answer pairs
- [ ] Retrieval metrics: precision@k, recall@k, MRR
- [ ] Generation metrics: faithfulness, groundedness, relevance
- [ ] LLM-as-judge harness with a stronger judge model than the generator
- [ ] Cost and latency tracked per query
- [ ] `docs/eval_results.md` — measured before/after for one improvement

**Deliverable:** Hybrid retrieval + reranking over project docs, with a reporting eval harness.

---

## Week 4 — Agents, integration, readiness

- [ ] Tool use / function calling; schema design and error recovery
- [ ] Agent loop (ReAct or plan-and-execute) with guardrails and blast-radius limits
- [ ] Triage agent: detect failed or SLA-at-risk run → retrieve context → emit incident brief
- [ ] Wire AI layer to data layer; one command stands up the whole system
- [ ] `docs/architecture.md` with a real diagram
- [ ] README as a landing page; repo pinned on GitHub profile
- [ ] One public post on a real technical decision from this project

**Deliverable:** Running end-to-end system, publicly visible, documented, with an eval harness.

---

## Decision log

Design choices already made and the reasoning, kept here so they can be defended out loud.

| Date | Decision | Why |
|---|---|---|
| 2026-08-27 | Airflow 3.3.1 via official Docker Compose | CeleryExecutor + Redis mirrors a realistic deployment; local install isn't supported on Windows |
| 2026-08-27 | Custom image over `_PIP_ADDITIONAL_REQUIREMENTS` | Reproducible builds; the env var reinstalls on every container start |
| 2026-08-27 | Deps copied before source in the Dockerfile | A DAG edit must not invalidate the pip layer |
| 2026-08-27 | Hive-style `dt=YYYY-MM-DD` partitions | Engines prune whole directories; gives a deterministic re-run target |
| 2026-08-27 | Write-to-tmp then atomic rename | Crash safety plus overwrite-based idempotency; makes aggressive retries safe |
| 2026-08-27 | Session date from `data_interval_start`, never `now()` | Makes the task a pure function of its interval, which is what makes backfill correct |
| 2026-08-27 | Daily cron, skip on empty, rather than a weekday cron | A `2-6` cron makes Tuesday's interval start on Saturday, ingesting the wrong session |
| 2026-08-27 | Validation split into its own task | Distinguishes "vendor was down" from "data was wrong" at a glance in the UI |
| 2026-08-27 | `auto_adjust=False`, keep close and adj_close | Adjusted prices are retroactively rewritten; raw must preserve what the vendor said that day |
| 2026-08-30 | Postgres published on `127.0.0.1` only | An unqualified port mapping listens on every interface with `airflow`/`airflow` credentials |

---

## Deliberately out of scope

- Cloud deployment. Local Docker is sufficient evidence and costs nothing.
- Streaming ingestion. Batch is the honest shape of this problem and of the target roles.
- Multi-agent orchestration. Complexity without a matching payoff at this scale.
- Real-money or trading-signal logic. This is a data platform, not a strategy.
