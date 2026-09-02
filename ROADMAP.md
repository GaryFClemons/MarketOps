---
status: active
last_updated: 2026-09-01
next_action: Week 1 / Days 3–4 (partial) — build `seed_ohlcv_history`, then Variables/Connections, deadline alerts, dynamic mapping
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
| Raw-zone ingestion DAG | Verified end to end; idempotency proven by hash |
| `yfinance` pool (1 slot) | Active, exported to `config/pools.json` |
| Historical seed DAG | Not started — blocked the 90-day backfill |
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
- [x] Expand `TICKERS` from 5 to ~20 *(landed at 73)*
- [x] Enforce the partition invariant: every row in `dt=D` has `date == D` *(see [incident](docs/incidents/2026-08-30-stale-session-partition.md))*

### Days 3–4 — Airflow for real `[~]` *(partial, 2026-08-31 → 2026-09-01)*

- [x] TaskFlow API depth — `fetch_ohlcv` XComs a path, never a DataFrame; the `XComArg` is simultaneously the data flow and the edge
- [x] Retries, exponential backoff, timeouts — and which exceptions bypass retries
- [x] Prove idempotency — same interval run twice, partitions byte-identical by SHA-256
- [x] Backfill mechanism — `airflow backfill create` proven on a 3-day window; `--from-date` maps to session date − 1 day
- [~] 90-day backfill — blocked by vendor throttling, see "opened 2026-09-01" below
- [ ] Flip `catchup=True` and document what breaks
- [ ] Deadline alerts — SLAs were **removed** in Airflow 3; `sla` and `sla_miss_callback` no longer exist
- [ ] Sensors, branching, dynamic task mapping (per-ticker fan-out)
- [ ] Move `TICKERS` to an Airflow Variable; add a Connection for the data source

**Carried over from the 2026-08-30 incident — all closed:**

- [x] Exercise the session-date guard — both branches fired, log evidence captured
- [x] Split `.any()` into all-mismatch (skip) vs some-mismatch (fail, no retry)
- [x] Close the `dt=2026-08-27` gap via backfill, not by hand
- [x] Assert the date/partition-key invariant in `validate_partition` too

**Root cause of that incident, found 2026-08-31:**

- [x] The DAG was requesting a session that had not yet opened. Airflow 3 defaults
  `create_cron_data_intervals=False`, so a bare cron string builds a
  `CronTriggerTimetable` with a **zero-width** data interval — `data_interval_start`
  resolved to the current day at 06:00 UTC (02:00 ET), before the session existed.
  Fixed by declaring the interval explicitly.

**Opened 2026-09-01 — vendor reliability:**

- [ ] `seed_ohlcv_history` DAG — one wide-range request per ticker writing many
  partitions per run (73 requests for 90 days instead of 6,570)
- [ ] Decide the daily-DAG pacing strategy once the throttle clears
- [ ] Extract the date and completeness checks into pure functions with `pytest` coverage
- [ ] Restore `retries: 3` (temporarily 0 while iterating against a throttled vendor)

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
| 2026-08-27 | Daily cron, skip on empty, rather than a weekday cron | A `2-6` cron makes Tuesday's interval start on Saturday, ingesting the wrong session. **Superseded 2026-08-31** — with an explicit 1-day interval the interval width no longer depends on cron spacing, so a weekday cron would now be correct. Daily retained for uniformity |
| 2026-08-27 | Validation split into its own task | Distinguishes "vendor was down" from "data was wrong" at a glance in the UI |
| 2026-08-27 | `auto_adjust=False`, keep close and adj_close | Adjusted prices are retroactively rewritten; raw must preserve what the vendor said that day |
| 2026-08-30 | Postgres published on `127.0.0.1` only | An unqualified port mapping listens on every interface with `airflow`/`airflow` credentials |
| 2026-08-30 | Postgres host port moved to 5433 | A native Postgres already owns 5432 on this machine; container-internal traffic is unaffected |
| 2026-08-30 | Validate the vendor response against the requested session date | yfinance returns the prior session for some closed-market dates and nothing for others; the behaviour can't be predicted, so it must be checked every run |
| 2026-08-30 | Invariant over a market calendar | A weekday check misses holidays and half-days and still trusts the vendor otherwise; one date comparison covers every case with no new dependency |
| 2026-08-31 | Explicit `CronTriggerTimetable(..., interval=timedelta(days=1))` over a bare cron string | Airflow 3 defaults `create_cron_data_intervals=False`, so a bare cron yields a zero-width interval and the session date resolves to the current, unopened session. Declaring the interval keeps the DAG correct independent of a deployment-level config flag |
| 2026-08-31 | Backfill, never `dags trigger -l`, for targeted re-ingestion | Measured, not assumed: a manual run's `data_interval` derives from trigger wall-clock time, not the supplied `logical_date`, so `-l` cannot address a historical partition |
| 2026-08-31 | Metadata DB wiped rather than deleting 115 orphaned example DAGs one by one | Disabling a DAG bundle leaves its `serialized_dag` rows behind; one row referencing an unloadable plugin broke `airflow dags list` entirely. Free to do while no Connection or Variable exists yet |
| 2026-09-01 | Airflow pool (`yfinance`, 1 slot) rather than `max_active_runs` | Measured: backfills ignore the DAG-level limit and default to 10 concurrent runs. A pool binds the limit to the resource, so scheduled, backfill, and manual runs all respect it |
| 2026-09-01 | Completeness gate at 15% missing tickers, enforced before the write | A throttled run silently replaced a complete 73-row partition with 28 rows and stayed green. Overwrite-based idempotency assumes a deterministic source; this vendor isn't one |
| 2026-09-01 | Retryable `ValueError` for missing tickers, `AirflowFailException` for wrong dates | Throttling clears, so it earns the 2/4/8-minute backoff. A vendor returning the wrong session will return the wrong session again, so retrying is waste |
| 2026-09-01 | Keep yfinance; move history into a separate seed DAG | `yfinance.multi._download_impl` loops one HTTP request per symbol \u2014 batching is client-side only, and Yahoo has no historical basket endpoint. Daily load is 73 requests/day (fine); the 90-day backfill was 6,570 (not fine). Flipping the axis to one wide range per ticker makes it 73 |
| 2026-09-01 | `threads=False` on the vendor call | Removed burst concurrency without adding tunables. Established empirically that the remaining constraint is sustained throughput (~6 req/s still throttled), which is what justified changing the request shape instead of tuning it |

---

## Deliberately out of scope

- Cloud deployment. Local Docker is sufficient evidence and costs nothing.
- Streaming ingestion. Batch is the honest shape of this problem and of the target roles.
- Multi-agent orchestration. Complexity without a matching payoff at this scale.
- Real-money or trading-signal logic. This is a data platform, not a strategy.
