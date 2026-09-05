---
status: active
last_updated: 2026-09-05
next_action: Week 1 / Days 3–4 — flip `catchup=True`, deadline alerts, sensors/branching/dynamic mapping, extract checks into pure functions with pytest
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
| Market-data vendor | **Polygon** grouped-daily bars (migrated off yfinance 2026-09-02 → 09-04) |
| `polygon` pool (1 slot) | Active, exported to `config/pools.json`; the `yfinance` entry is vestigial and should be dropped |
| Variable `tickers` + Connection `polygon_default` | Live; connection Fernet-encrypted and masked in task logs |
| Raw zone | 65 single-vendor partitions, 2026-06-04 → 2026-09-04 |
| Historical seed DAG | **Obsolete** — grouped-daily makes 90 days = 90 requests, not 6,570 |
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

### Days 3–4 — Airflow for real `[~]` *(2026-08-31 → 2026-09-05)*

- [x] TaskFlow API depth — `fetch_ohlcv` XComs a path, never a DataFrame; the `XComArg` is simultaneously the data flow and the edge
- [x] Retries, exponential backoff, timeouts — and which exceptions bypass retries
- [x] Prove idempotency — same interval run twice, partitions byte-identical by SHA-256
- [x] Backfill mechanism — `airflow backfill create`; the window must **span a cron fire time**, and `session_date = fire_time − interval`
- [x] 90-day backfill — **done 2026-09-04**, 65 partitions, one request per session
- [x] Move `TICKERS` to an Airflow Variable; add a Connection for the data source *(2026-09-03)*
- [ ] Flip `catchup=True` and document what breaks *(will queue exactly 3 runs: sessions 06-01 → 06-03)*
- [ ] Deadline alerts — SLAs were **removed** in Airflow 3; `sla` and `sla_miss_callback` no longer exist
- [ ] Sensors, branching, dynamic task mapping — **note:** grouped-daily removed the per-ticker fan-out rationale; map over dates or build it as a separate exercise DAG rather than forcing it into the ingestion path

**Vendor migration, 2026-09-02 → 2026-09-05 — closed:**

- [x] Polygon grouped-daily replaces per-symbol yfinance calls. Daily load: 1 request. 90-day backfill: 90 requests
- [x] API key moved into Fernet-encrypted Connection `polygon_default`; removed from `.env` entirely
- [x] `tickers` moved to a JSON Airflow Variable; both reads happen inside the task body
- [x] Explicit HTTP status taxonomy replaces inferred vendor errors
- [x] Schema changed: `adj_close` dropped, `vwap` and `trade_count` added
- [x] Wrong-session-date failure class designed out — you request a date and get that date or nothing
- [x] Reconciled Polygon against yfinance on the overlapping `dt=2026-09-02` partition
- [x] `retries: 3` restored; `yfinance` pool renamed to `polygon`
- [x] Raw zone purged of yfinance partitions and re-seeded; single schema verified by a DuckDB scan across all 65

**Still open, opened or carried into 2026-09-05:**

- [ ] Record the measured reconciliation deltas (max/median relative difference on `close` and `volume`). Currently only qualitative — "close almost identical, volume close" is not a measurement
- [ ] Daily-DAG / backfill pacing strategy — a pool caps concurrency, not throughput (see decision log 2026-09-04)
- [ ] Extract the date and completeness checks into pure functions with `pytest` coverage
- [ ] `ANSS` removed from the universe (acquired by Synopsys, already held as `SNPS`); universe is 73. Recurring missing tickers are a list-maintenance bug, not a data condition

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

**Opened 2026-09-01 — vendor reliability — all resolved by the Polygon migration:**

- [x] ~~`seed_ohlcv_history` DAG~~ — **struck 2026-09-02.** Grouped-daily makes the seed
  unnecessary: 90 days is 90 requests, and each run writes exactly one partition, which
  is the shape Airflow already models
- [x] Daily-DAG pacing — resolved at the request-shape level: 1 request/day
- [x] Restore `retries: 3`

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
| 2026-08-27 | `auto_adjust=False`, keep close and adj_close | Adjusted prices are retroactively rewritten; raw must preserve what the vendor said that day. **Superseded 2026-09-02** — the principle survives the vendor change (`adjusted=false` on Polygon), but `adj_close` no longer exists as a column; adjustments now come from Polygon's splits/dividends endpoints if ever needed |
| 2026-08-30 | Postgres published on `127.0.0.1` only | An unqualified port mapping listens on every interface with `airflow`/`airflow` credentials |
| 2026-08-30 | Postgres host port moved to 5433 | A native Postgres already owns 5432 on this machine; container-internal traffic is unaffected |
| 2026-08-30 | Validate the vendor response against the requested session date | yfinance returns the prior session for some closed-market dates and nothing for others; the behaviour can't be predicted, so it must be checked every run |
| 2026-08-30 | Invariant over a market calendar | A weekday check misses holidays and half-days and still trusts the vendor otherwise; one date comparison covers every case with no new dependency |
| 2026-08-31 | Explicit `CronTriggerTimetable(..., interval=timedelta(days=1))` over a bare cron string | Airflow 3 defaults `create_cron_data_intervals=False`, so a bare cron yields a zero-width interval and the session date resolves to the current, unopened session. Declaring the interval keeps the DAG correct independent of a deployment-level config flag |
| 2026-08-31 | Backfill, not a bare manual trigger, for targeted re-ingestion | **Corrected 2026-09-05.** The original claim — that a manual run's `data_interval` derives from trigger wall-clock time — is **wrong**. Measured against `dags list-runs`: a manual run's interval derives from its `logical_date`, so `session_date = logical_date − interval`. A run triggered at 09-04T06:54 with `logical_date` 09-03T04:55 wrote `dt=2026-09-02`. Manual runs *can* address history. Backfill remains the right tool for **ranges**, not because manual triggers can't reach the past |
| 2026-09-05 | Backfill `--from-date`/`--to-date` are timestamps, and the window must span a cron fire time | Measured: `--from-date 2026-09-02 --to-date 2026-09-02` parsed to a zero-width range at midnight, contained no 06:00 fire time, and created **no runs** with only a warning. Backfill replays fire times inside a window; it does not accept a list of dates |
| 2026-08-31 | Metadata DB wiped rather than deleting 115 orphaned example DAGs one by one | Disabling a DAG bundle leaves its `serialized_dag` rows behind; one row referencing an unloadable plugin broke `airflow dags list` entirely. Free to do while no Connection or Variable exists yet |
| 2026-09-01 | Airflow pool (1 slot) rather than `max_active_runs` | Measured: backfills ignore the DAG-level limit and default to 10 concurrent runs. A pool binds the limit to the resource, so scheduled, backfill, and manual runs all respect it. *(Renamed `yfinance` → `polygon` on 2026-09-04.)* |
| 2026-09-01 | Completeness gate enforced before the write | A throttled run silently replaced a complete 73-row partition with 28 rows and stayed green. Overwrite-based idempotency assumes a deterministic source; yfinance wasn't one. **Threshold raised to 20% and its meaning changed 2026-09-03** — see the grouped-daily entry below |
| 2026-09-01 | Retryable `ValueError` for missing tickers, `AirflowFailException` for wrong dates | Throttling clears, so it earns the 2/4/8-minute backoff. A vendor returning the wrong session will return the wrong session again, so retrying is waste |
| 2026-09-01 | Keep yfinance; move history into a separate seed DAG | `yfinance.multi._download_impl` loops one HTTP request per symbol \u2014 batching is client-side only, and Yahoo has no historical basket endpoint. Daily load is 73 requests/day (fine); the 90-day backfill was 6,570 (not fine). Flipping the axis to one wide range per ticker makes it 73. **Superseded 2026-09-02** \u2014 switched vendor instead of reshaping the yfinance request; the seed DAG was never built |
| 2026-09-01 | `threads=False` on the vendor call | Removed burst concurrency without adding tunables. Established empirically that the remaining constraint is sustained throughput (~6 req/s still throttled), which is what justified changing the request shape instead of tuning it. **Obsolete 2026-09-02** \u2014 no yfinance call remains |
| 2026-09-02 | Migrate to Polygon grouped-daily rather than pace yfinance | `/v2/aggs/grouped/locale/us/market/stocks/{date}` returns the entire US market in one request. Daily load goes 73 → 1; the 90-day backfill goes 6,570 → 90. Pacing yfinance would have made a bad request shape survivable; changing vendor made the problem not exist. It also deletes a failure class: you ask for a date and get that date or nothing, so "vendor returned the wrong session" is unrepresentable |
| 2026-09-02 | `adjusted=false` on the request | Same reasoning as the old `auto_adjust=False`: the raw zone must preserve what the vendor said on the day. Adjusted prices are retroactively rewritten by splits and dividends |
| 2026-09-02 | Plain `requests` over `polygon-api-client` | One endpoint. Keeping the HTTP layer visible is what makes the status-code → retry mapping explicit and defensible instead of buried in a library's error hierarchy |
| 2026-09-02 | `Authorization: Bearer` header, never `?apiKey=` | Query strings survive in proxy logs, exception messages, and shell history, where Airflow's secrets masker cannot reach them |
| 2026-09-03 | API key in a Fernet-encrypted Connection; `tickers` in a plain Variable | Two different reasons, not one. The key is a **secret** — the Connection encrypts it at rest and `SecretsMasker` redacts the `password` field in task logs (verified: `KEY CHECK: ***`). The ticker list is an **operational parameter** — it lives in a Variable so it can be edited without a container restart. `POLYGON_API_KEY` was deleted from `.env` outright rather than kept as a second copy |
| 2026-09-03 | Both config reads happen inside the task body | In Airflow 3 task code has no metadata-DB access and resolves these over the Task Execution API. A module-level read would cost the dag-processor a round trip *and* a Fernet decrypt on every ~30s parse |
| 2026-09-03 | Env-var config shadows the metadata DB, silently | `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` take precedence over DB rows with no warning and no UI indication. Resolution order is secrets backend → environment → metadata DB. Verified DB-backed via the `id` column in `connections get` (env-var connections have no row id) |
| 2026-09-03 | Connection stores `host` and scheme in separate fields | Putting `https://api.polygon.io` in `host` produced `http://https://api.polygon.io` from `get_uri`, and the DAG logged the doubled scheme on first run. `host=api.polygon.io` + `schema=https` composes correctly and stays compatible with provider hooks |
| 2026-09-03 | Skip on `not payload.get("results")`, not on `resultsCount` | A weekend returns `{"resultsCount":0,"status":"OK"}` with **no `results` key at all** — `payload["results"]` would raise `KeyError` every Saturday. `resultsCount` is vendor metadata; `results` is the data. Trust the payload, not the description of it |
| 2026-09-03 | Market-size gate (`len(results) < 8000` → fail) as the real systemic check | A truncated response could contain all 73 tracked tickers and still be badly wrong. The ticker-level gate cannot see that; a market-size assertion can. ~12,500 rows is the observed normal |
| 2026-09-03 | Completeness gate loosened to 20% and reinterpreted | Under grouped-daily a partial response is not possible — you get the market or nothing. A missing ticker now means it didn't trade, or the symbol is dead. The gate therefore measures **list drift**, not vendor reliability. Failing on any miss would discard 72 good tickers over one halted name; the warning is the signal, and any ticker warned two days running is a list-maintenance bug to fix, not a threshold to raise |
| 2026-09-03 | Completeness check removed from `validate_partition` | `validate_partition` should be a **pure function of the partition**. Every other check re-derives from the file on disk; the completeness check was the only one needing external state, which is the tell that it belongs to the vendor exchange (`fetch_ohlcv`), not the artifact. The atomic rename sits between the two, so nothing can change in between |
| 2026-09-03 | Date sourced from `data_interval_start`; Polygon's `t` demoted to an assertion | `t` is the bar's close in epoch-ms (measured: `1788379200000` = 2026-09-02T20:00Z = 16:00 ET). Deriving the date from it would make the partition key a function of vendor data. Instead the interval sets the date and `t` is checked against it |
| 2026-09-03 | Schema: drop `adj_close`, add `vwap` and `trade_count`, keep `volume` as float | Polygon returns no adjusted close but does return VWAP and trade count for free — the raw-zone rule says land them rather than re-pay for the history later. `volume` arrives fractional (e.g. `93171.830207`), so casting to int would silently truncate. `trade_count` types as float64 because some tickers in the 12,541-row market frame omit `n` |
| 2026-09-04 | A pool bounds concurrency, not throughput | Measured during the 90-day backfill: a 1-slot pool with a ~7-second task still issues ~8.5 requests/minute against Polygon's 5/minute free-tier limit. Related: `retry_delay=2m` is mis-scaled to a rate limit that resets every 60 seconds, so every 429 costs double the necessary penalty. Correct fix is pacing, not retry tuning \u2014 tracked as open |
| 2026-09-04 | Purge the yfinance partitions and re-seed rather than keep a mixed-vendor raw zone | Old partitions had 8 columns, `adj_close`, and int64 `volume`; new ones have 9, no `adj_close`, and float64 `volume`. Any reader scanning `dt=*` would hit a type conflict. Free to do because nothing consumes the raw zone yet; verified afterwards by a DuckDB `read_parquet('dt=*')` union across all 65 partitions succeeding with one schema |
| 2026-09-05 | Holiday detection stays vendor-driven, now with evidence | The 2026-08-30 "invariant over a market calendar" decision is no longer an assertion. Across 65 sessions the only missing weekdays are **2026-06-19** (Juneteenth) and **2026-07-03** (Independence Day observed, July 4 falling on a Saturday). Every other gap is a weekend. Asking the vendor and believing an empty response caught both holidays with no calendar dependency |

---

## Deliberately out of scope

- Cloud deployment. Local Docker is sufficient evidence and costs nothing.
- Streaming ingestion. Batch is the honest shape of this problem and of the target roles.
- Multi-agent orchestration. Complexity without a matching payoff at this scale.
- Real-money or trading-signal logic. This is a data platform, not a strategy.
