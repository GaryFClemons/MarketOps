---
status: active
last_updated: 2026-09-23
next_action: Work docs/build-order.md top to bottom — quality checks → alert callbacks (wire both into the DAG) → retrieval → retrieval eval numbers → triage agent → end-to-end demo. dbt and EXPLAIN wait until after.
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
| `polygon` pool (1 slot) | Active, exported to `config/pools.json`; vestigial `yfinance` entry dropped 2026-09-08 |
| Vendor pacing | In-task 429 wait loop honouring `Retry-After` (max 5 waits, 90s cap) |
| Variable `tickers` + Connection `polygon_default` | Live; connection Fernet-encrypted and masked in task logs |
| `catchup` | **True** — scheduled runs current through session 2026-09-21 |
| Raw zone | 75 single-vendor partitions, 2026-06-04 → 2026-09-21 (5,475 rows; zero grain duplicates, zero date-invariant violations — checked 2026-09-22) |
| Historical seed DAG | **Obsolete** — grouped-daily makes 90 days = 90 requests, not 6,570 |
| SQL drills (`sql/`) | 6 drill files + `00_setup.sql`, run against the raw zone via DuckDB |
| `market_ops/` package + `tests/` | **Scaffolded 2026-09-22** — contracts, docstrings and spec tests for every area; 34 core functions are `todo()` stubs. `pytest`: 40 passed, 210 TODO skips. Every spec test was shown passable by a throwaway reference implementation (not committed) |
| Quality checks as pure functions | Scaffolded — `market_ops/quality/checks.py`; DAG still runs its inline copies |
| Alert callbacks (failure + deadline) | Scaffolded — `market_ops/alerts/`; Airflow 3.3.1 deadline API verified; compose mounts the package; not wired into the DAG |
| Runbooks | **Written** — 6 in `docs/runbooks/`, every fact traced to this log or the postmortems |
| dbt project | Scaffolded — staging, `dim_date`, SCD2 ticker snapshot, incremental fact skeleton, 2 singular tests; SQL checked in DuckDB against the raw zone, `dbt build` not yet run |
| DuckDB warehouse | Not built — `marketops.duckdb` is still only the drills' ad-hoc engine |
| Retrieval layer | Scaffolded — corpus loader, offline embedder, wiring written; tokenizer, BM25, chunking, RRF, dense search pending |
| Triage agent | Scaffolded — schemas, tool specs, fallback brief, CLI, scripted demo transcript written; tools, guardrails, audit append, loop pending |
| Eval harness | Scaffolded — golden sets written (30 retrieval, 6 brief cases); metrics and harness loop pending; no numbers yet |

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
- [x] Flip `catchup=True` *(2026-09-08)* — the raw zone has run unbroken to session 2026-09-15 since
- [x] Write up what `catchup=True` actually changes *(2026-09-16)* — see the two corrected decision-log entries. **The premise of this item was wrong:** the run count is a function of the latest existing DagRun, not of `start_date`
- [~] Deadline alerts — SLAs were **removed** in Airflow 3; `sla` and `sla_miss_callback` no longer exist. **Scaffolded 2026-09-22** in `market_ops/alerts/deadlines.py`: API verified against the installed 3.3.1 (`DeadlineAlert` + `DeadlineReference.DAGRUN_QUEUED_AT` + `AsyncCallback`, run by the triggerer), anchoring decision recorded below. Implementation and DAG wiring still owner work
- [ ] Sensors, branching, dynamic task mapping — **note:** grouped-daily removed the per-ticker fan-out rationale; map over dates or build it as a separate exercise DAG rather than forcing it into the ingestion path. Design sketched in docs/build-order.md (a quality-sweep DAG mapping over the date spine); deferred behind the AI layer

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
- [x] Daily-DAG / backfill pacing strategy *(2026-09-08)* — in-task 429 wait loop, since a pool caps concurrency, not throughput
- [~] Extract the date and completeness checks into pure functions with `pytest` coverage. **Scaffolded 2026-09-22**: `market_ops/quality/checks.py` (7 functions, verdicts not exceptions) with 59 spec tests, plus a DAG-parity test, including one regression test per incident; `pytest` is in `requirements-dev.txt`. Bodies and the DAG refactor are owner work
- [x] `ANSS` removed from the universe *(2026-09-16)* — acquired by Synopsys, already held as `SNPS`. The `tickers` Variable was already at 73; `.env.example` still carried the 74th copy and has been trimmed to match. Recurring missing tickers are a list-maintenance bug, not a data condition

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

### Days 5–6 — SQL drills + partitioning `[~]` *(2026-09-08)*

- [x] 1 hour daily, timed, no autocomplete
- [x] Window functions, multi-CTE decomposition, dedupe, gaps-and-islands, anti-joins, date spines — all seven files in `sql/`, written against the real raw zone rather than toy tables
- [ ] `EXPLAIN` a slow query; make one measurably faster. **This is the whole point of the block and it is the part not done** — six correct queries prove syntax, one measured improvement proves you can reason about cost. Protocol and empty results table in docs/query-cost.md (`dt` vs `date` filter); deferred behind the AI layer
- [ ] Document why partition layout dominates query cost

> `sql/date-spine-anti-join.sql` answers the absent-partition question the 2026-08-30
> incident left open — generate weekdays, anti-join the raw zone, return the holes.
> Note the limit honestly: the query exists, **nothing runs it**. Detection is still
> manual until it becomes a scheduled check or a dbt test.

**Deliverable:** Working Dockerized Airflow + idempotent, backfillable ingestion DAG. README explains design decisions, not setup steps.

---

## Reprioritized 2026-09-22 — AI layer ahead of dbt

The AI layer (Weeks 3–4) was pulled ahead of the Week 2 warehouse work. The whole remaining project was scaffolded in one pass: every module's contracts, docstrings and spec tests, with the core logic left as `todo()` stubs to be written by hand. Build order, with estimates and the question each piece answers: [docs/build-order.md](docs/build-order.md).

How the scaffold works:
- `market_ops._scaffold.todo()` raises `NotBuiltYet`; `tests/conftest.py` reports it as a skip whose reason is the TODO text. `pytest -rs` is the backlog; `pytest --todo-fail` turns it red.
- Every spec test was checked by writing a throwaway reference implementation from the docstrings alone (kept out of the repo) and running the suite with `--todo-fail`: 250 of 250 passed. A test that no correct implementation could pass would have shown up there.
- dbt SQL was checked by rendering the Jinja by hand and running it in DuckDB against the real raw zone.

## Week 2 — Transformation, modeling, Python screen

- [~] dbt Core: sources, models, refs, materializations — `dbt/` scaffolded 2026-09-22; `dbt build` not yet run
- [~] Staging → intermediate → marts over raw market data — `stg_ohlcv`, `dim_date`, `dim_ticker`, `fct_daily_bars` (no intermediate layer yet: nothing needs one)
- [~] At least one incremental model with late-arriving-data handling — `fct_daily_bars` skeleton with a lookback window; sizing and the LAG boundary fix are TODO
- [~] Tests: `unique`, `not_null`, `relationships`, `accepted_values`, plus one custom — all declared; custom generic `unique_combination` and two singular tests (the partition invariant; the date-spine anti-join as a warn-level test)
- [~] Star schema design; **state the grain of every fact table** — docs/data_model.md
- [~] SCD Type 2 implementation in dbt — `ticker_universe_snapshot` (check strategy), with ANSS as the real membership change
- [~] `docs/data_model.md` — schema and grain rationale; owner TODOs marked
- [ ] Python drills, 1 hour daily, timed and spoken aloud
- [~] `pytest` coverage on transformation helpers — suite exists (`tests/`); coverage becomes real as the stubs are implemented

**Deliverable:** Layered dbt project, tests passing, documented lineage, one incremental model.

---

## Week 3 — Retrieval

- [~] Embeddings, cosine similarity, dimensionality — `Embedder` protocol; offline `HashingEmbedder` (explicitly non-semantic); `ProviderEmbedder` is next tier
- [~] Vector store: **flat exact numpy index**, not pgvector/Chroma — decision below; `DenseIndex.search` is a stub
- [~] Chunking strategy — defensible size and overlap, not defaults — `chunk_document` fully specified (section-aligned, 1,200 chars, overlap only on oversize splits); stub
- [~] Index runbooks, schema docs, pipeline logs — corpus loader written; 6 runbooks written; task logs are read live by an agent tool rather than indexed
- [~] Hybrid search (BM25 + dense) — required for tickers and error codes — identifier-preserving tokenizer, BM25 and RRF specified; stubs
- [~] Reranking over a wider candidate set — `Reranker` protocol; next tier, and only if the eval shows fusion leaves ranking errors
- [~] Golden dataset — 30 retrieval cases, section-level labels, tagged identifier/paraphrase/why/procedure/multi_hop; every label verified against a real heading
- [~] Retrieval metrics: precision@k, recall@k, MRR — specified with edge cases; stubs
- [~] Generation metrics — deterministic brief scoring specified (`score_brief`); 6 brief golden cases
- [~] LLM-as-judge harness with a stronger judge model than the generator — rubric drafted; next tier
- [~] Cost and latency tracked per query — latency in the harness; `cost_usd` next tier (no prices invented)
- [ ] `docs/eval_results.md` — measured before/after for one improvement — template only, no numbers

**Deliverable:** Hybrid retrieval + reranking over project docs, with a reporting eval harness.

---

## Week 4 — Agents, integration, readiness

- [~] Tool use / function calling; schema design and error recovery — 4 read-only tools + terminal `submit_brief`; schemas generated from the validating models; `dispatch` returns errors to the model as text
- [~] Agent loop with guardrails and blast-radius limits — `run_triage` fully specified (budget before each call, fenced untrusted results, one repair, deterministic fallback); stub
- [~] Triage agent: detect failed or deadline-missed run → retrieve context → emit incident brief — signal handoff, contracts, CLI and an offline scripted demo written; logic stubs
- [~] Wire AI layer to data layer — compose mounts `market_ops` into every Airflow container; callbacks not yet wired into the DAG
- [x] `docs/architecture.md` with a real diagram *(2026-09-22)* — status, trust boundaries, failure modes
- [~] README as a landing page *(2026-09-22)* — rewritten with an honest status table; repo not yet pinned
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
| 2026-09-04 | A pool bounds concurrency, not throughput | Measured during the 90-day backfill: a 1-slot pool with a ~7-second task still issues ~8.5 requests/minute against Polygon's 5/minute free-tier limit. Related: `retry_delay=2m` is mis-scaled to a rate limit that resets every 60 seconds, so every 429 costs double the necessary penalty. Correct fix is pacing, not retry tuning. **Closed 2026-09-08** by the in-task wait loop \u2014 see below |
| 2026-09-04 | Purge the yfinance partitions and re-seed rather than keep a mixed-vendor raw zone | Old partitions had 8 columns, `adj_close`, and int64 `volume`; new ones have 9, no `adj_close`, and float64 `volume`. Any reader scanning `dt=*` would hit a type conflict. Free to do because nothing consumes the raw zone yet; verified afterwards by a DuckDB `read_parquet('dt=*')` union across all 65 partitions succeeding with one schema |
| 2026-09-05 | Holiday detection stays vendor-driven, now with evidence | The 2026-08-30 "invariant over a market calendar" decision is no longer an assertion. Across 65 sessions the only missing weekdays are **2026-06-19** (Juneteenth) and **2026-07-03** (Independence Day observed, July 4 falling on a Saturday). Every other gap is a weekend. Asking the vendor and believing an empty response caught both holidays with no calendar dependency. **Extended 2026-09-16:** 71 sessions now, and the only new missing weekday is **2026-09-07** (Labor Day) — caught the same way, still no calendar dependency |
| 2026-09-08 | Pace inside the task with a 429 wait loop rather than tuning `retry_delay` | The 2026-09-04 measurement showed the real problem: a pool bounds *concurrency*, and a 1-slot pool with a ~7-second task still issues ~8.5 req/min against a 5/min limit. Airflow retries are the wrong instrument — `retry_delay=2m` is mis-scaled to a window that resets every 60s, and a retry tears down and rebuilds the whole task to solve a problem that lasts seconds. The loop honours `Retry-After` when the vendor sends it, falls back to 60s, caps a single wait at 90s, and after 5 waits gives up to Airflow's retry as the outer backstop. Two nested timescales, each matched to its cause |
| 2026-09-08 | `catchup=True` | **Reason corrected 2026-09-16 — the count was right, the explanation was wrong.** Catchup fills forward from the **latest existing DagRun** and never backwards into holes behind it; `start_date` anchors the schedule only when no DagRun exists at all. Measured in `dag_run`: at the flip the latest scheduler-created run was `logical_date` 09-04, so exactly three fire times had elapsed — logical 09-05, 09-06, 09-07 (Saturday, Sunday, Labor Day), all three skipped. What suppressed the other 90 was the existence of the **backfill DagRuns** covering 06-04 → 09-07, not the parquet on disk and not `start_date`. Proof that `start_date` is not the anchor: 06-01 → 06-03 still have no DagRun and no partition, and catchup has never created one in eight days of running. Safe either way, because the task is a pure function of its interval and the write is an overwrite — the worst case of a catchup storm is re-deriving partitions that already exist |
| 2026-09-16 | Catchup is the recovery mechanism for scheduler downtime, and it is not gentle | Measured, and the better answer to "what does `catchup=True` change": the stack was down from 09-09 to 09-16. On restart the scheduler queued **8** runs in 35 seconds — `queued_at` 23:43:46 → 23:44:21, logical 09-08 → 09-15 — and executed them back to back. So the run count is *fire times between the latest DagRun and now*, which after an outage is simply the length of the outage. `max_active_runs=1` is what kept the burst serial; without it, eight concurrent runs would have hit Polygon's 5/min limit at once and put the entire load on the in-task 429 loop |
| 2026-09-08 | SQL drills query the raw parquet through a DuckDB view, not a loaded table | `sql/00_setup.sql` is one `read_parquet('dt=*/ohlcv.parquet', hive_partitioning=1)` view. Keeps the drills honest — they run against the real 71-partition zone with its real nulls and schema, and `dt` is exercised as a genuine partition-pruned predicate rather than an ordinary column. Also means no load step to keep in sync. **Not** a warehouse: `marketops.duckdb` holds this view and nothing else, and is gitignored |
| 2026-09-22 | Shared code lives in a `market_ops/` package, mounted into every Airflow container at `/opt/airflow/src` with `PYTHONPATH=/opt/airflow/src` | Callbacks are stored by import path and resolved inside the worker and triggerer, so the package must be importable there. Mounted like `dags/` so an edit never needs an image rebuild. A dedicated `src/` keeps `/opt/airflow` itself off `sys.path`, where `config/` or `logs/` could shadow real modules. Host-side tests import the same package from the repo root |
| 2026-09-22 | Data-quality checks become pure functions that return verdicts; the DAG maps verdicts to Airflow exceptions | The retry taxonomy becomes testable in milliseconds without Airflow. One implementation serves `validate_partition`, `scripts/audit_partitions.py` and the agent's `check_partition` tool, so they can't drift. A pure check cannot read `now()` or the metadata DB, which is the property the 2026-08-30 incident lacked |
| 2026-09-22 | Unwritten logic raises `NotBuiltYet`; the test hook reports it as a TODO skip | Tests were written against docstrings before the implementations exist. A wall of red would hide real regressions; skips with the TODO text as the reason make `pytest -rs` the backlog. Only this subclass is intercepted, so a genuine `NotImplementedError` still fails, and `--todo-fail` turns the backlog red on demand. Each spec test was proven passable by a throwaway reference implementation written from the docstrings alone: 250/250 |
| 2026-09-22 | Deadline alert anchored on `DAGRUN_QUEUED_AT`, delivered by an `AsyncCallback` *(scaffold — confirm when implementing)* | A logical-date deadline is already blown for every catchup or backfill run, whose logical dates are in the past: an alert storm, and the 2026-09-16 restart queued 8 runs in 35 seconds. Queued-at measures "slow or stuck", which on-call can act on. The trade-off: it is not the business SLA ("yesterday's bars by a fixed time"), which would need a logical-date deadline with backfill suppression. `AsyncCallback` because, in the installed 3.3.1, a `SyncCallback` needs an executor with `supports_callbacks = True` (only `LocalExecutor` in core; the Celery provider isn't on the host to check), while the triggerer runs async callbacks and compose already runs one. The callback's `context` is the DagRun's REST representation (`models/deadline.py::handle_miss`), not a task context |
| 2026-09-22 | Airflow hands off to the AI layer through `RunSignal` JSON files, one per event, named by a stable hash of the event | Decoupled: Airflow never needs an LLM key and never waits on a model, so an LLM outage can't slow ingestion. Durable and replayable: the same signal can be re-triaged after a prompt change, which is what an eval needs. A stable id makes a double-fired callback an overwrite, not a second page. At scale this becomes a queue or the REST API |
| 2026-09-22 | The triage agent is read-only; state-changing recommendations require `requires_human`; every failure path returns a deterministic fallback brief | Autonomy matches reversibility: reading logs can run unattended, while reruns, backfills and config changes go to a person. The AI layer may degrade, but it never drops a signal: when the model refuses, stalls, errors, blows its budget or fails validation twice, a human still gets a brief carrying the raw signal |
| 2026-09-22 | The brief is a strict schema submitted through a terminal `submit_brief` tool, and every citation is checked against what the agent was shown | Free text can't be validated, diffed or scored. `extra="forbid"` stops invented fields. Groundedness is checked mechanically on every brief: each evidence ref must be something a tool returned, and each quote must appear in that text. A real ref with an invented quote is the subtlest hallucination, since it looks cited. "confirmed" requires first-hand evidence; a runbook alone only makes a cause "likely" |
| 2026-09-22 | Text from logs, vendors and signals is fenced as data, not filtered for injections | Pattern-matching attacks is a treadmill, and false positives delete real evidence from an incident. Normalizing, stripping control and bidi characters, neutralizing the delimiters, then wrapping in a labelled fence keeps the evidence and makes the boundary explicit; the audit trail flags every call that carried untrusted text |
| 2026-09-22 | A provider-neutral `LLMClient` with a `ScriptedLLM` test double; no model id in code | The loop, guardrails and audit trail are tested offline, deterministically and for free, by scripting whole conversations, including refusals, budget exhaustion and injection attempts. The provider is a swappable dependency, like Polygon was; the model is configuration (`LLM_MODEL`), and the eval harness says whether changing it was safe |
| 2026-09-22 | BM25 is a first-class retriever with an identifier-preserving tokenizer; hybrid ranking uses Reciprocal Rank Fusion (k = 60) | On-call queries are full of exact identifiers (`AirflowFailException`, `429`, `dt=2026-08-29`, `polygon_default`) that embeddings blur. The tokenizer keeps compounds whole and also emits their parts, never splitting ISO dates. BM25 scores and cosine similarities are on incomparable scales; RRF fuses ranks only, so there is nothing to normalize or tune |
| 2026-09-22 | Flat exact numpy index instead of pgvector or Chroma | A few hundred chunks: brute-force cosine is exact and sub-millisecond. ANN indexes trade recall for latency that only matters around 10^5–10^6 vectors. A vector database here would add a service, a failure mode and a recall loss to solve a problem that doesn't exist yet |
| 2026-09-22 | Golden labels are section-level (doc + exact heading), not chunk ids | Chunk ids change with chunk size and overlap; headings don't, so the golden set survives the experiments it exists to judge. Known coarseness: the decision log is one section, so a "why" label on it accepts any row |
| 2026-09-22 | Airflow 3 task logs are JSON lines, and on the Windows host `:` in run-id directory names is stored as U+F03A | Observed on disk 2026-09-22: `run_id=scheduled__2026-09-21T060000+0000`. A host-side log reader that builds the path from the run id finds nothing unless it maps `:`. The records carry `error_detail` with `exc_type`/`exc_value` plus stack frames; the agent's log tool keeps the first two and drops frames, which are most of the bytes and little of the signal |
| 2026-09-22 | `airflow backfill create` defaults to `--reprocess-behavior none` | Read from the 3.3.1 CLI definition, not yet exercised: when a run already exists for a logical date, the default creates no new run. Re-deriving an existing partition by backfill therefore needs `--reprocess-behavior completed` (or `failed`), previewed with `--dry-run`. Recorded in docs/runbooks/partition-integrity.md |

---

## Deliberately out of scope

- Cloud deployment. Local Docker is sufficient evidence and costs nothing.
- Streaming ingestion. Batch is the honest shape of this problem and of the target roles.
- Multi-agent orchestration. Complexity without a matching payoff at this scale.
- Real-money or trading-signal logic. This is a data platform, not a strategy.
