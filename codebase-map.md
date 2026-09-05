---
status: active
last_updated: 2026-09-05
next_action: Commit the re-seed work, then flip `catchup=True`, wire deadline alerts, and extract the checks into pure functions with pytest
---

# market-ops-platform — Codebase Map

Snapshot: 2026-09-05 · Polygon migration merged into `main`; re-seed work on `feature/days34-reseed`

Authoritative layout, contracts, and drift list. Read this before changing anything.

---

## What this is

A batch market-data platform built as interview evidence for DE/AI roles. Two halves:

- **Data layer** — ingest → orchestrate → transform → warehouse → quality checks. Only ingest + quality checks exist today.
- **AI layer** — retrieval over operational docs → triage agent → eval harness. Not started; `.env.example` reserves the config surface.

Everything runs locally in Docker. Cloud, streaming, and trading logic are explicitly out of scope (see ROADMAP).

---

## Topology

```text
market-ops-platform/
  docker-compose.yaml        # Airflow 3.3.1, CeleryExecutor + Postgres 16 + Redis 7.2
  Dockerfile                 # marketops/airflow:dev — deps layer before source
  requirements.txt           # extends the base image; never pins apache-airflow
  .env.example               # full config contract (tracked); .env is ignored
  .gitignore                 # ignores data/raw, logs/, scratch/, config/airflow.cfg, *.parquet
  README.md                  # 2 lines — aspirational, not current state
  ROADMAP.md                 # 4-week plan + decision log  ← the real narrative doc
  LICENSE                    # MIT
  config/
    airflow.cfg              # generated default, UNTRACKED (holds a fernet key)
    pools.json               # exported pool definitions: default_pool, yfinance, polygon
  dags/
    ingest_ohlcv_daily.py    # the only DAG
  scripts/
    audit_partitions.py      # host-side partition integrity + SHA-256 digest
  docs/
    incidents/               # two post-mortems — the strongest interview artifacts
  data/raw/ohlcv/dt=.../     # raw zone, host-mounted, gitignored (65 partitions, single-vendor)
  scratch/                   # captured vendor fixtures, gitignored
  logs/                      # Airflow task logs, gitignored
  plugins/                   # empty, mounted for completeness
```

**Tracked files: 13.** Everything else is generated, data, or secrets.

---

## Runtime architecture

`docker-compose.yaml` is the upstream Airflow 3.3.1 compose file with four local deltas:

| Delta | Why |
|---|---|
| `build: .` alongside `image:` | Builds `marketops/airflow:dev` instead of pulling stock |
| Postgres published on `127.0.0.1:5433` | Loopback-only (unqualified mapping exposes `airflow`/`airflow` to the LAN); 5433 because a native Postgres owns 5432 |
| `data/` added to the shared volume list | Raw zone is inspectable from the host |
| `AIRFLOW_CONFIG=/opt/airflow/config/airflow.cfg` | Points at the mounted config dir |

Services: `postgres`, `redis`, `airflow-apiserver` (:8080), `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`, `airflow-init`.

**Config precedence gotcha.** `config/airflow.cfg` is the *stock generated default* — it says `executor = LocalExecutor`, `sql_alchemy_conn = sqlite://...`, `load_examples = True`. None of that is live: the `AIRFLOW__*` env vars in compose take precedence. Do not read that file as the effective config. It is untracked precisely because it also carries a fernet key.

Image build order in `Dockerfile` is deliberate — `requirements.txt` is copied and installed before any source, so editing a DAG never invalidates the pip layer.

---

## The DAG — `dags/ingest_ohlcv_daily.py`

Single DAG, two tasks, TaskFlow API.

```text
fetch_ohlcv  ──(XCom: str path)──>  validate_partition
```

### Contract

For logical date `D`, write exactly one file:

```text
/opt/airflow/data/raw/ohlcv/dt=D/ohlcv.parquet
```

Grain: `(date, ticker)`. Schema pinned by `OHLCV_COLUMNS` and enforced with `reindex`, so a vendor column change cannot silently alter the parquet schema.

### Schedule

```python
CronTriggerTimetable("0 6 * * *", timezone="UTC", interval=timedelta(days=1))
```

The explicit `interval=` is load-bearing. Airflow 3 defaults `scheduler.create_cron_data_intervals=False`, so a bare cron string produces a **zero-width** data interval and `data_interval_start.date()` resolves to the current, unopened session. That defect caused the 2026-08-30 incident.

`catchup=False`, `max_active_runs=1` (scheduler-created runs only — backfills ignore it and default to 10 concurrent, which is why the pool exists).

`default_args`: `retries=3`, `retry_delay=2m`, exponential backoff, `max_retry_delay=30m`.

### `fetch_ohlcv`

Runs in the `polygon` pool (1 slot) with `execution_timeout=15m`.

1. Resolves config at task time, not parse time — `Variable.get("tickers", deserialize_json=True)` and `BaseHook.get_connection("polygon_default")`. In Airflow 3, task code has no DB access and reads these over the Task Execution API; a module-level read would cost the dag-processor a round trip per parse.
2. `session_date = ctx["data_interval_start"].date()` — never `now()`. This is what makes the task a pure function of its interval, and therefore backfillable.
3. Calls Polygon **grouped daily bars**: `GET /v2/aggs/grouped/locale/us/market/stocks/{session_date}?adjusted=false`. One request returns the entire US equity market for that session.
4. Status-code taxonomy — the retry decision is made here, not by Airflow:

   | Code | Raise | Retries? |
   |---|---|---|
   | 401 / 403 | `AirflowFailException` | No — a bad key stays bad |
   | 429 | `requests.HTTPError` | Yes — throttling clears |
   | 5xx | `requests.HTTPError` | Yes |
   | other non-200 | `AirflowFailException` | No |

5. Payload sanity: empty `results` → `AirflowSkipException` (market closed). `< 8000` rows → `AirflowFailException` (~12,500 expected; a short response is a vendor defect, not a holiday).
6. Filters the market frame down to the ticker universe, renames vendor keys (`T/v/vw/o/c/h/l/t/n`) to the pinned schema.
7. **Date invariant**: every row's `date` must equal `session_date`, else `AirflowFailException` naming the offending tickers. No retry — a vendor returning the wrong session will return it again.
8. **Completeness gate**: warn on any missing ticker; raise `ValueError` (retryable, on purpose) at ≥20% missing.
9. **Write-then-rename**: `df.to_parquet(target.tmp)` then `Path.replace(target)` — atomic within a filesystem. Readers see the old complete file or the new complete file, never a partial one. This is what makes aggressive retries safe.
10. Returns the **path string** as XCom, never the DataFrame.

### `validate_partition`

Separate task on purpose: "the vendor was down" and "the data was wrong" become distinguishable red nodes in the UI, and a re-check doesn't force a re-fetch.

Reads the parquet back off disk and asserts: non-empty · every row's `date` matches the `dt=` parsed from the path · no duplicate `(date, ticker)` · no null `close`.

Different threat model from `fetch_ohlcv` — that task validates the vendor response, this one validates the artifact on disk regardless of how it got there.

---

## Configuration surface

### Airflow objects that must exist (not in version control)

| Type | Name | Contents |
|---|---|---|
| Variable | `tickers` | JSON list, 73 symbols |
| Connection | `polygon_default` | `schema` = scheme, `host` = api host, `password` = API key |
| Pool | `polygon` | 1 slot |

The DAG will fail on parse-free/run-time lookup if any are missing. `config/pools.json` is the exported pool state and is the only one of the three that is tracked.

### `.env.example` (the real config contract)

Ingestion (`MARKET_DATA_PROVIDER`, `TICKERS`, `INGEST_START_DATE`), storage (`DUCKDB_PATH`, `RAW_ZONE_PATH` — container paths, not host), Airflow (`AIRFLOW_UID`, `AIRFLOW_IMAGE_NAME=marketops/airflow:dev`, `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, web user), dbt (`DBT_PROFILES_DIR`, `DBT_TARGET`), AI layer (`LLM_PROVIDER`, keys, `LLM_MODEL`, `EMBEDDING_MODEL`), eval (`EVAL_JUDGE_MODEL`, `EVAL_DATASET_PATH`).

`TICKERS` in `.env` and the `tickers` Airflow Variable are **two copies of the same list**. Only the Variable is read by the DAG. Divergence is a live footgun.

### Dependencies

`requirements.txt`: `requests`, `duckdb`, `pandas`, `pyarrow`. Deliberately does **not** pin `apache-airflow` — the base image owns that and re-resolving breaks the constraint tree. `yfinance` was removed during the Polygon migration.

---

## Data zone

```text
data/raw/ohlcv/dt=YYYY-MM-DD/ohlcv.parquet
```

Hive-style partitioning is functional, not cosmetic: DuckDB, Spark, Athena, and BigQuery all read `dt=` as a virtual column and prune whole directories. It also makes a re-run target a deterministic path, which is the precondition for overwrite-based idempotency.

**65 partitions on disk, 2026-06-04 → 2026-09-04.** Trading days only; weekends and holidays are absent by design (the DAG skips). Verified single-schema on 2026-09-05 by a DuckDB `read_parquet('dt=*/ohlcv.parquet', hive_partitioning=true)` union across all 65 — a surviving yfinance partition would have failed on the `volume` type conflict.

The only missing weekdays in that range are **2026-06-19** (Juneteenth) and **2026-07-03** (Independence Day observed). Both were detected by the vendor returning an empty `results`, with no market-calendar dependency. Nothing currently detects an *absent* partition — the audit script only walks directories that exist. That needs a date spine (Week 2).

Raw-zone rule: land data as close to source shape as possible. Cleaning belongs in dbt. If a transform is wrong, re-derive from raw rather than re-fetch from a vendor who may no longer serve that history.

---

## Supporting files

- **`scripts/audit_partitions.py`** — host-side, not an Airflow task. Walks `data/raw/ohlcv/dt=*`, reports missing files, date/partition-key mismatches, and a 12-char SHA-256 prefix per file. The digest is how idempotency was *proven* rather than assumed: run the same interval twice, compare hashes.
- **`scratch/`** (gitignored) — captured vendor responses used to reason about behavior without hitting the API: `polygon_trading_day.json` (12,541 rows), `polygon_trading_day_original.json`, `polygon_weekend.json` (`resultsCount: 0`, `status: OK` — the empty-but-successful case), `ohlcv_yfinance_2026-09-02.parquet`.
- **`logs/`** — includes `dag_id=aggregate_regional_sales`, a leftover from the Airflow example DAGs. Dead.
- **`plugins/`** — empty; mounted only because compose expects it.

---

## Incident docs

These are the most portfolio-valuable files in the repo. Both describe the same failure class one level apart: **internal consistency is not correctness**.

- **`2026-08-30-stale-session-partition.md`** — `dt=2026-08-29` (a Saturday) held 73 rows dated 2026-08-28. All four quality checks passed because every check compared the data to itself. Proximate cause: nothing validated the response against the request. Root cause (found 08-31): the zero-width `CronTriggerTimetable` interval meant the DAG was asking for a session that hadn't opened. Second-order lesson: a framework default changed the meaning of an unchanged line of code — anything load-bearing must be declared, not inherited.
- **`2026-09-01-partial-vendor-response-overwrote-partition.md`** — a throttled yfinance run returned 28 of 73 tickers, logged success, and overwrote a complete partition. Key insight: *"idempotent" is a property of a step against a specific source, not of a pipeline.* The write path was idempotent; the fetch wasn't, because the vendor wasn't deterministic. Overwrite-based idempotency against a flaky source is a liability, and aggressive retries make it worse.

---

## The Polygon migration (completed 2026-09-02 → 2026-09-05)

The 2026-09-01 incident established that yfinance issues one HTTP request per symbol — 73/day, 6,570 for a 90-day backfill — and fails *open* under throttling. The response was to change the shape of the request rather than pace it.

Polygon's grouped-daily endpoint returns the whole market in **one request per session**. Consequences:

- The `seed_ohlcv_history` DAG that ROADMAP listed as the next action was **struck, never built** — a 90-day backfill is 90 requests, not 6,570, and each run writes exactly one partition, which is the shape Airflow already models.
- Vendor errors are real status codes instead of a "possibly delisted" string, so the retry taxonomy is explicit rather than inferred.
- The wrong-session-date failure class is designed out: you request a date and get that date or nothing.
- `threads=False` and the yfinance pool are vestigial. `pools.json` still carries the `yfinance` entry — drop it.

### Doc/code drift — reconciled 2026-09-05

| Item | Status |
|---|---|
| Vendor | ROADMAP + decision log now describe Polygon; **both incident docs still say yfinance** and are left as-is deliberately — they are historical records of what happened, not current-state docs |
| Completeness threshold | 20%, with the reinterpretation recorded (measures list drift, not vendor reliability) |
| Completeness gate location | `fetch_ohlcv` only; removal from `validate_partition` recorded with reasoning |
| `retries` | 3, restored, recorded |
| ROADMAP decision log | 18 new entries covering the migration, plus 4 superseded and 1 **corrected** |
| README | **Still 2 lines and aspirational.** Scheduled as a Week 4 item; the only remaining known drift |

---

## Open work (ROADMAP Week 1, Days 3–4)

- Flip `catchup=True` and document what breaks — `start_date` is 2026-06-01 and the re-seed began at session 06-04, so this queues exactly **3** runs, not 90
- Deadline alerts — Airflow 3 removed `sla` / `sla_miss_callback` entirely
- Sensors, branching, dynamic task mapping — grouped-daily removed the per-ticker fan-out rationale; map over dates or build a separate exercise DAG rather than forcing it here
- Extract the date and completeness checks into pure functions with `pytest` coverage
- Record the measured Polygon-vs-yfinance reconciliation deltas on `dt=2026-09-02` (currently qualitative only)
- Pacing strategy — a pool caps concurrency, not throughput
- Drop the vestigial `yfinance` entry from `config/pools.json`
- SQL drills + a measured `EXPLAIN` improvement

Then Week 2 (dbt + DuckDB warehouse + star schema), Week 3 (hybrid retrieval + eval harness), Week 4 (triage agent + integration).

---

## Conventions

- Session date comes from `data_interval_start`, never `now()`.
- One partition = one atomic unit of work. Never append to an existing file.
- XComs carry references, never payloads.
- Heavy third-party imports go inside tasks, not at module scope — the dag-processor parses the file on a loop.
- Every design choice gets a row in the ROADMAP decision log with the reasoning, because the point is to defend it out loud.
- Retry-ability is decided by exception type at the raise site: `AirflowFailException` for deterministic wrongness, plain `ValueError`/`HTTPError` for transient conditions, `AirflowSkipException` only for genuine "nothing to do."

---

## Where to start

| Task | File |
|---|---|
| Change ingestion behavior | `dags/ingest_ohlcv_daily.py` |
| Change the ticker universe | Airflow Variable `tickers` (and mirror into `.env`) |
| Change vendor credentials | Airflow Connection `polygon_default` |
| Change concurrency | `config/pools.json` + the `pool=` arg on the task |
| Verify the raw zone | `scripts/audit_partitions.py` |
| Understand a past failure | `docs/incidents/` |
| Understand *why* something is the way it is | ROADMAP decision log |
