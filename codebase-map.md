---
status: active
last_updated: 2026-09-23
next_action: Implement the core todo() stubs in docs/build-order.md order, wiring quality checks and alert callbacks into the DAG first
---

# market-ops-platform — Codebase Map

Snapshot: 2026-09-23 · PR #8 is the last merge to `main`; branch `feature/deadline-alerts` carries the 2026-09-16 doc edits plus the 2026-09-22 scaffold, **uncommitted**

Authoritative layout, contracts, and drift list. Read this before changing anything.

---

## What this is

A batch market-data platform built as interview evidence for DE/AI roles. Two halves:

- **Data layer** — ingest → orchestrate → transform → warehouse → quality checks. Ingest and inline quality checks are built; the checks' extraction into `market_ops/quality` and the dbt warehouse are scaffolded.
- **AI layer** — retrieval over operational docs → triage agent → eval harness. **Scaffolded 2026-09-22**: contracts, docstrings, spec tests, runbook corpus and golden sets exist; the core logic is `todo()` stubs (see "The market_ops package" below).

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
  README.md                  # landing page with an honest built/scaffolded status table
  ROADMAP.md                 # 4-week plan + decision log  ← the real narrative doc
  pyproject.toml             # tool config only: pytest (importlib mode, pythonpath=.)
  requirements-dev.txt       # host dev deps: requirements.txt + pytest, pydantic, numpy
  LICENSE                    # MIT
  config/
    airflow.cfg              # generated default, UNTRACKED (holds a fernet key)
    pools.json               # exported pool definitions: default_pool, polygon
  dags/
    ingest_ohlcv_daily.py    # the only DAG
  sql/
    00_setup.sql             # DuckDB view over the raw parquet — run this first
    window-functions.sql     # interview drills, written against the real raw zone
    ranking.sql
    multi-cte-decomp.sql
    dedupe.sql
    gaps-islands.sql
    date-spine-anti-join.sql # finds absent partitions — nothing runs it yet
  scripts/
    audit_partitions.py      # host-side partition integrity + SHA-256 digest
  market_ops/                # the Python half — see "The market_ops package" below
    _scaffold.py             # todo() / NotBuiltYet: how unfinished logic is marked
    config.py                # Settings.from_env(): paths + LLM settings, host vs container
    quality/checks.py        # pure DQ checks extracted from the DAG
    alerts/                  # failure + deadline callbacks -> RunSignal files
    retrieval/               # corpus, chunking, BM25, dense, RRF, service
    agent/                   # schemas, llm protocol, tools, guardrails, audit, triage loop, CLI
    evals/                   # golden-set schemas, metrics, harness, judge, cost, CLI
  tests/                     # spec tests mirroring market_ops/; conftest.py holds the TODO hook
  evals/
    golden/                  # retrieval.jsonl (30), incident_briefs.jsonl (6)
    README.md                # labeling rules, tags, before/after protocol
  dbt/                       # staging + marts + SCD2 snapshot over the raw zone, DuckDB target
  docs/
    incidents/               # two post-mortems — the strongest interview artifacts
    runbooks/                # 6 on-call runbooks — also the retrieval corpus
    architecture.md          # diagram, status, trust boundaries, failure modes
    build-order.md           # the core backlog in order, with DAG-wiring instructions
    data_model.md            # star schema and grain decisions
    eval_results.md          # measured results only — currently a template
    query-cost.md            # EXPLAIN experiment protocol — template, no numbers
  marketops.duckdb           # ad-hoc drill database, gitignored — not a warehouse
  data/raw/ohlcv/dt=.../     # raw zone, host-mounted, gitignored (75 partitions, single-vendor)
  scratch/                   # captured vendor fixtures, gitignored
  logs/                      # Airflow task logs, gitignored
  plugins/                   # empty, mounted for completeness
```

**Tracked files: 21 on `main`.** The 2026-09-22 scaffold adds 94 untracked files (package, tests, docs, dbt, golden sets), none of them generated. Everything else is generated, data, or secrets.

---

## Runtime architecture

`docker-compose.yaml` is the upstream Airflow 3.3.1 compose file with six local deltas:

| Delta | Why |
|---|---|
| `build: .` alongside `image:` | Builds `marketops/airflow:dev` instead of pulling stock |
| Postgres published on `127.0.0.1:5433` | Loopback-only (unqualified mapping exposes `airflow`/`airflow` to the LAN); 5433 because a native Postgres owns 5432 |
| `data/` added to the shared volume list | Raw zone is inspectable from the host |
| `AIRFLOW_CONFIG=/opt/airflow/config/airflow.cfg` | Points at the mounted config dir |
| `./market_ops` mounted at `/opt/airflow/src/market_ops` *(2026-09-22)* | Callbacks are resolved by import path in the worker and triggerer; mounted like `dags/` so edits need no rebuild |
| `PYTHONPATH=/opt/airflow/src` *(2026-09-22)* | Makes the package importable without putting `/opt/airflow` (with `config/`, `logs/`) on `sys.path`. Takes effect on the next `docker compose up -d` |

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

`catchup=True` (set 2026-09-08), `max_active_runs=1` (scheduler-created runs only — backfills ignore it and default to 10 concurrent, which is why the pool exists).

Catchup fills forward from the **latest existing DagRun**, never backwards into holes behind it — `start_date` anchors the schedule only when no DagRun exists at all. That is why the 06-01 → 06-03 intervals have never been created despite `start_date=2026-06-01`, and why a week of scheduler downtime queued 8 runs in 35 seconds on 2026-09-16. `max_active_runs=1` is what keeps such a burst serial against a 5/min vendor limit. See the 09-08 and 09-16 decision-log entries.

`default_args`: `retries=3`, `retry_delay=2m`, exponential backoff, `max_retry_delay=30m`.

### `fetch_ohlcv`

Runs in the `polygon` pool (1 slot) with `execution_timeout=15m`.

1. Resolves config at task time, not parse time — `Variable.get("tickers", deserialize_json=True)` and `BaseHook.get_connection("polygon_default")`. In Airflow 3, task code has no DB access and reads these over the Task Execution API; a module-level read would cost the dag-processor a round trip per parse.
2. `session_date = ctx["data_interval_start"].date()` — never `now()`. This is what makes the task a pure function of its interval, and therefore backfillable.
3. Calls Polygon **grouped daily bars**: `GET /v2/aggs/grouped/locale/us/market/stocks/{session_date}?adjusted=false`. One request returns the entire US equity market for that session.
4. **In-task throttle loop** wraps the request: on a 429 it sleeps `Retry-After` (falling back to 60s, capped at `MAX_WAIT_SECONDS=90`) and retries in place, up to `MAX_THROTTLE_WAITS=5`. Only after the fifth wait does it raise to Airflow. Two nested timescales — seconds inside the task for rate limits, minutes outside it for genuine outages.
5. Status-code taxonomy — the retry decision is made here, not by Airflow:

   | Code | Raise | Retries? |
   |---|---|---|
   | 401 / 403 | `AirflowFailException` | No — a bad key stays bad |
   | 429 | handled in-task; `requests.HTTPError` only after 5 waits | Yes — throttling clears |
   | 5xx | `requests.HTTPError` | Yes |
   | other non-200 | `AirflowFailException` | No |

6. Payload sanity: empty `results` → `AirflowSkipException` (market closed). `< 8000` rows → `AirflowFailException` (~12,500 expected; a short response is a vendor defect, not a holiday).
7. Filters the market frame down to the ticker universe, renames vendor keys (`T/v/vw/o/c/h/l/t/n`) to the pinned schema.
8. **Date invariant**: every row's `date` must equal `session_date`, else `AirflowFailException` naming the offending tickers. No retry — a vendor returning the wrong session will return it again.
9. **Completeness gate**: warn on any missing ticker; raise `ValueError` (retryable, on purpose) at ≥20% missing.
10. **Write-then-rename**: `df.to_parquet(target.tmp)` then `Path.replace(target)` — atomic within a filesystem. Readers see the old complete file or the new complete file, never a partial one. This is what makes aggressive retries safe.
11. Returns the **path string** as XCom, never the DataFrame.

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

Ingestion (`MARKET_DATA_PROVIDER`, `TICKERS`, `INGEST_START_DATE`), storage (`DUCKDB_PATH`, `RAW_ZONE_PATH`, `OPS_DIR` — container paths, not host; `OPS_DIR` added 2026-09-22 for signals and the audit trail), Airflow (`AIRFLOW_UID`, `AIRFLOW_IMAGE_NAME=marketops/airflow:dev`, `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, web user), dbt (`DBT_PROFILES_DIR`, `DBT_TARGET`), AI layer (`LLM_PROVIDER`, keys, `LLM_MODEL`, `EMBEDDING_MODEL`), eval (`EVAL_JUDGE_MODEL`, `EVAL_DATASET_PATH`).

`TICKERS` in `.env` and the `tickers` Airflow Variable are **two copies of the same list**. Only the Variable is read by the DAG. Divergence is a live footgun — and it had already happened: `.env.example` still carried `ANSS` at 74 symbols after the Variable was trimmed to 73. Corrected 2026-09-16.

### Dependencies

`requirements.txt`: `requests`, `duckdb`, `pandas`, `pyarrow`. Deliberately does **not** pin `apache-airflow` — the base image owns that and re-resolving breaks the constraint tree. `yfinance` was removed during the Polygon migration.

`requirements-dev.txt` (host only, 2026-09-22): the above plus `pytest`, `pydantic`, `numpy`; provider SDKs and `dbt-duckdb` are listed commented-out until their TODOs are implemented. The Airflow image never installs it; `market_ops` in-container code (quality, alerts) needs only pandas and pydantic, both already in the image.

---

## Data zone

```text
data/raw/ohlcv/dt=YYYY-MM-DD/ohlcv.parquet
```

Hive-style partitioning is functional, not cosmetic: DuckDB, Spark, Athena, and BigQuery all read `dt=` as a virtual column and prune whole directories. It also makes a re-run target a deterministic path, which is the precondition for overwrite-based idempotency.

**75 partitions on disk, 2026-06-04 → 2026-09-21** (5,475 rows = 75 × 73; zero duplicate `(date, ticker)` rows and zero date-invariant violations, checked 2026-09-22). Trading days only; weekends and holidays are absent by design (the DAG skips). Verified single-schema on 2026-09-05 by a DuckDB `read_parquet('dt=*/ohlcv.parquet', hive_partitioning=true)` union across all partitions — a surviving yfinance partition would have failed on the `volume` type conflict.

The only missing weekdays in that range are **2026-06-19** (Juneteenth), **2026-07-03** (Independence Day observed), and **2026-09-07** (Labor Day). All three were detected by the vendor returning an empty `results`, with no market-calendar dependency. `sql/date-spine-anti-join.sql` now generates the weekday spine and anti-joins the raw zone to surface holes — but it is a drill, not a control: nothing schedules it, so an *absent* partition is still only found by looking.

Raw-zone rule: land data as close to source shape as possible. Cleaning belongs in dbt. If a transform is wrong, re-derive from raw rather than re-fetch from a vendor who may no longer serve that history.

---

## Supporting files

- **`scripts/audit_partitions.py`** — host-side, not an Airflow task. Walks `data/raw/ohlcv/dt=*`, reports missing files, date/partition-key mismatches, and a 12-char SHA-256 prefix per file. The digest is how idempotency was *proven* rather than assumed: run the same interval twice, compare hashes.
- **`sql/`** — Week 1 Days 5–6 interview drills, run through DuckDB against the raw zone. `00_setup.sql` creates the single `ohlcv` view over `read_parquet('dt=*')`; the rest are the standard set (window functions, ranking, multi-CTE decomposition, dedupe, gaps-and-islands, date-spine anti-join). Written against real data on purpose — real nulls, real schema, and `dt` exercised as a genuine pruned predicate. Nothing here is wired into the pipeline.
- **`marketops.duckdb`** (gitignored) — the drill database. Holds the view above and nothing else. Its existence does **not** mean the Week 2 warehouse is started.
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
- `threads=False` and the yfinance pool are gone. The vestigial `yfinance` entry was dropped from `pools.json` on 2026-09-08.

### Doc/code drift — reconciled 2026-09-16

| Item | Status |
|---|---|
| Vendor | ROADMAP + decision log describe Polygon; **both incident docs still say yfinance** and are left as-is deliberately — they are historical records of what happened, not current-state docs |
| Completeness threshold | 20%, with the reinterpretation recorded (measures list drift, not vendor reliability) |
| Completeness gate location | `fetch_ohlcv` only; removal from `validate_partition` recorded with reasoning |
| `catchup` | Now `True` in the DAG; docs and the inline comment all corrected |
| Pacing | In-task 429 wait loop landed 2026-09-08; the status-code table and decision log now reflect it |
| Raw zone size | 75 partitions through 2026-09-21 (was 71 / 09-15) |
| `sql/` + `marketops.duckdb` | Added to the topology; the DuckDB file is explicitly *not* the Week 2 warehouse |
| README | Rewritten 2026-09-22 as a landing page; status table distinguishes built from scaffolded |
| DAG vs `market_ops/quality` | **Two copies of the checks** until the DAG is refactored to call them; `tests/quality/test_schema_parity.py` guards the `OHLCV_COLUMNS` copy only |

---

## The market_ops package (scaffolded 2026-09-22)

The DAG owns scheduling and vendor I/O. Everything that should be a plain, testable function lives in `market_ops/`. It was scaffolded in one pass: contracts, docstrings and spec tests are written; **34 core functions are `todo()` stubs** for the owner to implement, in the order in `docs/build-order.md`.

### How unfinished work is marked

- `market_ops._scaffold.todo(what, tier=...)` raises `NotBuiltYet`, tiered `core` / `next` / `later`.
- `tests/conftest.py` turns `NotBuiltYet` into a skip whose reason is the TODO text. Current state: **40 passed, 210 skipped, 0 failed**. `pytest -rs` lists the backlog; `pytest --todo-fail` shows it red. Only `NotBuiltYet` is intercepted.
- Every spec test was proven passable: a throwaway reference implementation written from the docstrings alone (never committed) passed 250/250 with `--todo-fail`.
- `@pytest.mark.live` tests (real LLM calls) skip unless `MARKETOPS_LIVE=1`.

### Data flow across the two layers

```text
DAG task fails / run misses its deadline
  -> alerts/ callback writes a RunSignal   data/ops/signals/<signal_id>.json
  -> agent/ triage loop reads it, calls read-only tools (logs, partition, docs, run state)
  -> IncidentBrief (schema-validated, citations checked)  data/ops/briefs/
  -> one audit line per step                               data/ops/audit/triage.jsonl
  -> evals/ scores retrieval and briefs against evals/golden/
```

`data/ops/` is gitignored and host-mounted, so the containers write signals and the host CLI reads them.

### Contracts (written first; other code depends on their exact shape)

| Contract | File | Key rule |
|---|---|---|
| `RunSignal` | `agent/schemas.py` | `signal_id` = stable hash of the event, so a double-fired callback overwrites; `message` is untrusted |
| `IncidentBrief` | `agent/schemas.py` | `extra="forbid"`; ≥1 evidence; `requires_human` must be true for mutating actions or high/critical severity |
| `Chunk.section` | `retrieval/types.py` | Heading path joined with `" > "`; golden labels match on it, never on chunk ids |
| `LLMClient` / `ScriptedLLM` | `agent/llm.py` | Provider-neutral; the scripted double replays whole conversations in tests |
| `ToolResult.refs` | `agent/tools.py` | Every citable ref → the exact text shown; `validate_brief` checks citations against it |

### Verified facts the scaffold depends on

- **Airflow 3.3.1 deadlines.** `DeadlineAlert(reference, interval, callback)`. `AsyncCallback` runs in the triggerer as `await callback(**kwargs, context=context)`, where `context` is `{"dag_run": <REST DagRun JSON>, "deadline": {...}}`. `SyncCallback` needs an executor with `supports_callbacks`. Source: `sdk/definitions/deadline.py`, `sdk/definitions/callback.py`, `triggers/callback.py`, `models/deadline.py`.
- **Task logs.** `dag_id=/run_id=/task_id=/attempt=N.log`, written as JSON lines with an `error_detail` list on failures. On this Windows host, `:` in run-id directory names is stored as U+F03A.
- **`airflow backfill create`.** It defaults `--reprocess-behavior none`, so re-deriving an existing session needs `completed`.

---

## Open work

The core backlog, in order, is `docs/build-order.md`. In short:

- Implement the 34 core `todo()` stubs: quality → alerts → retrieval → retrieval metrics and harness → agent → brief scoring.
- Wire the checks and both callbacks into the DAG (owner writes DAG code), then `docker compose up -d` to pick up the mount.
- Record the first measured retrieval numbers in `docs/eval_results.md`. `HashingEmbedder` isn't semantic, so a real BM25-vs-hybrid comparison needs `ProviderEmbedder` (next tier).

Carried from Week 1, still open:

- Sensors, branching, dynamic task mapping. The design sketch (a quality-sweep DAG) is in `docs/build-order.md`.
- Record the measured Polygon-vs-yfinance reconciliation deltas on `dt=2026-09-02` (currently qualitative only).
- One measured `EXPLAIN` improvement: protocol in `docs/query-cost.md`.
- Absent-partition detection. `dbt/tests/assert_no_missing_sessions.sql` now ports the date-spine anti-join as a warn-level test, but dbt isn't run on a schedule, so absent partitions are still detectable, not detected.

---

## Conventions

- Session date comes from `data_interval_start`, never `now()`.
- One partition = one atomic unit of work. Never append to an existing file.
- XComs carry references, never payloads.
- Heavy third-party imports go inside tasks, not at module scope — the dag-processor parses the file on a loop.
- Every design choice gets a row in the ROADMAP decision log with the reasoning, because the point is to defend it out loud.
- Retry-ability is decided by exception type at the raise site: `AirflowFailException` for deterministic wrongness, plain `ValueError`/`HTTPError` for transient conditions, `AirflowSkipException` only for genuine "nothing to do."
- Checks in `market_ops/quality` return verdicts and never import Airflow; the DAG maps verdicts to exceptions.
- Unfinished logic calls `todo()`, never `pass` or a plausible placeholder. A stub must fail loudly (as a TODO skip), not return something that looks right.
- Owner writes DAG code; scaffolding goes in `market_ops/` and `docs/build-order.md` explains the wiring.
- The agent is read-only. No tool writes; state-changing recommendations require `requires_human`.
- No invented numbers. Eval results, prices, and model ids come from a measurement or config, never from a default in code.

---

## Where to start

| Task | File |
|---|---|
| Change ingestion behavior | `dags/ingest_ohlcv_daily.py` |
| Change the ticker universe | Airflow Variable `tickers` (and mirror into `.env`) |
| Change vendor credentials | Airflow Connection `polygon_default` |
| Change concurrency | `config/pools.json` + the `pool=` arg on the task |
| Change vendor pacing | `MAX_THROTTLE_WAITS` / `MAX_WAIT_SECONDS` in the DAG, not `retry_delay` |
| Verify the raw zone | `scripts/audit_partitions.py`, or `sql/date-spine-anti-join.sql` for absent dates |
| Query the raw zone ad hoc | `sql/00_setup.sql` first, then any drill file |
| Understand a past failure | `docs/incidents/` |
| Understand *why* something is the way it is | ROADMAP decision log |
| See what's left to build, in order | `docs/build-order.md`, or `pytest -rs` |
| Change a data-quality rule | `market_ops/quality/checks.py` (then the DAG, until it imports these) |
| Change what the agent may do | `market_ops/agent/tools.py` (`TOOLS`, `TOOL_SPECS`) and `guardrails.py` |
| Change the agent's instructions | `market_ops/agent/prompts.py`, then re-run the brief evals |
| Add an on-call runbook | `docs/runbooks/` with the standard seven H2 headings, then add golden cases |
| Add an eval case | `evals/golden/*.jsonl`; rules in `evals/README.md` |
| Run the offline demo | `python -m market_ops.agent demo-signal`, then `triage --latest --scripted tests/agent/fixtures/demo_transcript.json` |
