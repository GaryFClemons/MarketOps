# Runbook: deadline missed

Airflow 3 removed SLAs (`sla=` and `sla_miss_callback` no longer exist). Deadline Alerts replace them: a DAG declares "a run must finish within an interval of a reference time", and when the scheduler sees that time pass with the run unfinished, it fires a callback.

**Status:** this platform's deadline alert is being wired and is not yet declared in the DAG. The scaffold in `market_ops/alerts/deadlines.py` anchors on the DagRun's `queued_at` with a 30-minute interval and writes a `deadline_missed` signal. Until it is wired, a stuck run is only noticed by looking.

## Symptoms

- A `deadline_missed` signal in `data/ops/signals/` (once wired).
- A run still queued or running long after its 06:00 UTC fire time.
- No new partition for yesterday's session by mid-morning UTC.

## Triage

1. Is the stack up? `docker compose ps`: `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`, and `airflow-apiserver` should all be healthy. API health: http://localhost:8080/api/v2/monitor/health.
2. What state is the run in? `docker compose exec airflow-scheduler airflow dags list-runs ingest_ohlcv_daily`.
3. If running, is `fetch_ohlcv` sleeping on 429s? Look for `Throttled by vendor; sleeping` in its log.
4. If queued, is it behind other runs (a catchup burst or a backfill)?

## Diagnosis

Likely causes, in the order to check them:

| Cause | How it looks | Why it blocks |
|---|---|---|
| A service is down | `docker compose ps` shows it unhealthy or exited | No scheduler means no runs; no worker means nothing executes |
| Queued behind a catchup burst | Several runs queued after downtime | `max_active_runs=1` runs them one at a time |
| Pool slot held | `fetch_ohlcv` running a long time | The `polygon` pool has 1 slot; one hung task blocks every other run |
| Vendor slow or throttling | `Throttled by vendor` warnings | Up to 5 in-task waits of up to 90s each, before any Airflow retry |

### Why queued_at and not logical_date

A deadline anchored on the logical date is already blown the moment a catchup or backfill run is created, because its logical date is in the past. Every historical run would alert. Anchored on `queued_at`, the alert means "this run is slow or stuck", which is actionable. The trade-off: it is not the business SLA ("yesterday's data by a fixed time"), which would need a logical-date deadline with backfill runs excluded.

### A hung task

`fetch_ohlcv` has `execution_timeout=15m`, so a hang turns into a failure within 15 minutes instead of holding the pool slot indefinitely. A run stuck longer than that points at the scheduler, the worker, or the queue, not the task.

## Resolution

Safe unattended: `docker compose ps`, reading logs, listing runs.

Needs a human:
- Restarting services (`docker compose up -d`).
- Clearing a stuck task instance so it reschedules.
- Changing the deadline interval, the pool, or `max_active_runs`.

## Do not

- Do not raise `max_active_runs` or the pool size to clear a backlog faster. The vendor limit is 5 requests per minute.
- Do not mark a run success by hand to silence the alert. The partition would still be missing.

## Escalate when

- A service won't stay healthy after a restart.
- Deadlines are missed on consecutive days with healthy services.
- A run is stuck with no running task and a free pool slot.

## References

- `market_ops/alerts/deadlines.py` (the scaffolded alert, with the Airflow 3.3.1 behaviour it relies on)
- `docker-compose.yaml` (services, health checks)
- ROADMAP.md decision log: 2026-09-01 (pool vs `max_active_runs`), 2026-09-08 (in-task 429 wait loop), 2026-09-16 (catchup burst after downtime)
- vendor-auth-throttling-outage.md, missing-partition-and-catchup.md
