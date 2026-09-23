# Runbook: missing partitions, holidays, and catchup

A session with no `data/raw/ohlcv/dt=<session>/` directory. Most of the time that is correct: the market was closed.

## Symptoms

- A weekday missing from `data/raw/ohlcv/`.
- A skipped (pink) `fetch_ohlcv` with `AirflowSkipException: No bars returned for <date>; market likely closed due to being a weekend or market holiday`.
- After scheduler downtime, a burst of queued runs, or a gap where runs never existed.
- A `partition_missing` signal, once absent-partition detection is scheduled (it isn't yet; see Diagnosis).

## Triage

1. Is the date a weekend or a US market holiday? If yes, the missing partition is correct.
2. Does a DagRun exist for that session's fire time? `docker compose exec airflow-scheduler airflow dags list-runs ingest_ohlcv_daily`. The run for session `D` fires at 06:00 UTC on `D + 1`.
3. If a run exists and skipped, read its log for the `AirflowSkipException` message.
4. If no run exists, it is a scheduling gap. See "No DagRun was ever created".

## Diagnosis

| Situation | DagRun? | Partition? | Correct? |
|---|---|---|---|
| Weekend | Yes, skipped | No | Yes |
| Market holiday | Yes, skipped | No | Yes |
| Scheduler was down, now catching up | Queued/running | Arriving | Yes, wait |
| Interval before the first DagRun | Never | No | Gap |
| Run failed | Yes, failed | No (or the old one) | No: see ingest-ohlcv-task-failure.md |

### Market holidays

Holidays are detected by asking the vendor, not from a market calendar: an empty `results` means closed, and the run skips (decision log 2026-08-30, "invariant over a market calendar"). Across the raw zone the only missing weekdays are **2026-06-19** (Juneteenth), **2026-07-03** (Independence Day observed), and **2026-09-07** (Labor Day). All three were caught this way with no calendar dependency.

### Catchup after scheduler downtime

`catchup=True` fills forward from the **latest existing DagRun**. It never goes backwards into holes behind it, and `start_date` anchors the schedule only when no DagRun exists at all (decision log 2026-09-08, corrected 2026-09-16).

When the stack was down from 2026-09-09 to 2026-09-16, the scheduler queued **8 runs in 35 seconds** on restart (logical 09-08 → 09-15) and executed them back to back. The run count after an outage is simply the number of fire times missed. `max_active_runs=1` keeps that burst serial; without it the burst would hit Polygon's 5/minute limit at once.

Backfills ignore `max_active_runs` (they default to 10 concurrent runs), which is why `fetch_ohlcv` runs in the 1-slot `polygon` pool.

### No DagRun was ever created

`start_date` is 2026-06-01, but the 06-01 → 06-03 intervals have no DagRun and no partition, because catchup only fills forward from the latest run. Holes behind the latest run are never filled automatically. Fill them with a backfill.

### Detecting holes

`sql/date-spine-anti-join.sql` generates the weekdays in a range and anti-joins the raw zone to list the missing ones. Run `sql/00_setup.sql` first, from the repo root in DuckDB, and edit the date bounds in the query's `bounds` CTE. This is honest about its limit: **nothing schedules this query**, so an absent partition is detectable but not yet detected. Holidays will appear in its output and must be excluded by hand.

## Resolution

Safe unattended: listing runs, listing partitions, running the date-spine query.

Needs a human:
- Filling a gap with a backfill whose window spans the relevant 06:00 UTC fire times:

  ```
  docker compose exec airflow-scheduler airflow backfill create --dag-id ingest_ohlcv_daily \
    --from-date <first D+1>T00:00:00 --to-date <last D+1>T12:00:00 --dry-run
  ```

  Drop `--dry-run` once the planned runs are right. Weekends and holidays in the window will simply skip.
- Restarting a down scheduler (`docker compose up -d`).

## Do not

- Do not add a market-calendar dependency to "fix" holiday skips. The vendor's empty response is the calendar, and it is right.
- Do not flip `catchup` off to stop a post-outage burst. The burst is the recovery; `max_active_runs=1` keeps it safe.
- Do not raise `max_active_runs` or the pool size to finish a catchup faster. Throughput is bounded by the vendor, not by Airflow.

## Escalate when

- A weekday that is not a known holiday skipped with "No bars returned".
- A gap persists after a backfill over its window.
- Catchup queues far more runs than the length of the outage explains.

## References

- `sql/date-spine-anti-join.sql`, `sql/00_setup.sql`
- ROADMAP.md decision log: 2026-08-30 (invariant over a market calendar), 2026-09-01 (pool rather than `max_active_runs`), 2026-09-05 (holiday detection with evidence), 2026-09-08 and 2026-09-16 (what catchup actually does)
- partition-integrity.md for re-deriving a single session
