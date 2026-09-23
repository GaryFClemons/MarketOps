# Runbook: ingest_ohlcv_daily task failure

First response for a red `ingest_ohlcv_daily` run. Start here; the other runbooks go deeper on each cause.

## Symptoms

- A red `fetch_ohlcv` or `validate_partition` node in the Airflow UI (http://localhost:8080).
- A `task_failed` signal in `data/ops/signals/` (once the failure callback is wired).
- No `data/raw/ohlcv/dt=<session>/ohlcv.parquet` for a session that traded.

## Triage

1. **Which task failed?** The DAG is two tasks on purpose, so the red node tells you the class of problem before you open a log:
   - `fetch_ohlcv` red: the **vendor exchange** failed (auth, throttling, outage, bad payload, wrong dates, too many tickers missing).
   - `validate_partition` red: the **file on disk** is wrong, however it got there.
2. **Which session?** The run's session date is `data_interval_start` (one day before the 06:00 UTC fire time), never the wall clock. `dt=` in the partition path is that date.
3. **Read the log.** Host path: `logs/dag_id=ingest_ohlcv_daily/run_id=<run_id>/task_id=<task_id>/attempt=<n>.log`. Airflow 3 writes JSON lines; the failure line has `"level":"error"` and an `error_detail` list with `exc_type` and `exc_value`. On the Windows host, `:` in a run_id directory name appears as the character U+F03A.
4. **Classify by exception type.** This decides whether waiting helps (see Diagnosis).

## Diagnosis

The retry decision is made in code, by exception type, at the raise site.

| Exception in the log | Meaning | Airflow retries? | Go to |
|---|---|---|---|
| `AirflowFailException` | Deterministic: retrying returns the same wrong answer | No, fails immediately | cause below |
| `requests.HTTPError` | Transient vendor problem (429 after in-task waits, or 5xx) | Yes | vendor-auth-throttling-outage.md |
| `ValueError` | Retryable data condition (≥20% tickers missing), or `tickers is empty` | Yes | partition-integrity.md, config-and-secrets.md |
| `AirflowSkipException` | Nothing to do: market closed | Not a failure (pink/skipped) | missing-partition-and-catchup.md |
| `KeyError`, `TypeError`, other | A code bug or an unexpected payload shape | Yes, pointlessly | Escalate |

### fetch_ohlcv raised AirflowFailException

Read the message; each one is distinct:

| Message starts with | Cause |
|---|---|
| `Response code 401; Authentication/Permission Issue` (or 403) | Bad or revoked API key |
| `Response code <n>; Unexpected error, check the vendor's status page` | A non-200 status outside the taxonomy |
| `Vendor returned <n> rows; Possible vendor issue` | Truncated response: fewer than 8000 rows |
| `Some rows returned have a mismatched date` | Vendor answered for a different session than requested |

### validate_partition failed

`validate_partition` re-reads the parquet and checks: not empty (`ValueError: <path> is empty`), every row's `date` equals the `dt=` date (`AirflowFailException: Partition date is for ...`), no duplicate `(date, ticker)` rows, no null `close`. It inherits `retries: 3` from `default_args`, but it re-reads the same file each try, so a data problem fails identically four times.

### The task hung

`fetch_ohlcv` has `execution_timeout=15m`. A hang shows as a timeout failure after 15 minutes rather than a task holding the `polygon` pool slot forever.

## Resolution

Safe unattended (read-only):
- Read logs, check the partition with `python scripts/audit_partitions.py` from the repo root, list runs with `docker compose exec airflow-scheduler airflow dags list-runs ingest_ohlcv_daily`.

Needs a human:
- Fixing the cause (key, config, code).
- Re-running: clear the failed task (`docker compose exec airflow-scheduler airflow tasks clear ingest_ohlcv_daily -t fetch_ohlcv -s <start> -e <end>`) or re-derive the partition by backfill (partition-integrity.md). Re-running is safe **because** the write is idempotent: write to `ohlcv.parquet.tmp`, then an atomic rename over the target.

Retry policy for reference: `retries: 3`, `retry_delay` 2 minutes, exponential backoff, `max_retry_delay` 30 minutes.

## Do not

- Do not edit or hand-write a partition file. Re-derive it with a run so the idempotent write path produces it.
- Do not raise `retries` to get past an `AirflowFailException`; it was raised precisely because retrying cannot help.
- Do not trust a green run as proof the data is right. Both postmortems in `docs/incidents/` were green runs with wrong data.

## Escalate when

- The exception type is not in the Diagnosis table (a code bug).
- `validate_partition` fails on a partition that `fetch_ohlcv` just wrote: the two tasks disagree about the same data.
- The same failure repeats on consecutive sessions.

## References

- `dags/ingest_ohlcv_daily.py`
- ROADMAP.md decision log: 2026-08-27 (validation split into its own task; write-then-rename), 2026-09-01 (retryable `ValueError` vs `AirflowFailException`)
- docs/incidents/2026-08-30-stale-session-partition.md
- docs/incidents/2026-09-01-partial-vendor-response-overwrote-partition.md
