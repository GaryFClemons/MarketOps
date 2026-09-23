# Runbook: partition integrity (wrong dates, missing tickers, duplicates, nulls)

The raw zone is `data/raw/ohlcv/dt=YYYY-MM-DD/ohlcv.parquet`, one file per trading session, grain `(date, ticker)`. This runbook covers a partition that exists but is wrong.

## Symptoms

- `fetch_ohlcv` fails with `AirflowFailException: Some rows returned have a mismatched date; <date> was requested but below dates are being returned instead`.
- `fetch_ohlcv` fails with `ValueError: <pct>% of requested tickers are missing (<n> of <m>) from the vendor response`.
- A WARNING that some tickers are missing, while the task still succeeds.
- `validate_partition` fails: `Partition date is for <dt> but dates for <n> of <m> rows are mismatched`, `<n> duplicate (date, ticker) rows`, `<n> rows with null close`, or `<path> is empty`.
- `scripts/audit_partitions.py` prints `MISMATCH` or `NO FILE`.

## Triage

1. Run the audit from the repo root: `python scripts/audit_partitions.py`. Each partition prints `ok` or `MISMATCH <dates>`, plus a 12-character SHA-256 prefix of the file.
2. Note which task raised. `fetch_ohlcv` means the vendor response was wrong and nothing was written. `validate_partition` means the file on disk is wrong.
3. For a completeness warning, list the missing tickers from the log and compare them with the `tickers` Variable.

## Diagnosis

| Signal | Cause | Raised by | Retries? |
|---|---|---|---|
| Rows dated a different session | Vendor answered a different request | `fetch_ohlcv` (`AirflowFailException`) or `validate_partition` | No |
| A few tickers missing, task green | Ticker didn't trade, or the symbol is dead (list drift) | `fetch_ohlcv` WARNING | n/a |
| ≥20% of tickers missing | Large list drift, or a bad response | `fetch_ohlcv` (`ValueError`) | Yes |
| Duplicate `(date, ticker)` | Reshape bug double-counted | `validate_partition` (`ValueError`) | Yes, pointlessly |
| Null `close` | Bad vendor row | `validate_partition` (`ValueError`) | Yes, pointlessly |

### Rows dated the wrong session

The invariant: every row in `dt=D` has `date == D`. The session date comes from the run's `data_interval_start`; Polygon's `t` (bar close, epoch milliseconds) is only checked against it, never used as the source (decision log 2026-09-03).

History: on 2026-08-30 the partition `dt=2026-08-29` (a Saturday) held 73 rows dated 2026-08-28, and every check passed, because every check compared the data to itself. See docs/incidents/2026-08-30-stale-session-partition.md. Polygon's grouped-daily endpoint returns the requested date or nothing, so this is now a guard against a regression rather than an expected condition.

### Missing tickers and list drift

Under grouped-daily a partial response is not possible: you get the whole market or nothing. A missing ticker therefore means the ticker didn't trade that session, or the symbol no longer exists. The gate warns on any miss and fails only at ≥20% missing (decision log 2026-09-03). A ticker missing two sessions running is a list-maintenance bug. Example: `ANSS` was removed on 2026-09-16 after Synopsys acquired Ansys; `SNPS` was already in the universe.

History: on 2026-09-01, under the previous vendor, a throttled response returned 28 of 73 tickers, stayed green, and overwrote a complete partition. See docs/incidents/2026-09-01-partial-vendor-response-overwrote-partition.md.

### Duplicate rows or null close

The grain is `(date, ticker)`. A duplicate silently inflates every downstream aggregate. `close` has no valid reason to be null on a trading day. Both indicate a code or vendor defect, and re-reading the same file will not fix either.

## Resolution

Safe unattended: `python scripts/audit_partitions.py`, reading the parquet, comparing digests.

Needs a human, re-deriving one partition. Re-running is safe because the write is idempotent (write-then-rename to a deterministic path); idempotency was proven by running the same interval twice and matching SHA-256 digests. To re-derive session `D`:

- **Backfill.** The window must contain the 06:00 UTC fire time on `D + 1`, because `session_date = fire time − 1 day`. A zero-width window such as `--from-date D --to-date D` creates no runs and only warns (decision log 2026-09-05). `--reprocess-behavior` defaults to `none`, which skips dates that already have a run, so pass `completed` (or `failed`) to redo one, and preview with `--dry-run`:

  ```
  docker compose exec airflow-scheduler airflow backfill create --dag-id ingest_ohlcv_daily \
    --from-date <D+1>T00:00:00 --to-date <D+1>T12:00:00 --reprocess-behavior completed --dry-run
  ```

- **Manual trigger.** A manual run's interval derives from its `logical_date`, so `session_date = logical_date − 1 day` (corrected in the decision log on 2026-09-05; the earlier claim that manual runs can't address history was wrong).

Then re-run the audit and confirm the partition reads `ok`.

## Do not

- Do not raise the 20% threshold to silence a warning. Fix the ticker list.
- Do not add completeness back into `validate_partition`. It needs the ticker list, which is external state, so it belongs in `fetch_ohlcv` (decision log 2026-09-03).
- Do not delete a bad partition by hand and leave the hole. Re-derive it.

## Escalate when

- Date mismatches reappear: Polygon returning a different session would break the migration's core assumption.
- Duplicates or nulls appear: that is a code or vendor defect.
- The same ticker set goes missing across many sessions.

## References

- `scripts/audit_partitions.py`
- ROADMAP.md decision log: 2026-08-30 (validate response against request), 2026-09-01 (completeness gate), 2026-09-03 (completeness reinterpreted as list drift; date from interval), 2026-09-05 (backfill window must span a fire time; manual runs derive from logical_date)
- docs/incidents/2026-08-30-stale-session-partition.md
- docs/incidents/2026-09-01-partial-vendor-response-overwrote-partition.md
