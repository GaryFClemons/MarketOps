# Incident: a throttled vendor response silently overwrote a complete partition

| | |
|---|---|
| **Date found** | 2026-09-01 |
| **Severity** | High (silent data loss) — contained, no downstream consumers yet |
| **Component** | `dags/ingest_ohlcv_daily.py`, raw zone |
| **Status** | Fixed and verified; vendor strategy still open |

---

## Summary

A re-run of `dt=2026-08-28` returned only 28 of 73 requested tickers. The task
logged `Wrote 28 rows for 28 tickers`, reported **success**, and replaced a
previously complete 73-row partition.

Every data-quality check passed. The partition was internally consistent, correctly
dated, free of duplicates and nulls. It was simply missing 62% of the universe.

This is the same failure class as the [2026-08-30 incident](2026-08-30-stale-session-partition.md),
one level up: the file was consistent with itself and wrong about the world. The
difference is that this time the pipeline destroyed good data it had already landed.

---

## Timeline

| When | What |
|---|---|
| 2026-09-01 ~20:30 | `fetch_ohlcv` attempt 8 logs 45 failed downloads, writes 28 rows, exits green. |
| 2026-09-01 | Noticed while reading task logs for an unrelated verification. Not surfaced by any check. |
| 2026-09-01 | yfinance source inspected; per-symbol request pattern confirmed. |
| 2026-09-01 | Completeness gate added to `fetch_ohlcv` and `validate_partition`. |
| 2026-09-01 23:27 | Attempt 10 hits the same condition (35 of 73 missing) and **fails** at 47.9%. No partition written. |

---

## Impact

None realised. The raw zone has no downstream consumers — no dbt models, no
warehouse, no reporting. The partition was rebuilt from the vendor afterwards.

Had it reached production, the failure mode would have been a day where 62% of the
universe silently vanished from every aggregate, with no error anywhere. Index
returns, breadth measures, and any cross-sectional ranking spanning that date would
have been wrong, and nothing would have said so.

---

## Root cause

Three things stacked.

**1. The vendor call is 73 requests, not one.**

`yfinance.multi._download_impl` loops:

```python
for ticker in tickers:
    _download_one(ctx, ticker, ...)
```

`threads` only decides whether each `_download_one` runs on its own thread. The
batching is client-side. Yahoo has no historical basket endpoint, so 73 symbols is
always 73 HTTP requests.

**2. Yahoo throttled, and yfinance reported it as missing data.**

The error text is `possibly delisted; no price data found`. That message is emitted
for *any* empty response, including a throttled one. ASML, AMAT, ADBE, PEP and TMUS
are not delisted — a single-symbol request for the same date returned data normally.

The vendor **failed open**: it degraded silently instead of returning an error.

**3. Nothing gated on completeness before the write.**

Missing tickers were a `log.warning` in `validate_partition`. Warnings don't stop
writes and don't fail tasks.

### Why the "idempotency" guarantee didn't help

The DAG's docstring claimed that re-running an interval produces a byte-equivalent
partition. That had been measured and held.

The claim was too broad. What is actually idempotent is the **write path** —
deterministic target, atomic rename, overwrite rather than append. The **fetch** is
not idempotent, because the vendor is not deterministic.

Overwrite-based idempotency assumes a stable source. Against a flaky one it becomes a
liability: a retry can replace good data with worse data, and the aggressive retry
policy makes that *more* likely, not less.

**Generalizable lesson:** "idempotent" is not a property of a pipeline. It is a
property of a specific step, against a specific source, and the claim has to name both.

---

## Fix

**Completeness gate, before the write, in both tasks.** Fails above 15% missing,
warns below. Present in `fetch_ohlcv` (validating the vendor response) and in
`validate_partition` (validating the file on disk) — different threat models, same
invariant.

**Retryable exception, deliberately.** Missing tickers raise `ValueError`, not
`AirflowFailException`, because throttling clears and the 2/4/8-minute backoff is
exactly the right response. Contrast with the date-mismatch guard, which uses
`AirflowFailException` because a vendor returning the wrong session will return the
wrong session again.

**`threads=False`.** Removed burst concurrency without introducing tunables.

### Verified

Attempt 10 reproduced the condition and behaved correctly:

```
warning  47.9% of requested tickers (35 of 73) are missing from the vendor response
ValueError: 47.9% of requested tickers are missing (35 of 73)
Task failed with exception
```

No partition written. The complete file survived.

---

## What `threads=False` did not fix

Failures persisted at roughly **6 requests/second sustained** across ~12 seconds.
Removing concurrency removed the burst but not the throughput, which establishes that
the constraint is volume per unit time rather than parallelism.

That result is what rules out tuning as a solution. The fix has to change the *shape*
of the request, not its pacing.

---

## Open

- **`seed_ohlcv_history` DAG.** yfinance can return a wide date range for one symbol in
  a single request. Flipping the axis turns a 90-day backfill from 6,570 requests into
  73. Belongs in its own DAG because one fetch produces many partitions, which breaks
  the daily DAG's one-run-one-partition contract.
- **Daily-DAG pacing** once the throttle clears. 73 requests once per day is a
  non-problem; the throttling came from ~50 development runs in four hours.
- **Keyed vendor** remains an option rather than a rescue. An authenticated endpoint
  with a published rate limit and real status codes is a better input than one that
  fails open, but the current design works within yfinance's actual limits.
- **Threshold rationale.** 15% is currently a judgment call, not a derived number.
