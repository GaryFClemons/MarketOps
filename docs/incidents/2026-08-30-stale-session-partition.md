# Incident: stale session data written to the wrong partition

| | |
|---|---|
| **Date found** | 2026-08-30 |
| **Severity** | High (silent data corruption) — contained, no downstream consumers yet |
| **Component** | `dags/ingest_ohlcv_daily.py`, raw zone |
| **Status** | Closed 2026-09-01 — root cause found, fix verified, all remediation complete |

---

## Summary

The ingestion DAG wrote a partition keyed `dt=2026-08-29` containing 73 rows whose `date` column read `2026-08-28`. 2026-08-29 was a Saturday; US markets were closed and no bars existed for that date. The vendor returned the previous trading session's data instead of an empty response, and the DAG accepted it without checking.

Every automated data-quality check passed. The partition looked healthy.

---

## Timeline

| When | What |
|---|---|
| 2026-08-27 | DAG built and verified against `dt=2026-08-26`, a normal trading day. Correct output. |
| 2026-08-30 | Ticker universe expanded from 5 to 73. DAG re-run for 2026-08-26, 2026-08-28, 2026-08-29. |
| 2026-08-30 01:06 UTC | Run for 2026-08-29 logs `Wrote 73 rows for 73 tickers to .../dt=2026-08-29/ohlcv.parquet`. `validate_partition` passes. |
| 2026-08-30 | Partition contents inspected manually after noticing 2026-08-29 was a Saturday. `date` column reads 2026-08-28 across all 73 rows. |
| 2026-08-30 | Root cause identified; guard added; bad partition deleted. |

---

## Impact

None realised. The raw zone has no downstream consumers yet — no dbt models, no warehouse, no reporting.

Had this reached production, the failure mode would have been:

- Friday's closing bars attributed to Saturday in every query filtering on `dt`.
- Duplicate price observations for 2026-08-28 across two partitions, inflating any `COUNT` or `SUM` over a date range.
- A phantom trading day on a weekend, corrupting any moving average, volatility window, or return calculation spanning it.

None of these would surface as an error. They would surface weeks later as "the numbers look wrong."

---

## Root cause

The task requested a specific date range and never verified that the response matched the request.

```python
raw = yf.download(TICKERS, start=session_date, end=session_date + 1 day, ...)
```

For a closed-market date, yfinance does not reliably return an empty frame. It returned the most recent prior session's bars. The code's only emptiness guard was:

```python
if not frames:
    raise AirflowSkipException(...)
```

That catches "the vendor returned nothing." It does not catch "the vendor returned something, but not what was asked for."

The partition key was derived from `data_interval_start` — correctly. The row dates came from the vendor. Nothing compared the two. The directory name asserted a fact about its contents that was never enforced.

### Why the existing validation missed it

`validate_partition` checked four things:

- non-empty
- expected tickers present
- no duplicate `(date, ticker)` pairs
- no null `close`

All four passed, and all four would pass on a perfectly-formed partition of the *wrong day's* data. Every check validated the file against itself. None validated it against what was requested.

**Internal consistency is not correctness.** That is the generalizable lesson.

### Contributing factor: inconsistent vendor behaviour

The same vendor behaves differently on different closed-market dates:

- Request for 2026-08-29 (Saturday) → returned Friday's bars.
- Request for 2026-08-31 → returned nothing, task skipped correctly.

Because the behaviour is not predictable, it cannot be special-cased. It has to be validated on every request.

---

## Fix

An invariant, enforced before any filesystem write:

> Every row in partition `dt=D` has `date == D`.

```python
if (frame["date"] != session_date).any():
    raise AirflowSkipException(...)
```

Placed after normalisation and *before* `partition.mkdir(...)`, so a rejected fetch leaves no empty partition directory behind.

### Why an invariant rather than a calendar

The obvious fix is to skip weekends. It was rejected:

- It does not cover market holidays — Thanksgiving and Independence Day are weekdays with no session. Same bug, different date.
- It requires a market-calendar dependency and keeping that calendar current.
- It still trusts the vendor on every other day, including half-days and early closes.

Comparing returned dates to the requested date covers weekends, holidays, half-days, timezone drift, and vendor quirks with one check and no new dependency.

---

## Root cause, revised (2026-08-31)

The analysis above is correct but stops at the proximate cause. It explains why the
bad data was *accepted*. It does not explain why the vendor was asked for a session
that did not exist.

The DAG declared `schedule="0 6 * * *"` — a bare cron string. In Airflow 3 the
`scheduler.create_cron_data_intervals` setting defaults to `False`, which means a
bare cron string no longer builds a `CronDataIntervalTimetable`. It builds a
`CronTriggerTimetable`, whose run info is constructed as:

```python
DagRunInfo.interval(next_start_time - self._interval, next_start_time)
```

with `_interval` defaulting to `timedelta()` — **zero width**. So
`data_interval_start == data_interval_end == the cron fire time`.

Under Airflow 2 semantics, the run firing at D 06:00 UTC covered D-1 06:00 → D 06:00,
and `data_interval_start.date()` was the previous day's session. Under Airflow 3's
default it resolved to **D itself** — 06:00 UTC is 02:00 ET, hours before that day's
session opens.

Every scheduled run was therefore asking for a session that had not happened yet.
For 2026-08-29 the vendor answered with the most recent session it had. The partition
key and the row dates disagreed because the *request* was wrong, not merely unchecked.

Evidence, from the scheduler log at the moment of the fix:

```
last_automated_run_info=DagRunInfo(
    data_interval=DataInterval(start=2026-08-30 06:00, end=2026-08-30 06:00))
```

`start == end`. A persisted zero-width interval on the last run created under the
old configuration.

### Fix

Pass an explicit timetable rather than a bare cron string:

```python
CronTriggerTimetable("0 6 * * *", timezone="UTC", interval=timedelta(days=1))
```

Rejected alternative: setting `create_cron_data_intervals = True` globally. It works,
but makes correctness depend on a deployment-level config flag that isn't in version
control — the DAG would be silently wrong on any default Airflow 3 install.

**Generalizable lesson, second order:** a framework default changed the meaning of an
unchanged line of code. The DAG file was byte-identical before and after Airflow 3 and
its behaviour inverted. Anything load-bearing should be declared, not inherited.

---

## Remediation

- [x] Bad `dt=2026-08-29` partition deleted.
- [x] Guard added to `fetch_ohlcv`.
- [x] **Guard exercised** *(2026-09-01)*. Both branches fired with log evidence — all-mismatch
      skips, partial-mismatch fails.
- [x] **`.any()` vs `.all()` resolved** *(2026-09-01)*. All-mismatch raises `AirflowSkipException`
      (market closed, benign). Some-mismatch raises `AirflowFailException` and names the
      offending tickers — no retries, because the vendor's answer is deterministic.
- [x] **Gap at `dt=2026-08-27` closed via backfill** *(2026-09-01)*, not by hand.
- [x] **Invariant asserted in `validate_partition`** *(2026-09-01)*, comparing the `date`
      column against the date parsed out of the partition path. Not redundant: `fetch_ohlcv`
      validates the vendor's response, `validate_partition` validates the file on disk.
- [x] **Root cause identified and fixed** *(2026-08-31)*. See above.

### Still open, tracked in the roadmap

- Detecting an **absent** partition. Nothing in the system would have reported that
  `dt=2026-08-27` was missing — the audit script only iterates directories that exist.
  Requires a date spine to join against (Week 2).
- [ ] `validate_partition` still does not assert the date/partition-key invariant. Defence in depth is missing if a file arrives by any path other than `fetch_ohlcv`.

---

## Lessons

1. **Validate the response against the request.** An upstream source returning *something* is not evidence it returned the *right* thing. This applies to every API, file drop, and database extract in a pipeline.
2. **A partition key is an assertion. Enforce it.** `dt=D` claims every row belongs to D. If nothing checks that, the directory name is a comment, and comments drift from reality.
3. **Self-consistent checks miss whole classes of bug.** Every quality check here compared the data to itself. Add at least one check that compares it to an external expectation.
4. **Skipped is not free.** It is the correct state for "nothing to do," but it looks benign in the UI. Using it for anomalies buries them.
5. **Verifying against a known-good case proves less than you think.** The DAG was validated against a normal trading day and looked correct. The bug only existed on the days that were never tested. Test the boundaries — weekends, holidays, the first and last day of a range, empty responses.
