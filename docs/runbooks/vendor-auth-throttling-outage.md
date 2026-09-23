# Runbook: Polygon auth failures, throttling, and outages

`fetch_ohlcv` makes one request per session: `GET /v2/aggs/grouped/locale/us/market/stocks/{session_date}?adjusted=false`, authenticated with an `Authorization: Bearer` header built from Connection `polygon_default`. This runbook covers every way that request fails.

## Symptoms

- `fetch_ohlcv` red with `AirflowFailException: Response code 401; Authentication/Permission Issue` (or 403).
- `fetch_ohlcv` retrying with `requests.HTTPError: Response code 429 after 5 in-task waits; retrying`.
- `fetch_ohlcv` retrying with `requests.HTTPError: Response code 5xx; retrying` (the real code in place of `5xx`).
- WARNING lines `Throttled by vendor; sleeping <n>s before attempt <m>` in the task log.
- A run that takes minutes instead of seconds.

## Triage

1. Find the status code in the task log's error line.
2. 401/403: check the connection, not the vendor.
3. 429: count the `Throttled by vendor` lines. Five waits means the in-task loop gave up and handed the problem to Airflow's retry.
4. 5xx: check whether the vendor is having an incident before touching anything here.

## Diagnosis

| Status | What the DAG does | Retries? |
|---|---|---|
| 200 | Parse the payload | n/a |
| 401 / 403 | `AirflowFailException` | No: a bad key stays bad |
| 429 | Sleep in-task, re-request, up to 5 times, then `requests.HTTPError` | Yes, after the in-task waits |
| 5xx | `requests.HTTPError` | Yes, 2m / 4m / 8m backoff |
| anything else | `AirflowFailException` ("Unexpected error") | No |

### 401 or 403 Unauthorized

The key in Connection `polygon_default` was rejected. Check, in order:

- The Connection exists and is the one being read: `docker compose exec airflow-scheduler airflow connections get polygon_default`. The API key is the `password` field; task logs mask it as `***`.
- It is not being shadowed. An `AIRFLOW_CONN_POLYGON_DEFAULT` environment variable silently takes precedence over the metadata-DB row. A DB-backed connection shows a row `id`; an env-var connection has none.
- `host` is `api.polygon.io` and `schema` is `https`, as two separate fields. Putting the scheme in `host` produced `http://https://api.polygon.io` (decision log 2026-09-03).

### 429 Too Many Requests

The free tier allows 5 requests per minute. The in-task loop sleeps for the `Retry-After` header value when it is all digits, otherwise 60 seconds, capped at `MAX_WAIT_SECONDS` = 90, and re-requests. After `MAX_THROTTLE_WAITS` = 5 waits it raises `requests.HTTPError` so Airflow's retry takes over.

Why two layers: throttling clears in seconds, so it is handled inside the task. Airflow's 2-minute `retry_delay` is mis-scaled to a limit that resets every 60 seconds (decision log 2026-09-08).

The `polygon` pool (1 slot) does **not** prevent 429s. A pool bounds concurrency, not throughput: measured on 2026-09-04, a 1-slot pool with a ~7-second task still issued ~8.5 requests/minute against the 5/minute limit. 429 bursts are most likely during a backfill or a catchup burst after scheduler downtime.

### 5xx Server Error

The vendor's problem. The task raises `requests.HTTPError` and Airflow retries with exponential backoff (2, 4, 8 minutes; `max_retry_delay` 30 minutes). If all retries fail, the session is missing until re-run.

### Payload gates after a 200

A 200 is not success yet:

- `results` missing or empty: `AirflowSkipException` ("No bars returned for <date>; market likely closed"). A weekend returns `{"resultsCount": 0, "status": "OK"}` with no `results` key at all.
- Fewer than 8000 rows: `AirflowFailException` ("Vendor returned <n> rows; Possible vendor issue"). A normal session is ~12,500 rows; a truncated response can still contain every tracked ticker, which the ticker-level check cannot see.

## Resolution

Safe unattended: reading logs, `airflow connections get polygon_default` (the key prints masked), checking the vendor's status page.

Needs a human:
- Rotating the key in `polygon_default` (a config change).
- Clearing `fetch_ohlcv` to re-run after a 401/403 fix or a long outage.
- Anything that changes the pool, `MAX_THROTTLE_WAITS`, or `MAX_WAIT_SECONDS`.

## Do not

- Do not put the API key in a query string (`?apiKey=`). Query strings end up in proxy logs, exception messages, and shell history, where Airflow's masker cannot reach them.
- Do not "fix" 429s by raising `retry_delay`. The problem lasts seconds; the fix is pacing.
- Do not add pool slots to go faster. Concurrency makes throttling worse, not better.
- Do not put the key in `.env`; it was deleted from there on purpose (decision log 2026-09-03).

## Escalate when

- 401/403 persists after confirming the connection and key.
- 5xx persists across all retries for more than one session.
- 429 appears on a single scheduled daily run with no backfill or catchup in flight: one request per day should never be throttled.

## References

- `dags/ingest_ohlcv_daily.py` (`MAX_THROTTLE_WAITS`, `MAX_WAIT_SECONDS`, the status-code chain)
- ROADMAP.md decision log: 2026-09-02 (Bearer header; explicit status taxonomy), 2026-09-03 (Connection and host/schema split; env-var shadowing; skip on empty `results`; 8000-row gate), 2026-09-04 (pool bounds concurrency, not throughput), 2026-09-08 (in-task 429 wait loop)
- config-and-secrets.md
