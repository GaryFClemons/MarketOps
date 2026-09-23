# Runbook: configuration and secrets

Where the pipeline's configuration lives, and the ways each piece has gone wrong.

## Symptoms

- `ValueError: tickers is empty` from `fetch_ohlcv`.
- A 401/403 right after a config change.
- A doubled scheme in the logged URL (`http://https://...`).
- The ticker universe in `.env` and in Airflow disagreeing.
- A change to `config/airflow.cfg` having no effect.

## Triage

1. Identify which object the task reads. `fetch_ohlcv` reads the Variable `tickers` and the Connection `polygon_default` inside the task, at run time. Neither is read from `.env`.
2. Check for environment-variable shadowing before editing anything in the UI.
3. Confirm what the running containers actually see: `docker compose exec airflow-scheduler airflow variables get tickers` and `airflow connections get polygon_default`.

## Diagnosis

| Object | Holds | Kind | Where it lives |
|---|---|---|---|
| Connection `polygon_default` | API key (`password`), `host=api.polygon.io`, `schema=https` | Secret | Metadata DB, Fernet-encrypted |
| Variable `tickers` | JSON list, 73 symbols | Operational parameter | Metadata DB |
| Pool `polygon` | 1 slot | Concurrency limit | Metadata DB; exported to `config/pools.json` |
| `.env` | Compose settings, a mirror of the tickers list | Deployment config | Host file, gitignored |

Two mechanisms for two reasons (decision log 2026-09-03): the key is a secret, encrypted at rest and masked in task logs as `***`; the ticker list is an operational parameter that should be editable without a container restart.

### Environment variables shadow the database

Resolution order is secrets backend → environment → metadata DB. An `AIRFLOW_CONN_*` or `AIRFLOW_VAR_*` variable silently wins over a DB row, with no warning and no UI indication. `airflow connections get` shows a row `id` for a DB-backed connection; an env-var connection has none.

### Host and scheme in separate fields

Putting `https://api.polygon.io` in `host` made `get_uri` produce `http://https://api.polygon.io`. `host=api.polygon.io` plus `schema=https` composes correctly (decision log 2026-09-03).

### Two copies of the ticker list

`TICKERS` in `.env` is only a mirror; the DAG reads the `tickers` Variable. They have already diverged once: `.env.example` still carried `ANSS` (74 symbols) after the Variable was trimmed to 73. Treat the Variable as the source of truth.

### config/airflow.cfg is not the effective config

`config/airflow.cfg` is the stock generated default. It says `LocalExecutor`, SQLite, `load_examples = True`, and none of that is live: the `AIRFLOW__*` environment variables in `docker-compose.yaml` take precedence. It is untracked because it contains a Fernet key.

## Resolution

Safe unattended: `airflow variables get`, `airflow connections get` (the key prints masked), `airflow pools list`.

Needs a human:
- Editing the Connection or the Variable.
- Restoring the pool after a metadata-DB rebuild: `airflow pools import config/pools.json`, run inside a container where the repo's `config/` is mounted at `/opt/airflow/config`.

## Do not

- Do not put the API key in `.env`, in code, or in a query string (`?apiKey=`). Query strings survive in proxy logs and exception text, where the secrets masker can't reach them. The request uses an `Authorization: Bearer` header.
- Do not read Variables or Connections at module level in a DAG file. The dag-processor re-parses the file roughly every 30 seconds, and in Airflow 3 each read is a round trip over the Task Execution API.
- Do not edit `config/airflow.cfg` expecting a change.

## Escalate when

- A Connection or Variable change doesn't take effect and no env var explains it.
- The Fernet key has been lost: encrypted connections become unreadable.

## References

- `dags/ingest_ohlcv_daily.py` (both reads inside `fetch_ohlcv`)
- `docker-compose.yaml`, `.env.example`, `config/pools.json`
- ROADMAP.md decision log: 2026-09-02 (Bearer header), 2026-09-03 (Connection vs Variable; reads inside the task body; env-var shadowing; host/schema split)
- codebase-map.md, "Configuration surface"
