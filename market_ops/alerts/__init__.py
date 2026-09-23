"""The bridge from Airflow to the AI layer.

Airflow callbacks turn "a task failed" or "a run missed its deadline" into a
``RunSignal`` JSON file under ``Settings.signals_dir``; the triage agent reads
those files. The handoff is a directory on purpose:

- **Decoupled.** Airflow never imports the agent, needs an LLM key, or waits on
  a model call. An LLM outage cannot slow down or fail an ingestion run.
- **Durable and replayable.** A signal survives a crashed agent and can be
  re-triaged after a prompt change — which is exactly what an eval needs.
- **Inspectable.** ``cat`` is the debugger.

At larger scale this becomes a queue or the Airflow REST API; see
docs/architecture.md.

Wiring into the DAG (owner does this)
-------------------------------------

1. ``default_args["on_failure_callback"] = on_task_failure``
   (``from market_ops.alerts.failures import on_task_failure``).
2. ``@dag(..., deadline=make_deadline_alert())``
   (``from market_ops.alerts.deadlines import make_deadline_alert``).
3. The package must be importable inside the containers: docker-compose.yaml
   mounts ``./market_ops`` at ``/opt/airflow/src/market_ops`` and sets
   ``PYTHONPATH=/opt/airflow/src``. Recreate with ``docker compose up -d``.
"""
