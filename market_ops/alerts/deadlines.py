"""Deadline alerts — Airflow 3's replacement for SLAs.

``sla=`` and ``sla_miss_callback`` were removed in Airflow 3. A ``DeadlineAlert``
declares "this run must finish within <interval> of <reference>"; when the
scheduler sees the deadline pass with the run unfinished, it queues a callback.

Verified against Airflow 3.3.1 (paths under .venv/Lib/site-packages/airflow/):

- ``sdk/definitions/deadline.py`` — ``DeadlineAlert(reference, interval, callback,
  name=None)``; references include ``DeadlineReference.DAGRUN_QUEUED_AT`` and
  ``DAGRUN_LOGICAL_DATE``. ``sdk/definitions/dag.py`` accepts
  ``deadline=DeadlineAlert | list[DeadlineAlert]``.
- ``sdk/definitions/callback.py`` — the callback is stored as an import path, so
  it must be a top-level callable importable where it runs. ``AsyncCallback``
  runs in the **triggerer**; ``SyncCallback`` runs on an executor.
- ``executors/base_executor.py`` — an executor must declare
  ``supports_callbacks = True`` to run a ``SyncCallback``; in core only
  ``LocalExecutor`` does. The Celery provider is not installed on the host, so
  its support could not be checked here. ``AsyncCallback`` avoids the question:
  compose runs an ``airflow-triggerer`` service.
- ``triggers/callback.py`` — the triggerer does
  ``await callback(**kwargs, context=context)`` when the callable accepts
  ``context`` (or ``**kwargs``).
- ``models/deadline.py::handle_miss`` — ``context`` is **not** a task context. It
  is ``{"dag_run": DAGRunResponse.model_dump(mode="json"), "deadline": {"id":
  ..., "deadline_time": ...}}``: the run's REST representation, so datetimes
  arrive as ISO strings and the run id is under ``"dag_run_id"``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from market_ops._scaffold import todo
from market_ops.agent.schemas import RunSignal

CALLBACK_PATH = "market_ops.alerts.deadlines.on_deadline_missed"

# How long after a run is queued it must be finished. A normal run takes seconds
# to a few minutes; the in-task 429 loop can add up to 5 x 90s = 7.5 minutes;
# execution_timeout is 15 minutes. Thirty minutes means "something is stuck, not
# merely slow". A tunable, not a law: tighten it once the audit log shows real
# run-time distributions.
DEADLINE_INTERVAL = timedelta(minutes=30)


def make_deadline_alert():
    """Build the ``DeadlineAlert`` for ``ingest_ohlcv_daily``.

    Returns:
        ``DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=DEADLINE_INTERVAL, callback=AsyncCallback(CALLBACK_PATH))``.
        Import ``DeadlineAlert``, ``DeadlineReference`` and ``AsyncCallback``
        from ``airflow.sdk`` *inside* this function, so this module stays
        importable without Airflow (tests, the agent CLI).

    Why queued_at and not logical_date: catchup and backfill runs carry logical
    dates days or weeks in the past. A logical-date deadline is already blown
    the moment such a run is created, so every backfill would page someone
    once per historical run — and a week of scheduler downtime queued 8 runs
    in 35 seconds on 2026-09-16. Anchoring on queued_at measures "the
    platform is slow or stuck", which is what an on-call person can act on.

    The trade-off, stated honestly: queued_at is *not* the business SLA
    ("yesterday's bars by 07:00 UTC"). That SLA is logical-date-anchored and
    would need backfill-run suppression to be usable. This is the Control-M
    distinction between a job's own runtime limit and the batch's
    must-complete-by time.

    Why the callback is a string path: Airflow stores the callback by import
    path and resolves it in the triggerer process, so it must be importable
    there — which is what the compose mount + ``PYTHONPATH`` provide.
    """
    todo("make_deadline_alert: DeadlineAlert anchored on DAGRUN_QUEUED_AT with an AsyncCallback(CALLBACK_PATH)")


def build_deadline_signal(context: Mapping[str, Any], *, now: datetime) -> RunSignal:
    """Turn a deadline-miss context into a ``RunSignal``. Pure.

    Args:
        context: the dict Airflow passes (see module docstring):
            ``context["dag_run"]`` has ``dag_id``, ``dag_run_id``, and ISO-string
            ``data_interval_start``, ``queued_at``, ``logical_date`` (any of the
            timestamps may be ``None``); ``context["deadline"]["deadline_time"]``
            is a datetime or ISO string.
        now: detection time, passed in so the function stays pure and testable.

    Returns:
        ``RunSignal`` with:
            kind           DEADLINE_MISSED
            dag_id, run_id from ``context["dag_run"]``
            task_id        None (a deadline is on the whole run)
            session_date   ``data_interval_start``'s date — the same derivation the
                           DAG uses for its partition — or None when absent
            detected_at    ``now``
            message        a sentence naming the run and the deadline time
            attributes     ``deadline_time`` and ``queued_at`` as strings
            signal_id      ``RunSignal.stable_id(kind, dag_id, run_id)``

    Why the id ignores time: the same run missing the same deadline is one
    event. If the callback fires twice, the second write overwrites the first
    signal file instead of paging twice.
    """
    todo("build_deadline_signal: map the deadline-miss context to a DEADLINE_MISSED RunSignal")


async def on_deadline_missed(context: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
    """The deadline callback. Runs in the triggerer.

    Behaviour:
        1. ``build_deadline_signal(context, now=<UTC now>)``
        2. ``write_signal`` to ``Settings.from_env().signals_dir`` — via
           ``await asyncio.to_thread(...)``, not a direct call
        3. log at WARNING with the signal id and path
        4. **never raise**: catch everything, log it with the traceback, return

    Why ``to_thread``: the triggerer runs every deferred task's trigger on one
    event loop. Blocking file IO inside an ``async def`` stalls all of them.

    Why never raise: a broken alert path must not mask the problem it was
    reporting. A failed callback shows up in the triggerer log; an exception
    here would just be one more thing on fire.
    """
    todo("on_deadline_missed: build + write the signal off the event loop, log a warning, never raise")
