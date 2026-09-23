"""Task-failure callback -> RunSignal.

Wired as ``default_args["on_failure_callback"]``. In Airflow 3 it runs in the
task-SDK process on the worker after the task has failed its *final* try
(``sdk/execution_time/task_runner.py``: ``_run_task_state_change_callbacks(task,
"on_failure_callback", context, log)``), with the full task context.

Context keys used here — verified in ``sdk/definitions/context.py``:
``ti`` (``dag_id``, ``task_id``, ``run_id``, ``try_number``), ``run_id``,
``data_interval_start``, and ``exception`` (``BaseException``, a string, or
missing).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from market_ops._scaffold import todo
from market_ops.agent.schemas import RunSignal


def build_failure_signal(context: Mapping[str, Any], *, now: datetime) -> RunSignal:
    """Turn a task-failure context into a ``RunSignal``. Pure.

    Returns:
        ``RunSignal`` with:
            kind           TASK_FAILED
            dag_id, task_id, run_id, try_number  from ``context["ti"]``
            session_date   ``context["data_interval_start"].date()`` when present
                           — the partition this run was responsible for
            detected_at    ``now``
            message        ``str(context["exception"])``, truncated to the
                           ``RunSignal.message`` limit (4000 chars); ``""`` if absent
            attributes     ``{"exception_type": <class name>}`` when the
                           exception is an object (e.g. ``"AirflowFailException"``);
                           omit the key when it's a string or missing
            signal_id      ``RunSignal.stable_id(kind, dag_id, run_id, task_id, try_number)``

    Why the exception type is its own field: it *is* the retry taxonomy.
    ``AirflowFailException`` means deterministic wrongness and
    ``HTTPError``/``ValueError`` means retries were exhausted on something
    transient — different runbooks, and the agent should not have to parse
    that out of a message.

    Why truncate: exception text can include a whole vendor response body. The
    signal is a pointer to the problem; the full text lives in the task log,
    which the agent can read with a tool.
    """
    todo("build_failure_signal: map the failure context (ti, exception, interval) to a TASK_FAILED RunSignal")


def on_task_failure(context: Mapping[str, Any]) -> None:
    """The ``on_failure_callback``. Build, write, log — and never raise.

    Behaviour: ``build_failure_signal(context, now=<UTC now>)`` ->
    ``write_signal(signal, Settings.from_env().signals_dir)`` -> log WARNING.
    Any exception is caught and logged: an alerting bug must never replace the
    task's real error in the log.
    """
    todo("on_task_failure: build + write the failure signal, log a warning, never raise")
