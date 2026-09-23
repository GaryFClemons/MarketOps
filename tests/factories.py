"""Builders for realistic signals and briefs, shared by the agent and eval tests.

Messages are copied from the exceptions dags/ingest_ohlcv_daily.py actually
raises, so tests exercise the text the agent will really see.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from market_ops.agent.schemas import (
    ActionKind,
    Evidence,
    EvidenceSource,
    IncidentBrief,
    RecommendedAction,
    RunSignal,
    Severity,
    SignalKind,
)

RUN_ID = "scheduled__2026-09-18T06:00:00+00:00"
AUTH_MESSAGE = "Response code 401; Authentication/Permission Issue"


def make_signal(
    *,
    kind: SignalKind = SignalKind.TASK_FAILED,
    message: str = AUTH_MESSAGE,
    task_id: str | None = "fetch_ohlcv",
    try_number: int | None = 1,
    **overrides,
) -> RunSignal:
    fields = dict(
        signal_id=RunSignal.stable_id(kind, "ingest_ohlcv_daily", RUN_ID, task_id, try_number),
        kind=kind,
        dag_id="ingest_ohlcv_daily",
        run_id=RUN_ID,
        task_id=task_id,
        try_number=try_number,
        session_date=date(2026, 9, 17),
        detected_at=datetime(2026, 9, 18, 6, 2, tzinfo=UTC),
        message=message,
        attributes={"exception_type": "AirflowFailException"},
    )
    fields.update(overrides)
    return RunSignal(**fields)


def make_brief(signal: RunSignal | None = None, **overrides) -> IncidentBrief:
    """A grounded brief for the 401 signal. Its evidence refs and quotes match
    ``DOC_REF``/``DOC_TEXT`` and ``LOG_REF``/``LOG_TEXT`` below."""
    signal = signal or make_signal()
    fields = dict(
        signal_id=signal.signal_id,
        title="fetch_ohlcv rejected by vendor: 401 on polygon_default",
        severity=Severity.HIGH,
        summary="fetch_ohlcv failed without retrying because Polygon rejected the API key with a 401.",
        probable_cause="The API key in Connection polygon_default is invalid, expired, or revoked.",
        cause_confidence="confirmed",
        evidence=[
            Evidence(source=EvidenceSource.TASK_LOG, ref=LOG_REF, quote="Response code 401; Authentication/Permission Issue"),
            Evidence(source=EvidenceSource.DOC, ref=DOC_REF, quote="A bad key stays bad"),
        ],
        recommended_actions=[
            RecommendedAction(kind=ActionKind.INVESTIGATE, step="Confirm the key with: airflow connections get polygon_default"),
            RecommendedAction(kind=ActionKind.CONFIG_CHANGE, step="Rotate the API key in Connection polygon_default"),
            RecommendedAction(kind=ActionKind.RERUN, step="Clear fetch_ohlcv for this run once the key is fixed"),
        ],
        requires_human=True,
    )
    fields.update(overrides)
    return IncidentBrief(**fields)


LOG_REF = "dag_id=ingest_ohlcv_daily/run_id=scheduled__2026-09-18T06:00:00+00:00/task_id=fetch_ohlcv/attempt=1.log#L40-L42"
LOG_TEXT = (
    "40 | [2026-09-18T06:01:58Z] ERROR - Task failed\n"
    "41 | airflow.sdk.exceptions.AirflowFailException: Response code 401; Authentication/Permission Issue\n"
    "42 | [2026-09-18T06:01:58Z] INFO - Marking task as FAILED"
)
DOC_REF = "docs/runbooks/vendor-auth-throttling-outage.md::002"
DOC_TEXT = "### 401 or 403\n\nA bad key stays bad: the DAG raises AirflowFailException and does not retry."

SEEN = {LOG_REF: LOG_TEXT, DOC_REF: DOC_TEXT}
