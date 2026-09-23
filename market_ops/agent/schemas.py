"""Contracts for the triage agent: RunSignal in, IncidentBrief out.

These are written first and kept strict because they are the part of the AI
layer that everything else depends on: alert callbacks produce signals, the
agent consumes them, a human on call reads the brief, and the eval harness
scores it. The model is free to reason however it likes; it is not free to
return a different shape.

Three design choices worth defending:

1. **The brief is structured, not prose.** A free-text summary cannot be
   validated, diffed, or scored. Every field here is something a guardrail or
   an eval can check mechanically — severity is an enum, confidence is a
   calibrated label rather than a made-up float, and every claim has to point
   at evidence.

2. **``extra="forbid"`` everywhere.** A model that invents a field
   (``"root_cause_certainty": 0.93``) fails validation instead of silently
   smuggling unvalidated content into the record.

3. **The agent recommends; it never acts.** ``RecommendedAction.kind`` marks
   which steps change state (rerun, backfill, config change). The guardrail
   layer requires ``requires_human=True`` whenever one of those is present.
   Autonomy matches reversibility: reading logs can run unattended; touching a
   system of record goes behind a person.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SignalKind(StrEnum):
    TASK_FAILED = "task_failed"
    DEADLINE_MISSED = "deadline_missed"
    PARTITION_MISSING = "partition_missing"
    CHECK_WARNING = "check_warning"


class RunSignal(BaseModel):
    """Something happened that a person on call would want triaged.

    Emitted by ``market_ops.alerts`` from inside Airflow and written as one JSON
    file per signal. ``message`` carries raw error text from a log or a vendor
    response, so it is **untrusted input** to the model and is fenced as data,
    never concatenated into instructions.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    signal_id: str = Field(min_length=1)
    kind: SignalKind
    dag_id: str
    run_id: str | None = None
    task_id: str | None = None
    try_number: int | None = None
    session_date: date | None = Field(
        default=None, description="The dt= partition the run was responsible for."
    )
    detected_at: datetime
    message: str = Field(default="", max_length=4000)
    attributes: dict[str, str] = Field(default_factory=dict)

    @staticmethod
    def stable_id(*parts: object) -> str:
        """Deterministic id from the fields that identify an event.

        Same event -> same id, so an Airflow callback that fires twice writes the
        same file twice (an overwrite) rather than two signals — the same
        overwrite-based idempotency the raw zone relies on.
        """
        key = "|".join("" if p is None else str(p) for p in parts)
        return hashlib.sha256(key.encode()).hexdigest()[:16]


class Severity(StrEnum):
    # Ordered by consumer harm, and the order is the point: silently *wrong*
    # data outranks loudly *missing* data. Both incidents in docs/incidents/
    # were wrong-but-green, which is the failure that reaches a trader's screen.
    CRITICAL = "critical"  # wrong data was, or may have been, published
    HIGH = "high"  # expected data is missing past its deadline
    MEDIUM = "medium"  # degraded or at risk; no consumer impact yet
    LOW = "low"  # informational, self-healing, or already resolved


CauseConfidence = Literal["confirmed", "likely", "unknown"]
"""A label, not a probability. ``confirmed`` must be backed by first-hand
evidence (a task log line or a partition check), not only by a runbook that
says this kind of thing usually happens — the guardrail enforces that."""


class EvidenceSource(StrEnum):
    DOC = "doc"  # a retrieved chunk: runbook, incident, decision log
    TASK_LOG = "task_log"  # a line range from an Airflow task log
    RUN_METADATA = "run_metadata"  # DagRun / TaskInstance state from a tool call
    PARTITION = "partition"  # a check run against a dt= partition on disk


FIRST_HAND_SOURCES = frozenset(
    {EvidenceSource.TASK_LOG, EvidenceSource.RUN_METADATA, EvidenceSource.PARTITION}
)


class Evidence(BaseModel):
    """One citation. ``ref`` must resolve to something the agent actually saw.

    ``ref`` formats:
        DOC           -> a ``Chunk.chunk_id`` returned by search_docs
        TASK_LOG      -> ``"<log path>#L<start>-L<end>"``
        RUN_METADATA  -> the tool-call id that returned it
        PARTITION     -> the tool-call id that returned it
    """

    model_config = ConfigDict(extra="forbid")

    source: EvidenceSource
    ref: str = Field(min_length=1)
    quote: str = Field(min_length=1, max_length=400)


class ActionKind(StrEnum):
    INVESTIGATE = "investigate"  # read-only; safe unattended
    RERUN = "rerun"  # clear / re-trigger a task or run
    BACKFILL = "backfill"
    CONFIG_CHANGE = "config_change"  # Variable, Connection, pool, or code
    ESCALATE = "escalate"  # page a human or the vendor


MUTATING_ACTIONS = frozenset({ActionKind.RERUN, ActionKind.BACKFILL, ActionKind.CONFIG_CHANGE})


class RecommendedAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: ActionKind
    step: str = Field(min_length=1, max_length=300)


class IncidentBrief(BaseModel):
    """The standardized output. One per RunSignal.

    This is also the JSON schema handed to the model as the input schema of the
    terminal ``submit_brief`` tool, so the provider enforces the shape at
    generation time and pydantic re-checks it on the way in.
    """

    model_config = ConfigDict(extra="forbid")

    signal_id: str
    title: str = Field(min_length=1, max_length=120)
    severity: Severity
    summary: str = Field(min_length=1, max_length=800)
    probable_cause: str = Field(min_length=1, max_length=800)
    cause_confidence: CauseConfidence
    evidence: list[Evidence] = Field(min_length=1)
    recommended_actions: list[RecommendedAction] = Field(min_length=1)
    requires_human: bool
