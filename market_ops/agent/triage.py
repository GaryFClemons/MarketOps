"""The triage loop: RunSignal in, IncidentBrief out, every step audited.

A plain tool-use loop (ReAct-shaped: the model reasons, calls a tool, reads the
result, repeats), with a terminal ``submit_brief`` tool instead of free-text
output. Guardrails wrap every edge: a budget before each model call, fencing on
every untrusted tool result, validation on the brief, and a deterministic
fallback on every failure path.

Why one agent and not several: the task is sequential and small — find the
failing task, read its log, check the partition, look up the runbook. A
planner/worker split would add a second failure surface and a second set of
prompts to evaluate for no capability this loop lacks.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel

from market_ops._scaffold import todo
from market_ops.agent.audit import AuditLog
from market_ops.agent.guardrails import Budget
from market_ops.agent.llm import LLMClient, Usage
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
from market_ops.agent.tools import ToolContext

StopReason = Literal[
    "submitted", "budget_exhausted", "invalid_brief", "model_refused", "max_tokens", "no_submission", "error"
]


class TriageResult(BaseModel):
    """Outcome of one run. ``brief`` is always present — the model's, or the fallback."""

    trace_id: str
    signal_id: str
    brief: IncidentBrief
    fallback_used: bool
    stop_reason: StopReason
    violations: list[str] = []
    turns: int
    tool_calls: int
    usage: Usage
    latency_ms: float


_FALLBACK_SEVERITY = {
    SignalKind.TASK_FAILED: Severity.HIGH,
    SignalKind.DEADLINE_MISSED: Severity.HIGH,
    SignalKind.PARTITION_MISSING: Severity.HIGH,
    SignalKind.CHECK_WARNING: Severity.MEDIUM,
}


def fallback_brief(signal: RunSignal, reason: str) -> IncidentBrief:
    """A deterministic brief for when the agent can't produce a validated one.

    Implemented, and deliberately dumb: severity comes from the signal kind,
    confidence is "unknown", a human is required, and the only evidence is the
    raw signal itself. The AI layer is allowed to degrade; it is never allowed
    to drop a signal on the floor. Whoever is on call still gets paged with
    everything the alert knew.
    """
    quote = signal.message[:400] if signal.message else f"{signal.kind} for {signal.dag_id}"
    return IncidentBrief(
        signal_id=signal.signal_id,
        title=f"Needs manual triage: {signal.kind} in {signal.dag_id}"[:120],
        severity=_FALLBACK_SEVERITY[signal.kind],
        summary=(
            f"Automated triage did not produce a validated brief ({reason}). "
            "The raw signal is attached as evidence."
        )[:800],
        probable_cause="Unknown: automated triage stopped before a cause was established.",
        cause_confidence="unknown",
        evidence=[Evidence(source=EvidenceSource.RUN_METADATA, ref=f"signal:{signal.signal_id}", quote=quote)],
        recommended_actions=[
            RecommendedAction(
                kind=ActionKind.ESCALATE,
                step="Triage manually from the signal and the task log; start at docs/runbooks/ingest-ohlcv-task-failure.md.",
            )
        ],
        requires_human=True,
    )


def run_triage(
    signal: RunSignal,
    *,
    llm: LLMClient,
    ctx: ToolContext,
    audit: AuditLog,
    budget: Budget = Budget(),
    trace_id: str | None = None,
    max_tokens: int = 1024,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> TriageResult:
    """Run the tool-use loop for one signal. Never raises.

    Setup:
        ``trace_id`` defaults to a fresh ``uuid4().hex``. ``seen`` starts as
        ``{f"signal:{signal.signal_id}": signal.message}``. ``messages`` starts
        as one user ``Message`` with ``render_signal(signal)``.

    Each turn:
        1. **Budget first.** If ``turns == budget.max_turns``, or cumulative
           ``usage.input_tokens >= budget.max_input_tokens``, or cumulative
           ``usage.output_tokens >= budget.max_output_tokens`` -> fallback
           ``"budget_exhausted"``. Checked *before* calling the model, so a
           spent budget never buys one more call.
        2. ``llm.complete(system=SYSTEM_PROMPT, messages=messages,
           tools=TOOL_SPECS, max_tokens=max_tokens)``. Any exception ->
           fallback ``"error"``. Otherwise ``turns += 1``, add ``resp.usage``,
           append the assistant ``Message(text, tool_calls)``, audit
           ``llm_call`` (model, tokens, latency from ``clock``).
        3. ``stop_reason == "refusal"`` -> fallback ``"model_refused"``;
           ``"max_tokens"`` -> fallback ``"max_tokens"``.
        4. No tool calls: the first time, append a user message telling the
           model it must finish with ``submit_brief`` and continue; the second
           time -> fallback ``"no_submission"``.
        5. Each tool call, in order:
           - ``submit_brief``: ``IncidentBrief.model_validate(arguments)`` (a
             ``ValidationError`` becomes violation strings), then
             ``validate_brief(brief, signal=signal, seen=seen)``. Audit a
             ``guardrail`` event (``ok`` = no violations). No violations ->
             audit ``brief`` and return ``"submitted"``. Violations and
             repairs left -> append a ``role="tool"`` message listing them
             (tied to this call's id), count the repair, keep going.
             Violations and no repairs left -> fallback ``"invalid_brief"``
             with the violations recorded on the result.
           - anything else: if ``tool_calls == budget.max_tool_calls`` ->
             fallback ``"budget_exhausted"``. Else ``tool_calls += 1``,
             ``result = dispatch(call, ctx)``, ``seen.update(result.refs)``,
             content = ``fence_untrusted(result.content, label=call.name)``
             when ``result.untrusted`` else ``result.content``, audit
             ``tool_call`` (tool, arguments, refs, ok,
             ``contains_untrusted_text``), append a ``role="tool"`` message
             with that content and ``tool_call_id=call.id``.

    Fallback (every non-submitted exit): ``brief = fallback_brief(signal,
    reason)``; audit one terminal record — event ``"error"`` for
    ``stop_reason == "error"``, else ``"fallback"`` — and return with
    ``fallback_used=True``.

    Audit: one record per model call, per tool call, per brief validation, and
    exactly one terminal record (``brief`` | ``fallback`` | ``error``), all
    sharing ``trace_id``, with ``step`` increasing from 1. A clean
    three-turn run (search_docs, read_task_log, submit_brief) therefore
    writes: llm_call, tool_call, llm_call, tool_call, llm_call, guardrail,
    brief.

    Why the budget check comes before the call: it's the only placement where
    the limit is a limit. Checking after means the run can always spend one
    call past it.

    Why violations go back once instead of failing immediately: most rejected
    briefs are one typo'd ref away from valid, and the messages say exactly
    what to fix. Why only once: a second failure means the model is guessing.
    """
    todo("run_triage: budgeted tool-use loop, fenced results, validated submit_brief, fallback on every failure")
