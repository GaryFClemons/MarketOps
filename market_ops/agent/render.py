"""Render an IncidentBrief for a human. Implemented."""

from __future__ import annotations

from market_ops.agent.schemas import MUTATING_ACTIONS, IncidentBrief


def brief_to_markdown(brief: IncidentBrief) -> str:
    lines = [f"# {brief.title}", "", f"**Severity:** {brief.severity.value}  ", f"**Signal:** `{brief.signal_id}`"]
    if brief.requires_human:
        lines += ["", "> **Requires human action** before anything is rerun, backfilled, or reconfigured."]
    lines += [
        "",
        "## Summary",
        brief.summary,
        "",
        f"## Probable cause ({brief.cause_confidence})",
        brief.probable_cause,
        "",
        "## Evidence",
    ]
    lines += [f"- [{e.source.value}] `{e.ref}` — \"{e.quote}\"" for e in brief.evidence]
    lines += ["", "## Recommended actions"]
    for i, action in enumerate(brief.recommended_actions, start=1):
        gate = " **(needs approval)**" if action.kind in MUTATING_ACTIONS else ""
        lines.append(f"{i}. [{action.kind.value}] {action.step}{gate}")
    return "\n".join(lines) + "\n"
