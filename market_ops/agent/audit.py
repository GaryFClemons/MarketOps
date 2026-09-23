"""Append-only JSONL record of every step the triage agent takes.

The chat transcript is the model's account of what it did. This is the
system's account of what happened: which tools ran with which arguments, what
they returned (as refs and sizes), how many tokens each call cost, how long it
took, and whether untrusted text entered the context. When someone asks where
a claim in a brief came from, this file is the answer.

What is deliberately **not** recorded: API keys, full prompts, and full tool
outputs. Ids, refs, counts and short details are enough to reconstruct a run
against the same inputs without turning the audit log into a second copy of
every log file it read.

It is also the future eval set: after a few weeks it holds the signals that
actually happened and what the agent did with them.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from market_ops._scaffold import todo

AuditEvent = Literal["llm_call", "tool_call", "guardrail", "brief", "fallback", "error"]


class AuditRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts: datetime
    trace_id: str
    signal_id: str
    step: int
    event: AuditEvent
    model: str | None = None
    tool: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    refs: list[str] = Field(default_factory=list)
    ok: bool = True
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    contains_untrusted_text: bool = False
    detail: str = Field(default="", max_length=500)


class AuditLog:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def append(self, record: AuditRecord) -> None:
        """Append ``record`` as one JSON line.

        - Create parent directories if needed.
        - Open in **append** mode, write ``record.model_dump_json()`` + ``"\\n"``,
          flush.
        - Never rewrite, reorder or truncate existing lines.

        Why append-only is the whole point: an audit trail that can be edited
        in place is not evidence. Appending a line is also atomic enough for a
        single writer, with no read-modify-write window.
        """
        todo("AuditLog.append: one JSON line per record, append mode, create parent dirs, never rewrite")

    def read(self, trace_id: str | None = None) -> list[AuditRecord]:
        """Every record in file order, optionally only one trace. Missing file -> ``[]``."""
        if not self.path.exists():
            return []
        records = [
            AuditRecord.model_validate_json(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return [r for r in records if trace_id is None or r.trace_id == trace_id]
