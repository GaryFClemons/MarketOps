"""Provider-neutral LLM interface for the triage agent.

The agent loop talks to ``LLMClient`` and nothing else. Provider adapters
(``agent/providers.py``) translate these neutral types to and from a vendor SDK.
Two reasons to pay for that indirection in a project this size:

1. **Testability without a network.** ``ScriptedLLM`` replays canned responses,
   so the loop, the guardrails, and the audit trail are all exercised in pytest
   deterministically and for free. The live model is tested separately, and
   deliberately (``@pytest.mark.live``).
2. **The vendor is a swappable dependency, like Polygon was.** Swapping provider
   or model is a config change plus one adapter, and the eval harness is what
   says whether the swap was safe.

The shapes are a lowest common denominator of the major tool-use APIs:
assistant turns may carry ``tool_calls``; each tool result goes back as its own
``role="tool"`` message tied by ``tool_call_id``. Adapters handle the rest —
e.g. one provider nests tool results inside a user turn, another gives them a
dedicated role.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field


class ToolSpec(BaseModel):
    """A tool the model may call. ``input_schema`` is JSON Schema."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,63}$")
    description: str
    input_schema: dict[str, Any]


class ToolCall(BaseModel):
    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
        )


class Message(BaseModel):
    role: Literal["user", "assistant", "tool"]
    content: str = ""
    tool_calls: list[ToolCall] = Field(default_factory=list)
    tool_call_id: str | None = None


StopReason = Literal["end_turn", "tool_use", "max_tokens", "refusal", "other"]


class LLMResponse(BaseModel):
    model: str
    text: str = ""
    tool_calls: list[ToolCall] = Field(default_factory=list)
    stop_reason: StopReason
    usage: Usage = Field(default_factory=Usage)


class LLMClient(Protocol):
    """Anything that can take one turn of a tool-use conversation."""

    model: str

    def complete(
        self,
        *,
        system: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec],
        max_tokens: int,
    ) -> LLMResponse: ...


class ScriptedLLM:
    """Deterministic test double: replays a fixed list of responses, records every request.

    Running out of responses raises instead of returning something plausible —
    a loop that asks for more turns than the script expected is a bug the test
    should surface, not paper over.
    """

    def __init__(self, responses: Iterable[LLMResponse], model: str = "scripted") -> None:
        self.model = model
        self._responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def complete(
        self,
        *,
        system: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec],
        max_tokens: int,
    ) -> LLMResponse:
        self.requests.append(
            {
                "system": system,
                "messages": [m.model_copy(deep=True) for m in messages],
                "tools": [t.name for t in tools],
                "max_tokens": max_tokens,
            }
        )
        if not self._responses:
            raise AssertionError(
                f"ScriptedLLM exhausted after {len(self.requests) - 1} responses; "
                "the loop asked for more turns than the test scripted"
            )
        return self._responses.pop(0)
