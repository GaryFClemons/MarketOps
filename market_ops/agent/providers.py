"""Real LLM providers behind the neutral ``LLMClient`` interface.

Implement **one** adapter — whichever provider the demo will use. The loop,
tools, guardrails and tests never see provider types; they see ``Message``,
``ToolSpec`` and ``LLMResponse`` (agent/llm.py).

SDKs are imported inside ``__init__`` so the rest of the package, and every
test, works without them installed. Install with ``pip install anthropic`` or
``pip install openai`` (see requirements-dev.txt).
"""

from __future__ import annotations

from collections.abc import Sequence

from market_ops._scaffold import todo
from market_ops.agent.llm import LLMClient, LLMResponse, Message, ToolSpec
from market_ops.config import Settings


def make_client(settings: Settings) -> LLMClient:
    """The client named by ``LLM_PROVIDER`` / ``LLM_MODEL``.

    No default model id: a hardcoded model silently changes behaviour when the
    provider retires it. The model is configuration, and the eval harness is
    what says whether changing it was safe.
    """
    if not settings.llm_model:
        raise ValueError("LLM_MODEL is not set; choose a model id for LLM_PROVIDER in .env")
    if settings.llm_provider == "anthropic":
        return AnthropicClient(model=settings.llm_model, temperature=settings.llm_temperature)
    if settings.llm_provider == "openai":
        return OpenAIClient(model=settings.llm_model, temperature=settings.llm_temperature)
    raise ValueError(f"unknown LLM_PROVIDER {settings.llm_provider!r}; expected 'anthropic' or 'openai'")


def _require(module: str, package: str):
    try:
        return __import__(module)
    except ImportError as exc:
        raise ImportError(f"LLM_PROVIDER needs the {package!r} package: pip install {package}") from exc


class AnthropicClient:
    """Anthropic Messages API adapter.

    Translation contract:
      - ``system`` -> the top-level ``system`` parameter (not a message).
      - ``ToolSpec`` -> ``{"name", "description", "input_schema"}``.
      - An assistant ``Message`` with ``tool_calls`` -> an assistant turn whose
        content has a ``text`` block (if any text) and one ``tool_use`` block
        per call (``id``, ``name``, ``input``).
      - Consecutive ``role="tool"`` messages -> **one** user turn containing a
        ``tool_result`` block per message, keyed by ``tool_use_id``. Every
        ``tool_use`` must be answered in the very next user turn, or the API
        rejects the request.
      - Response -> ``LLMResponse``: text blocks joined into ``text``;
        ``tool_use`` blocks -> ``ToolCall(id, name, arguments=input)``;
        ``stop_reason`` end_turn / tool_use / max_tokens / refusal map to the
        same names (anything else -> "other"); ``usage.input_tokens`` /
        ``usage.output_tokens``.
    """

    def __init__(self, model: str, temperature: float = 0.0) -> None:
        self.model = model
        self.temperature = temperature
        self._sdk = _require("anthropic", "anthropic")

    def complete(
        self, *, system: str, messages: Sequence[Message], tools: Sequence[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        todo("AnthropicClient.complete: neutral messages/tools -> Messages API -> LLMResponse")


class OpenAIClient:
    """OpenAI Chat Completions adapter.

    Translation contract:
      - ``system`` -> a first message with ``role="system"``.
      - ``ToolSpec`` -> ``{"type": "function", "function": {"name",
        "description", "parameters": input_schema}}``.
      - An assistant ``Message`` with ``tool_calls`` -> ``{"role":
        "assistant", "content": ..., "tool_calls": [{"id", "type":
        "function", "function": {"name", "arguments": <JSON string>}}]}``.
      - ``role="tool"`` messages -> ``{"role": "tool", "tool_call_id",
        "content"}``.
      - Response -> ``LLMResponse``: ``message.tool_calls[].function.arguments``
        is a JSON **string** — parse it (a parse failure should become an empty
        dict plus a note, so the loop's validation reports it to the model);
        ``finish_reason`` tool_calls -> "tool_use", stop -> "end_turn",
        length -> "max_tokens", anything else -> "other";
        ``usage.prompt_tokens`` / ``usage.completion_tokens``.
    """

    def __init__(self, model: str, temperature: float = 0.0) -> None:
        self.model = model
        self.temperature = temperature
        self._sdk = _require("openai", "openai")

    def complete(
        self, *, system: str, messages: Sequence[Message], tools: Sequence[ToolSpec], max_tokens: int
    ) -> LLMResponse:
        todo("OpenAIClient.complete: neutral messages/tools -> Chat Completions -> LLMResponse")
