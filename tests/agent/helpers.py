"""Builders for the agent tests (imported explicitly; not a conftest)."""

from __future__ import annotations

import itertools
import json
from pathlib import Path

from market_ops.agent.llm import LLMResponse, ToolCall, Usage
from market_ops.retrieval.types import Chunk, SearchHit
from tests.factories import DOC_REF, DOC_TEXT

_ids = itertools.count(1)


def call(name: str, **arguments) -> ToolCall:
    return ToolCall(id=f"call_{next(_ids)}", name=name, arguments=arguments)


def turn(*calls: ToolCall, text: str = "", stop: str | None = None, tokens: tuple[int, int] = (100, 20)) -> LLMResponse:
    """One scripted model turn. ``stop`` defaults to tool_use when there are calls."""
    return LLMResponse(
        model="scripted",
        text=text,
        tool_calls=list(calls),
        stop_reason=stop or ("tool_use" if calls else "end_turn"),
        usage=Usage(input_tokens=tokens[0], output_tokens=tokens[1]),
    )


def write_log(logs_root: Path, dag_id: str, run_id: str, task_id: str, attempt: int, lines: list) -> Path:
    """Write a task log the way it appears on this Windows host.

    ``:`` in a run_id is stored as U+F03A on the bind-mounted host directory
    (NTFS forbids ``:``), so write that form everywhere — the tool must map it.
    Dict lines are written as JSON (Airflow 3's structlog format).
    """
    run_dir = f"run_id={run_id}".replace(":", "")
    path = logs_root / f"dag_id={dag_id}" / run_dir / f"task_id={task_id}" / f"attempt={attempt}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(x) if isinstance(x, dict) else x for x in lines) + "\n", encoding="utf-8")
    return path


def log_ref(logs_root: Path, path: Path, first: int, last: int) -> str:
    return f"{path.relative_to(logs_root).as_posix()}#L{first}-L{last}"


class StubRetriever:
    """Always returns the vendor-runbook chunk from tests.factories."""

    def __init__(self, text: str = DOC_TEXT) -> None:
        self.chunk = Chunk(
            chunk_id=DOC_REF, doc_id=DOC_REF.split("::")[0], section="Vendor > Diagnosis > 401 or 403", text=text, ordinal=2
        )
        self.queries: list[str] = []

    def search(self, query: str, k: int = 5) -> list[SearchHit]:
        self.queries.append(query)
        return [SearchHit(chunk=self.chunk, score=1.0, rank=1, retriever="hybrid")]


AUTH_FAILURE_LOG = [
    {"timestamp": "2026-09-18T06:01:57Z", "level": "info", "event": "Filling up the DagBag"},
    {
        "timestamp": "2026-09-18T06:01:58Z",
        "level": "error",
        "event": "Task failed with exception",
        "error_detail": [
            {
                "exc_type": "AirflowFailException",
                "exc_value": "Response code 401; Authentication/Permission Issue",
                "frames": [{"filename": "/opt/airflow/dags/ingest_ohlcv_daily.py", "lineno": 140, "name": "fetch_ohlcv"}],
            }
        ],
    },
    {"timestamp": "2026-09-18T06:01:58Z", "level": "info", "event": "Marking task as FAILED."},
]
