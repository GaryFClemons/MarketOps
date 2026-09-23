"""The read-only tools the triage agent may call.

Every tool is a function ``(args, ctx, call_id) -> ToolResult``. None of them
writes anything: the agent can look, never touch. The only file the agent's
run writes is the audit log, and the loop writes it, not a tool.

Tool arguments are pydantic models, and the JSON schema the model sees is
generated from the same model that validates the call — one source of truth,
so the schema can't promise something validation rejects. The schema is the
first guardrail: ``tail_lines`` has a maximum, ``k`` has a maximum, and no
tool accepts free-form SQL, paths, or shell.

Errors are data. A bad argument or a missing file comes back to the model as
``ok=False`` with a message written so it can correct itself ("valid tools
are ..."), never as an exception that kills the loop.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from market_ops._scaffold import todo
from market_ops.agent.llm import ToolCall, ToolSpec
from market_ops.agent.schemas import IncidentBrief
from market_ops.config import REPO_ROOT, Settings
from market_ops.retrieval.types import Retriever

# --- context ----------------------------------------------------------------


class RunSource(Protocol):
    """Where DagRun / TaskInstance state comes from."""

    def get_run(self, dag_id: str, run_id: str) -> dict[str, Any] | None: ...

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]: ...


class FixtureRunSource:
    """In-memory run state. Implemented: the test double and the offline demo.

    ``runs`` maps ``(dag_id, run_id)`` to a dict holding the run's fields plus a
    ``"task_instances"`` list.
    """

    def __init__(self, runs: dict[tuple[str, str], dict[str, Any]] | None = None) -> None:
        self._runs = runs or {}

    @classmethod
    def from_json(cls, path: Path) -> FixtureRunSource:
        """Load ``{"runs": [{"dag_id": ..., "run_id": ..., "task_instances": [...], ...}]}``."""
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls({(r["dag_id"], r["run_id"]): r for r in data["runs"]})

    def get_run(self, dag_id: str, run_id: str) -> dict[str, Any] | None:
        run = self._runs.get((dag_id, run_id))
        return None if run is None else {k: v for k, v in run.items() if k != "task_instances"}

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        return list(self._runs.get((dag_id, run_id), {}).get("task_instances", []))


class AirflowApiRunSource:
    """Run state from the Airflow 3 REST API (tier: next).

    Verified in ``airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml``:

    - ``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
    - ``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances``
    - auth: a JWT bearer token from ``POST /auth/token``
      (``auth/managers/simple/routes/login.py`` for the simple auth manager).

    Read-only by construction: this class must only ever issue GETs.
    """

    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token

    def get_run(self, dag_id: str, run_id: str) -> dict[str, Any] | None:
        todo("AirflowApiRunSource.get_run: GET the dagRun endpoint; 404 -> None", tier="next")

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        todo("AirflowApiRunSource.list_task_instances: GET taskInstances for the run", tier="next")


@dataclass
class ToolContext:
    """Everything the tools can reach. Built once per triage run."""

    run_source: RunSource
    retriever: Retriever | None = None
    raw_zone: Path = field(default_factory=lambda: Settings.from_env().raw_zone)
    # Airflow's task logs, host-mounted at ./logs by docker-compose.yaml.
    logs_root: Path = REPO_ROOT / "logs"


@dataclass(frozen=True)
class ToolResult:
    """One tool call's outcome, as the model and the guardrails see it.

    Attributes:
        content: exactly what the model is shown (the loop fences it first
            when ``untrusted``).
        refs: every citable ref this result exposed -> the exact text shown for
            it. The loop merges these into ``seen``, which is what
            ``validate_brief`` checks citations against.
        untrusted: True when the content includes text this system didn't
            write (task logs). The loop fences it and the audit log flags it.
    """

    tool_call_id: str
    ok: bool
    content: str
    refs: dict[str, str] = field(default_factory=dict)
    untrusted: bool = False


# --- argument models: validation and the model-facing schema in one place ----


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GetRunSummaryArgs(_Args):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)


class ReadTaskLogArgs(_Args):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    try_number: int = Field(ge=1, le=50)
    tail_lines: int = Field(default=80, ge=1, le=200)


class SearchDocsArgs(_Args):
    query: str = Field(min_length=1, max_length=300)
    k: int = Field(default=5, ge=1, le=8)


class CheckPartitionArgs(_Args):
    session_date: date


# --- the tools (core) --------------------------------------------------------


def get_run_summary(args: GetRunSummaryArgs, ctx: ToolContext, call_id: str) -> ToolResult:
    """State of one DagRun and its task instances.

    ``content``: compact JSON — the run's ``state``, ``run_type``,
    ``logical_date``, ``data_interval_start``/``end``, ``start_date``/``end_date``
    (whichever are present), and ``task_instances`` as a list of
    ``{task_id, state, try_number, duration}``. Nothing else: no ``conf``, no
    ``note`` — free-text fields are where untrusted content would hide.
    ``refs``: ``{call_id: content}``. ``untrusted``: False.
    Unknown run -> ``ok=False`` with a message naming the dag_id and run_id.
    """
    todo("get_run_summary: whitelisted run + task-instance fields as compact JSON; unknown run -> ok=False")


def read_task_log(args: ReadTaskLogArgs, ctx: ToolContext, call_id: str) -> ToolResult:
    """The last ``tail_lines`` lines of one task attempt's log, numbered.

    Path layout — Airflow 3.3.1's default ``log_filename_template``
    (config_templates/config.yml)::

        {logs_root}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log

    (mapped tasks add ``map_index=N/`` before ``attempt=``; this DAG maps
    nothing, so it's out of scope.)

    Safety — the ids come from the model, so treat them as hostile:
        - ``dag_id``, ``run_id``, ``task_id`` must not contain ``/`` or ``\\``,
          must not be ``.`` or ``..``, and must not be absolute paths.
        - After building the path, ``resolve()`` it and require it to be inside
          ``logs_root.resolve()``.
        - Violations raise ``ValueError`` (``dispatch`` turns that into
          ``ok=False``).

    Windows host gotcha: the log directories are bind-mounted from the Linux
    containers, and on this Windows host ``:`` in a directory name is stored as
    U+F03A (NTFS forbids ``:``). A ``run_id`` like
    ``scheduled__2026-09-18T06:00:00+00:00`` therefore exists on disk as
    ``...T06\\uf03a00\\uf03a00+00\\uf03a00``. Try the literal path first, then
    the variant with every ``:`` replaced by ``\\uf03a``.

    Rendering — Airflow 3 writes task logs as JSON lines (structlog), e.g.
    ``{"timestamp": "...", "level": "error", "event": "Task failed with
    exception", "error_detail": [{"exc_type": "KeyError", "exc_value":
    "'results'", "frames": [...]}], ...}``. Render each returned line as::

        {n} | {timestamp} {LEVEL} {event}

    appending `` | {exc_type}: {exc_value}`` for each ``error_detail`` entry
    (drop ``frames`` — stack frames are the bulk of the bytes and almost none
    of the signal). Lines that aren't JSON objects pass through as
    ``{n} | {line}``. ``n`` is the 1-based line number in the file.

    Returns:
        ``ok=True``; ``content`` = the rendered lines joined by ``\\n``;
        ``ref`` = ``"<path relative to logs_root, POSIX>#L<first n>-L<last n>"``
        (the on-disk relative path, so it resolves as written);
        ``refs = {ref: content}``; ``untrusted=True``.
        Missing file -> ``ok=False`` whose content lists the attempts that do
        exist for that task (``attempt=1.log``, ...), or says none do.
    """
    todo("read_task_log: safe path (+ U+F03A variant), tail N, render JSON lines, ref with line range")


def search_docs(args: SearchDocsArgs, ctx: ToolContext, call_id: str) -> ToolResult:
    """Hybrid search over runbooks, postmortems and the decision log.

    ``content``: one block per hit, in rank order::

        [{rank}] {chunk_id} — {section}
        {excerpt}

    where ``excerpt`` is the chunk text truncated to 800 characters.
    ``refs``: ``{chunk_id: excerpt}`` for every hit — the excerpt, not the full
    chunk, because a brief may only quote what the model was actually shown.
    No hits -> ``ok=True`` with content saying nothing matched. ``untrusted``:
    False (these are our own documents). ``ctx.retriever is None`` ->
    ``ok=False`` explaining search is unavailable.
    """
    todo("search_docs: retriever.search -> numbered blocks; refs map chunk_id -> excerpt shown")


def check_partition(args: CheckPartitionArgs, ctx: ToolContext, call_id: str) -> ToolResult:
    """Inspect ``{raw_zone}/ohlcv/dt={session_date}/ohlcv.parquet``.

    ``content``: compact JSON with ``session_date``, ``exists``, and — when it
    exists — ``rows``, ``tickers`` (distinct count), ``problems`` (from
    ``market_ops.quality.checks.validate_partition_frame``, imported inside
    the function) and ``sha256_12`` (first 12 hex chars of the file's SHA-256:
    the same digest scripts/audit_partitions.py uses to prove idempotency).
    A missing partition is a normal answer (``exists: false``, ``ok=True``) —
    on a weekend or holiday it is the *correct* state.
    ``refs``: ``{call_id: content}``. ``untrusted``: False.
    """
    todo("check_partition: exists / rows / tickers / problems / sha256_12 as JSON; missing is ok=True")


# --- registry and dispatch ---------------------------------------------------

ToolFn = Callable[[Any, ToolContext, str], ToolResult]

TOOLS: dict[str, tuple[type[_Args], ToolFn, str]] = {
    "get_run_summary": (
        GetRunSummaryArgs,
        get_run_summary,
        "State of a DagRun and its task instances (state, try_number, duration). "
        "Use first, to see which task failed and on which try.",
    ),
    "read_task_log": (
        ReadTaskLogArgs,
        read_task_log,
        "The last lines of one task attempt's log, numbered, with exception types. "
        "Use to find the exact error. Cite the returned ref.",
    ),
    "search_docs": (
        SearchDocsArgs,
        search_docs,
        "Search runbooks, incident postmortems and the design decision log. "
        "Include exact identifiers from the log (exception names, status codes). Cite chunk ids.",
    ),
    "check_partition": (
        CheckPartitionArgs,
        check_partition,
        "Inspect the raw-zone partition for one session date: exists, row count, "
        "data-quality problems, content hash.",
    ),
}

SUBMIT_BRIEF = "submit_brief"

TOOL_SPECS: list[ToolSpec] = [
    ToolSpec(name=name, description=desc, input_schema=model.model_json_schema())
    for name, (model, _fn, desc) in TOOLS.items()
] + [
    ToolSpec(
        name=SUBMIT_BRIEF,
        description=(
            "Submit the final incident brief. Call exactly once, last. Every evidence item must cite "
            "a ref returned by a tool (or signal:<signal_id>) and quote its text verbatim."
        ),
        input_schema=IncidentBrief.model_json_schema(),
    )
]


def dispatch(call: ToolCall, ctx: ToolContext) -> ToolResult:
    """Run one tool call. Never raises.

    1. Unknown name (including ``submit_brief``, which the loop handles) ->
       ``ok=False``, content ``"Unknown tool '<name>'. Valid tools: <names>"``
       listing ``TOOLS`` keys.
    2. Validate ``call.arguments`` with the tool's args model. Failure ->
       ``ok=False``, content ``"Invalid arguments for <name>: <field>: <msg>;
       ..."`` — field-level, so the model can fix exactly that field.
    3. Run the tool. Any exception -> ``ok=False``, content
       ``"<name> failed: <ExceptionClass>: <message>"`` — except
       ``market_ops._scaffold.NotBuiltYet``, which must propagate: it marks an
       unwritten function, not a runtime condition, and swallowing it would
       turn "not built" into a confusing test failure.
    4. The returned result carries ``tool_call_id=call.id``.

    Why never raise: the loop's contract is that the model always gets an
    answer to every tool call. An exception here would either crash the run
    (no brief at all) or leave a tool call unanswered, which most provider APIs
    reject outright.
    """
    todo("dispatch: allowlist by name, validate args, run, convert every failure into ok=False text")
