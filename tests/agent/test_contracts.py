"""Implemented contracts and plumbing: schemas, ScriptedLLM, tool specs,
fallback brief, rendering, provider factory. These pass today."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from market_ops.agent.llm import ScriptedLLM
from market_ops.agent.providers import make_client
from market_ops.agent.render import brief_to_markdown
from market_ops.agent.schemas import IncidentBrief, RunSignal, SignalKind
from market_ops.agent.tools import TOOL_SPECS, FixtureRunSource
from market_ops.agent.triage import fallback_brief
from market_ops.config import Settings
from tests.agent.helpers import turn
from tests.factories import make_brief, make_signal


def test_brief_rejects_invented_fields():
    data = make_brief().model_dump()
    data["root_cause_certainty"] = 0.93
    with pytest.raises(ValidationError):
        IncidentBrief.model_validate(data)


def test_brief_requires_evidence():
    with pytest.raises(ValidationError):
        make_brief(evidence=[])


def test_stable_id_is_deterministic_and_discriminating():
    a = RunSignal.stable_id(SignalKind.TASK_FAILED, "d", "r", "t", 1)
    assert a == RunSignal.stable_id(SignalKind.TASK_FAILED, "d", "r", "t", 1)
    assert a != RunSignal.stable_id(SignalKind.TASK_FAILED, "d", "r", "t", 2)


def test_scripted_llm_replays_and_records():
    llm = ScriptedLLM([turn(text="one"), turn(text="two")])
    assert llm.complete(system="s", messages=[], tools=[], max_tokens=10).text == "one"
    assert llm.complete(system="s", messages=[], tools=[], max_tokens=10).text == "two"
    assert len(llm.requests) == 2
    with pytest.raises(AssertionError, match="exhausted"):
        llm.complete(system="s", messages=[], tools=[], max_tokens=10)


def test_tool_specs_are_the_read_only_set_plus_submit():
    names = [t.name for t in TOOL_SPECS]
    assert names == ["get_run_summary", "read_task_log", "search_docs", "check_partition", "submit_brief"]
    specs = {t.name: t.input_schema for t in TOOL_SPECS}
    assert specs["read_task_log"]["properties"]["tail_lines"]["maximum"] == 200
    assert specs["search_docs"]["properties"]["k"]["maximum"] == 8
    assert specs["read_task_log"]["additionalProperties"] is False
    assert specs["submit_brief"] == IncidentBrief.model_json_schema()


@pytest.mark.parametrize(
    ("kind", "severity"),
    [
        (SignalKind.TASK_FAILED, "high"),
        (SignalKind.DEADLINE_MISSED, "high"),
        (SignalKind.PARTITION_MISSING, "high"),
        (SignalKind.CHECK_WARNING, "medium"),
    ],
)
def test_fallback_brief_is_safe(kind, severity):
    signal = make_signal(kind=kind)
    brief = fallback_brief(signal, "budget_exhausted")
    assert brief.severity == severity
    assert brief.requires_human is True
    assert brief.cause_confidence == "unknown"
    assert brief.evidence[0].ref == f"signal:{signal.signal_id}"
    assert "budget_exhausted" in brief.summary


def test_fallback_brief_with_empty_message_still_validates():
    brief = fallback_brief(make_signal(message=""), "error")
    assert brief.evidence[0].quote


def test_markdown_marks_actions_needing_approval():
    md = brief_to_markdown(make_brief())
    assert md.startswith("# fetch_ohlcv rejected by vendor")
    assert "Requires human action" in md
    assert "Rotate the API key in Connection polygon_default **(needs approval)**" in md
    assert "airflow connections get polygon_default\n" in md  # investigate: no approval marker


def test_make_client_refuses_to_guess():
    with pytest.raises(ValueError, match="LLM_MODEL"):
        make_client(Settings.from_env({"LLM_PROVIDER": "anthropic"}))
    with pytest.raises(ValueError, match="LLM_PROVIDER"):
        make_client(Settings.from_env({"LLM_PROVIDER": "acme", "LLM_MODEL": "x"}))


def test_fixture_run_source(tmp_path):
    path = tmp_path / "runs.json"
    path.write_text(
        '{"runs": [{"dag_id": "d", "run_id": "r", "state": "failed", '
        '"task_instances": [{"task_id": "t", "state": "failed", "try_number": 1}]}]}',
        encoding="utf-8",
    )
    src = FixtureRunSource.from_json(path)
    assert src.get_run("d", "r") == {"dag_id": "d", "run_id": "r", "state": "failed"}
    assert src.list_task_instances("d", "r")[0]["task_id"] == "t"
    assert src.get_run("d", "missing") is None
