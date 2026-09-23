"""LLM-as-judge for what deterministic checks can't see. Not built (tier: next).

Known failure modes, and what the design does about each:

- **Self-preference.** A model grades its own family's output generously. Use a
  different, stronger model than the generator (``EVAL_JUDGE_MODEL``).
- **Verbosity bias.** Longer answers score higher. The rubric anchors on
  faithfulness to cited evidence, not thoroughness.
- **Position bias** in pairwise comparisons. Score briefs one at a time; when
  comparing two, run both orders.
- **Unvalidated judges.** Before trusting a single score, grade ~20 briefs by
  hand and check the judge agrees. A judge nobody calibrated is a random
  number generator with good prose.
"""

from __future__ import annotations

from collections.abc import Mapping

from market_ops._scaffold import todo
from market_ops.agent.llm import LLMClient
from market_ops.agent.schemas import IncidentBrief

# A draft rubric — tune it against hand-graded briefs before relying on it.
JUDGE_RUBRIC = """\
You are grading an incident brief written for an on-call data engineer.
You are given the brief and the exact evidence text it cites.

Score each dimension from 1 to 5.

faithfulness — Is every claim in the brief supported by the cited evidence?
  5 = every claim traceable to a quote; 3 = minor unsupported detail;
  1 = a central claim contradicts or is absent from the evidence.
actionability — Could an engineer act on the recommended actions at 3am?
  5 = specific, ordered, names the exact object (task, partition, connection);
  3 = right direction, vague; 1 = generic advice ("check the logs").
severity_calibration — Does the severity match the harm described?
  Wrong-but-published data outranks missing data, which outranks degraded.
  5 = exactly right; 3 = off by one level; 1 = off by two or more.

Return only JSON:
{"faithfulness": int, "actionability": int, "severity_calibration": int, "rationale": str}
"""


def judge_brief(brief: IncidentBrief, evidence_texts: Mapping[str, str], *, llm: LLMClient) -> dict:
    """Score one brief against the evidence it cites, using ``JUDGE_RUBRIC``.

    ``evidence_texts`` maps each evidence ``ref`` to the text the agent was
    shown (from the audit trail). Returns the parsed JSON scores; a response
    that isn't valid JSON is retried once, then recorded as a judge failure —
    never silently as a low score.
    """
    todo("judge_brief: render rubric + brief + evidence, call the judge model, parse JSON scores", tier="next")
