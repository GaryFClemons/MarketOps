"""The triage agent: RunSignal in, IncidentBrief out, every step audited.

    schemas.py     the contracts (done)
    llm.py         provider-neutral client protocol + ScriptedLLM test double (done)
    providers.py   real provider adapters
    tools.py       read-only tools the model may call
    guardrails.py  checks a brief must pass before anyone sees it
    audit.py       append-only JSONL record of every step
    triage.py      the loop that ties them together
    __main__.py    ``python -m market_ops.agent`` CLI

The agent is read-only by construction: no tool it can call writes anything
except the audit log, and the audit log is written by the loop, not by a tool.
"""
