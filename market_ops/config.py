"""Runtime settings, read from the environment once.

Two execution contexts share this package, and they disagree about paths:

- **Inside the Airflow containers** (quality/ and alerts/ only), ``.env`` is
  injected via compose and paths are container paths — ``RAW_ZONE_PATH`` is
  ``/opt/airflow/data/raw``. The package is mounted at ``/opt/airflow/src``,
  so ``REPO_ROOT`` below is *not* the repo there, and nothing in-container
  should depend on it.
- **On the host** (the agent CLI, evals, tests), ``.env`` is not loaded and the
  defaults resolve relative to the repo root, where ``data/`` is the same
  host-mounted directory the containers write into.

Settings are a frozen dataclass rather than module-level globals so a test can
build one with ``Settings.from_env({...})`` and never mutate process state.
Secrets (API keys) are deliberately *not* fields here: provider SDKs read their
own key variables, which keeps keys out of reprs, logs, and audit records.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    raw_zone: Path
    ops_dir: Path
    llm_provider: str
    llm_model: str
    embedding_model: str
    llm_max_tokens: int
    llm_temperature: float
    eval_judge_model: str

    @property
    def signals_dir(self) -> Path:
        """Where alert callbacks drop RunSignal JSON files for the agent to pick up."""
        return self.ops_dir / "signals"

    @property
    def audit_log(self) -> Path:
        """Append-only JSONL trail of every triage step. The system's account, not the model's."""
        return self.ops_dir / "audit" / "triage.jsonl"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        return cls(
            raw_zone=Path(env.get("RAW_ZONE_PATH", REPO_ROOT / "data" / "raw")),
            ops_dir=Path(env.get("OPS_DIR", REPO_ROOT / "data" / "ops")),
            # Default mirrors .env.example so host and container agree when unset.
            llm_provider=env.get("LLM_PROVIDER", "openai"),
            llm_model=env.get("LLM_MODEL", ""),
            embedding_model=env.get("EMBEDDING_MODEL", ""),
            llm_max_tokens=int(env.get("LLM_MAX_TOKENS", "1024")),
            # Triage is a reporting task, not a creative one: the same signal and
            # the same evidence should produce the same brief. Temperature 0 is
            # what makes a regression eval meaningful run over run.
            llm_temperature=float(env.get("LLM_TEMPERATURE", "0")),
            eval_judge_model=env.get("EVAL_JUDGE_MODEL", ""),
        )
