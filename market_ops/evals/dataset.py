"""Golden-set schemas and loading. Implemented.

Labels are written at the **section** level — "doc X, heading Y answers this"
— never against chunk ids. Chunk ids change whenever chunk size or overlap
changes; headings don't. So the golden set survives exactly the experiments it
exists to judge.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from market_ops.agent.schemas import RunSignal, Severity

Tag = Literal["identifier", "paraphrase", "why", "procedure", "multi_hop"]


class RelevanceLabel(BaseModel):
    """``heading=None`` means any chunk of the document counts as relevant."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    doc_id: str
    heading: str | None = None


class RetrievalCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    question: str
    relevant: list[RelevanceLabel] = Field(min_length=1)
    tags: list[Tag] = Field(min_length=1)
    notes: str = ""


class BriefCase(BaseModel):
    """What a correct brief for ``signal`` must (and must not) contain."""

    model_config = ConfigDict(extra="forbid")

    id: str
    signal: RunSignal
    expected_severity: Severity
    expected_requires_human: bool
    must_cite_docs: list[str] = Field(default_factory=list)
    must_mention: list[str] = Field(default_factory=list)
    must_not_mention: list[str] = Field(default_factory=list)
    notes: str = ""


M = TypeVar("M", bound=BaseModel)


def load_jsonl(path: Path, model: type[M]) -> list[M]:
    """One model per non-blank line. ``//`` lines are comments.

    Errors name the file and 1-based line, because a golden set is edited by
    hand and "validation error" with no location costs ten minutes each time.
    Duplicate ids are an error: two cases with one id make per-case diffs
    between runs meaningless.
    """
    path = Path(path)
    items: list[M] = []
    seen: set[str] = set()
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        try:
            item = model.model_validate(json.loads(stripped))
        except (json.JSONDecodeError, ValidationError) as exc:
            raise ValueError(f"{path}:{lineno}: {exc}") from exc
        item_id = getattr(item, "id", None)
        if item_id is not None:
            if item_id in seen:
                raise ValueError(f"{path}:{lineno}: duplicate id {item_id!r}")
            seen.add(item_id)
        items.append(item)
    return items
