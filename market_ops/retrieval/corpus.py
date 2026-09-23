"""Load the retrieval corpus: this repo's own operational documents.

Implemented plumbing. What gets indexed is a deliberate list, not "every
markdown file": the agent should retrieve operational knowledge (runbooks,
postmortems, the decision log), not the project's backlog or its eval results.
"""

from __future__ import annotations

import re
from pathlib import Path

from market_ops.config import REPO_ROOT
from market_ops.retrieval.types import Document, SourceType

# Ordered: when two globs match one file, the first wins — so a runbook is typed
# "runbook", not the generic "doc" that docs/*.md would give it.
DEFAULT_SOURCES: tuple[tuple[str, SourceType], ...] = (
    ("docs/runbooks/*.md", "runbook"),
    ("docs/incidents/*.md", "incident"),
    ("ROADMAP.md", "decision_log"),
    ("codebase-map.md", "codebase_map"),
    ("docs/*.md", "doc"),
)

# build-order.md is the backlog, not operational knowledge. eval_results.md is
# excluded for a sharper reason: indexing the eval's own output would let
# retrieval "find" answers that exist only because the eval was run.
EXCLUDED = frozenset({"docs/build-order.md", "docs/eval_results.md"})

_FRONT_MATTER = re.compile(r"\A---\r?\n.*?\r?\n---\r?\n", re.DOTALL)
_H1 = re.compile(r"^# (.+?)\s*$", re.MULTILINE)


def load_corpus(
    root: Path = REPO_ROOT,
    sources: tuple[tuple[str, SourceType], ...] = DEFAULT_SOURCES,
) -> list[Document]:
    """Every source file as a ``Document``, sorted by ``doc_id`` for determinism.

    YAML front-matter (the ``status`` / ``last_updated`` block) is stripped: it is
    metadata about the file, and "next_action: ..." lines would otherwise match
    queries they have nothing to do with.
    """
    root = Path(root)
    seen: dict[str, Document] = {}
    for pattern, source_type in sources:
        for path in sorted(root.glob(pattern)):
            doc_id = path.relative_to(root).as_posix()
            if doc_id in seen or doc_id in EXCLUDED or not path.is_file():
                continue
            text = _FRONT_MATTER.sub("", path.read_text(encoding="utf-8"), count=1)
            h1 = _H1.search(text)
            seen[doc_id] = Document(
                doc_id=doc_id,
                title=h1.group(1) if h1 else path.stem,
                text=text,
                source_type=source_type,
            )
    return [seen[k] for k in sorted(seen)]
