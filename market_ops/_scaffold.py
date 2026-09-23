"""Markers for code that is deliberately scaffolded but not yet written.

This repo is built in the open as evidence of work, so the line between "done" and
"planned" has to be visible in the code itself, not only in ROADMAP.md. Every
function whose body is left for the owner to write calls ``todo()`` instead.

Why a dedicated exception rather than a bare ``NotImplementedError``:

- ``tests/conftest.py`` turns ``NotBuiltYet`` into a *skip* whose reason is the
  TODO text. ``pytest`` stays green while the backlog stays visible —
  ``pytest -rs`` lists every open item, and ``pytest --todo-fail`` shows the same
  items as red failures when you want the pressure. A genuine
  ``NotImplementedError`` (an abstract method, a library) still fails loudly,
  because only this subclass is intercepted.
- ``grep -rn "todo(" market_ops`` is the entire backlog, in code order.

Tiers, so the backlog can be read by priority:

- ``core``  — the owner writes this before it can be demoed or defended.
- ``next``  — makes a core piece better (real embeddings, reranking, a judge).
- ``later`` — rounds out the platform (dbt marts, SCD2, the EXPLAIN write-up).
"""

from __future__ import annotations

from typing import Literal, NoReturn

Tier = Literal["core", "next", "later"]


class NotBuiltYet(NotImplementedError):
    """Raised by a scaffolded function whose implementation is intentionally pending."""


def todo(what: str, *, tier: Tier = "core") -> NoReturn:
    """Mark a function body as pending.

    ``what`` should state what "done" means, concretely enough that the test
    spec and this sentence agree — it is printed verbatim as the skip reason.
    """
    raise NotBuiltYet(f"[{tier}] {what}")
