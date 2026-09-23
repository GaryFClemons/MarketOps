"""Token cost per call — a first-class metric next to quality and latency.

Prices are deliberately not filled in: they change, and a stale number in code
is worse than none. Copy current USD-per-million-token prices from the
provider's pricing page for the exact model ids in use, and note the date.
"""

from __future__ import annotations

from market_ops._scaffold import todo
from market_ops.agent.llm import Usage

# model id -> (input USD per 1M tokens, output USD per 1M tokens). Owner fills in.
PRICING: dict[str, tuple[float, float]] = {}


def cost_usd(usage: Usage, model: str) -> float | None:
    """Dollar cost of one call, or ``None`` for a model not in ``PRICING``.

    ``None``, not ``0.0``: an unknown cost must never read as free in a report.
    """
    todo("cost_usd: tokens x per-million price for input and output; None for unknown models", tier="next")
