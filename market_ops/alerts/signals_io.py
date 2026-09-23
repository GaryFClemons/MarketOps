"""Read and write RunSignal files. Plumbing, implemented.

One file per signal, named by ``signal_id``. Because the id is derived from the
event (see ``RunSignal.stable_id``), a callback that fires twice for the same
event overwrites one file instead of creating two signals — the same
overwrite-based idempotency the raw zone uses.
"""

from __future__ import annotations

from pathlib import Path

from market_ops.agent.schemas import RunSignal


def write_signal(signal: RunSignal, signals_dir: Path) -> Path:
    """Atomically write ``signal`` to ``signals_dir/<signal_id>.json``.

    Write-then-rename, as in the DAG's parquet write: the agent polling this
    directory sees either no file or a complete one, never a half-written JSON.
    """
    signals_dir = Path(signals_dir)
    signals_dir.mkdir(parents=True, exist_ok=True)
    target = signals_dir / f"{signal.signal_id}.json"
    staging = target.with_suffix(".json.tmp")
    staging.write_text(signal.model_dump_json(indent=2), encoding="utf-8")
    staging.replace(target)
    return target


def read_signals(signals_dir: Path) -> list[RunSignal]:
    """All signals in ``signals_dir``, oldest first. Missing directory -> ``[]``.

    ``*.tmp`` files are in-flight writes and are skipped. Sorting by
    ``(detected_at, signal_id)`` makes ``--latest`` deterministic even when two
    signals share a timestamp.
    """
    signals_dir = Path(signals_dir)
    if not signals_dir.is_dir():
        return []
    signals = [
        RunSignal.model_validate_json(p.read_text(encoding="utf-8"))
        for p in signals_dir.glob("*.json")
    ]
    return sorted(signals, key=lambda s: (s.detected_at, s.signal_id))
