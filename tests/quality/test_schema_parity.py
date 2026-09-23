"""OHLCV_COLUMNS exists in two places until the DAG imports it from market_ops.

Parsed with ``ast`` rather than importing the DAG, so this runs without Airflow
and without executing DAG-file side effects. Delete this test once the DAG
imports the constant — then there is only one copy.
"""

from __future__ import annotations

import ast

from market_ops.quality.checks import OHLCV_COLUMNS


def _dag_constant(source: str, name: str) -> list[str]:
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not assigned in the DAG file")


def test_ohlcv_columns_match_the_dag(repo_root):
    source = (repo_root / "dags" / "ingest_ohlcv_daily.py").read_text(encoding="utf-8")
    assert OHLCV_COLUMNS == _dag_constant(source, "OHLCV_COLUMNS")
