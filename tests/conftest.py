"""Root pytest configuration: the scaffold TODO hook and the live-API gate.

Area-specific fixtures live in each area's own conftest.py (tests/quality/,
tests/agent/, ...). Keep this file to cross-cutting behaviour only.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from market_ops._scaffold import NotBuiltYet

REPO_ROOT = Path(__file__).resolve().parents[1]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--todo-fail",
        action="store_true",
        default=False,
        help="Report scaffolded (NotBuiltYet) tests as failures instead of skips.",
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Turn a scaffold stub into a visible TODO skip rather than a failure.

    Tests are written against the spec before the implementation exists. Without
    this hook the suite would be a wall of red that hides real regressions; with
    it, an unimplemented function reads as ``SKIPPED ... TODO [core] <what done
    means>`` and the moment the body is written the same test starts running for
    real. Only ``NotBuiltYet`` is intercepted — any other exception, including a
    plain ``NotImplementedError``, still fails.
    """
    outcome = yield
    report = outcome.get_result()
    if call.excinfo is None or not call.excinfo.errisinstance(NotBuiltYet):
        return
    if item.config.getoption("--todo-fail"):
        return
    report.outcome = "skipped"
    # (path, 1-based line, reason) is the tuple pytest itself uses for skips,
    # so the terminal summary formats these exactly like any other skip.
    report.longrepr = (str(item.path), item.location[1] + 1, f"TODO {call.excinfo.value}")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip ``@pytest.mark.live`` tests unless explicitly enabled.

    Live tests cost money and depend on a network and an API key; they should
    never run because someone typed ``pytest``.
    """
    if os.getenv("MARKETOPS_LIVE") == "1":
        return
    skip_live = pytest.mark.skip(reason="live API test; set MARKETOPS_LIVE=1 to run")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT
