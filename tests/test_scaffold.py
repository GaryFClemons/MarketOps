"""The scaffold's own machinery: todo() and the conftest hook that turns it into a skip.

Tested because the whole backlog's visibility rests on it. If the hook silently
stopped working, unimplemented functions would either fail as noise or — worse —
some future edit could make them pass as vacuous greens.
"""

from __future__ import annotations

import pytest

from market_ops._scaffold import NotBuiltYet, todo

pytest_plugins = ["pytester"]


def test_todo_raises_with_tier_prefix():
    with pytest.raises(NotBuiltYet, match=r"^\[next\] do the thing$"):
        todo("do the thing", tier="next")


def test_not_built_yet_is_a_not_implemented_error():
    # Callers that already handle NotImplementedError keep working.
    assert issubclass(NotBuiltYet, NotImplementedError)


_INNER_TESTS = """
from market_ops._scaffold import todo

def test_stub():
    todo("write me")

def test_plain_not_implemented():
    raise NotImplementedError("real bug")

def test_passes():
    assert True
"""


def test_hook_skips_stubs_and_fails_everything_else(pytester, repo_root):
    pytester.makeconftest((repo_root / "tests" / "conftest.py").read_text())
    pytester.makepyfile(test_inner=_INNER_TESTS)
    result = pytester.runpytest("-rs")
    result.assert_outcomes(passed=1, skipped=1, failed=1)
    result.stdout.fnmatch_lines(["*TODO [[]core[]] write me*"])


def test_todo_fail_flag_restores_failures(pytester, repo_root):
    pytester.makeconftest((repo_root / "tests" / "conftest.py").read_text())
    pytester.makepyfile(test_inner=_INNER_TESTS)
    result = pytester.runpytest("--todo-fail")
    result.assert_outcomes(passed=1, failed=2)
