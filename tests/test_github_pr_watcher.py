"""Unit tests for github-pr-watcher adaptive poll cadence.

Pure-logic tests for ``GitHubPRWatcher._next_interval_locked`` — no broker
subprocess required. The helper picks the fast interval while any watched PR
is mid-flight (pre-CLEAN) so the short-lived BLOCKED→CLEAN edge is sampled
before the PR merges away, and the slow idle interval once all PRs settle.
"""
from __future__ import annotations

from plugins.github_pr_watcher import (
    _PR_FAST_S,
    _PR_POLL_S,
    GitHubPRWatcher,
    PRState,
    WatchedRepo,
)


def _watcher_with(*states: str) -> GitHubPRWatcher:
    w = GitHubPRWatcher()
    repo = WatchedRepo(repo="o/r")
    for i, st in enumerate(states):
        repo.known_prs[i] = PRState(number=i, title="t", merge_state=st)
    w._watched["o/r"] = repo
    return w


def test_idle_interval_when_no_repos_watched():
    assert GitHubPRWatcher()._next_interval_locked() == _PR_POLL_S


def test_idle_interval_when_all_prs_clean():
    assert _watcher_with("CLEAN", "CLEAN")._next_interval_locked() == _PR_POLL_S


def test_fast_interval_when_a_pr_is_blocked():
    assert _watcher_with("CLEAN", "BLOCKED")._next_interval_locked() == _PR_FAST_S


def test_fast_interval_when_a_pr_is_unknown():
    assert _watcher_with("UNKNOWN")._next_interval_locked() == _PR_FAST_S


def test_fast_beats_slow_with_mixed_states():
    # One mid-flight PR is enough to keep the whole watcher in fast mode.
    assert _watcher_with("CLEAN", "DIRTY", "BLOCKED")._next_interval_locked() == _PR_FAST_S
