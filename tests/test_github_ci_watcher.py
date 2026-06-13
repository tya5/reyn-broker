"""Unit tests for github-ci-watcher.

Pure-logic tests that don't need a broker subprocess.
"""
from __future__ import annotations

from plugins.github_ci_watcher import (
    _SKIP_STATUSES,
    CiWatcherPlugin,
    WatchedPR,
    WatchedRepo,
)


def _plugin() -> CiWatcherPlugin:
    return CiWatcherPlugin()


def test_command_schema_includes_repo_commands():
    p = _plugin()
    names = {c["name"] for c in p.commands()}
    assert "watch-repo" in names
    assert "unwatch-repo" in names
    assert "watch" in names
    assert "unwatch" in names
    assert "list" in names


def test_watch_repo_not_in_args_schema():
    # sender must NOT appear in args (it's injected, not caller-supplied)
    p = _plugin()
    for cmd in p.commands():
        assert "sender" not in cmd["args"], f"sender leaked into args for {cmd['name']}"


def test_skip_statuses_covers_non_terminal():
    # pending / no-checks / unknown must not trigger relay events
    assert "pending" in _SKIP_STATUSES
    assert "no-checks" in _SKIP_STATUSES
    assert "unknown" in _SKIP_STATUSES
    assert "success" not in _SKIP_STATUSES
    assert "failure" not in _SKIP_STATUSES


def test_watched_pr_requester_stored_per_session():
    # WatchedPR stores multiple independent requesters
    w = WatchedPR(pr_number="42")
    w.requesters.add("session-a")
    w.requesters.add("session-b")
    assert "session-a" in w.requesters
    assert "session-b" in w.requesters


def test_watched_repo_pr_last_status_tracks_per_pr():
    w = WatchedRepo(repo="o/r")
    w.pr_last_status["10"] = "success"
    w.pr_last_status["11"] = "failure"
    assert w.pr_last_status["10"] == "success"
    assert w.pr_last_status["11"] == "failure"


def test_stale_pr_cleanup_logic():
    # Simulate what _poll_watched_repos does when a PR closes: its key is deleted.
    w = WatchedRepo(repo="o/r")
    w.pr_last_status["10"] = "success"
    w.pr_last_status["11"] = "failure"
    open_numbers = {"11"}  # #10 has closed
    stale = set(w.pr_last_status) - open_numbers
    for n in stale:
        del w.pr_last_status[n]
    assert "10" not in w.pr_last_status
    assert "11" in w.pr_last_status
