"""Unit tests for github-pr-watcher adaptive poll cadence and #5265
permanently-blocked relay.

Pure-logic tests for ``GitHubPRWatcher._next_interval_locked`` — no broker
subprocess required. The helper picks the fast interval while any watched PR
is mid-flight (pre-CLEAN) so the short-lived BLOCKED→CLEAN edge is sampled
before the PR merges away, and the slow idle interval once all PRs settle.

The #5265 tests below monkeypatch the module-level ``_gh_list_open_prs``/
``_check_permanently_blocked`` functions (no ``gh``/subprocess/network) and
drive ``on_poll`` against a fake broker that just records posted messages —
the real behavior under test is the once-per-BLOCKED-episode bookkeeping in
``on_poll`` itself, not the detector's own decision logic (already
unit-tested in reyn's own repo, #5574).
"""
from __future__ import annotations

import plugins.github_pr_watcher as pr_watcher
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


# ---------------------------------------------------------------------------
# #5265: permanently-blocked relay — fires once per BLOCKED episode
# ---------------------------------------------------------------------------


class _FakeBroker:
    def __init__(self) -> None:
        self.posted: list[tuple[str, str]] = []

    async def post(self, to: str, message: str) -> None:
        self.posted.append((to, message))

    def start_poll(self, interval: float) -> None:
        pass

    def stop_poll(self) -> None:
        pass


async def _run_polls(monkeypatch, poll_states: list[str], repo: str = "tya5/reyn"):
    """Drive ``on_poll`` once per entry in *poll_states* (all for PR #1),
    with a fake ``_check_permanently_blocked`` that always reports blocked
    and counts its own calls. Returns (broker, check_call_count)."""
    watcher = GitHubPRWatcher()
    watcher._watched[repo] = WatchedRepo(repo=repo, subscribers={"a-session"})
    broker = _FakeBroker()

    states_iter = iter(poll_states)
    monkeypatch.setattr(
        pr_watcher, "_gh_list_open_prs",
        lambda r: [{"number": 1, "title": "t", "mergeStateStatus": next(states_iter)}],
    )
    check_calls = {"n": 0}

    def fake_check(pr_number: int) -> str:
        check_calls["n"] += 1
        return f"RED #5265 — PR #{pr_number} permanently BLOCKED (fake)"

    monkeypatch.setattr(pr_watcher, "_check_permanently_blocked", fake_check)

    for _ in poll_states:
        await watcher.on_poll(broker)
    return broker, check_calls["n"]


async def test_blocked_forever_fires_once_per_episode_then_rearms(monkeypatch):
    broker, check_calls = await _run_polls(
        monkeypatch, ["BLOCKED", "BLOCKED", "CLEAN", "BLOCKED"],
    )
    blocked_events = [m for _, m in broker.posted if m.startswith("pr_blocked_forever:")]
    # poll 1: new BLOCKED episode -> fires. poll 2: same episode -> skipped
    # (no re-check). poll 3: left BLOCKED -> flag resets. poll 4: NEW
    # BLOCKED episode -> fires again.
    assert len(blocked_events) == 2, blocked_events
    assert check_calls == 2, "detector must not be re-invoked while still BLOCKED"


async def test_blocked_forever_never_fires_when_not_blocked(monkeypatch):
    broker, check_calls = await _run_polls(monkeypatch, ["CLEAN", "DIRTY", "CLEAN"])
    assert not [m for _, m in broker.posted if m.startswith("pr_blocked_forever:")]
    assert check_calls == 0, "detector must never be invoked outside BLOCKED"


async def test_blocked_forever_skipped_for_non_reyn_repo(monkeypatch):
    broker, check_calls = await _run_polls(monkeypatch, ["BLOCKED"], repo="other/repo")
    assert not [m for _, m in broker.posted if m.startswith("pr_blocked_forever:")]
    assert check_calls == 0, "the detector is tya5/reyn-only (its own hardcoded target)"


def test_check_permanently_blocked_skips_when_repo_path_unset(monkeypatch):
    # reyn#29 review (lead-coder): no guessed default for REYN_REPO_PATH —
    # unset must skip cleanly (None), the same "skip + log, never guess"
    # discipline this module already applies to non-tya5/reyn repos.
    monkeypatch.setattr(pr_watcher, "_REYN_REPO_PATH", None)
    assert pr_watcher._check_permanently_blocked(1) is None
