#!/usr/bin/env python3
"""GitHub PR state watcher plugin for reyn-broker.

Monitors GitHub pull request state (open/merged/closed, CLEAN/DIRTY) for
watched repos and notifies subscribed sessions on changes.

Runs as a broker-managed plugin (auto_start=True recommended).

Usage
-----
Register via broker MCP::

    add_plugin(
        name="github-pr-watcher",
        command="/path/to/.venv/bin/reyn-broker-github-pr",
        session_id="github-pr-watcher",
        auto_start=True,
    )

Commands (send via post_message to "github-pr-watcher")
---------------------------------------------------------
watch:owner/repo        Subscribe caller to all PR events for that repo.
unwatch:owner/repo      Unsubscribe caller from that repo.
list                    List watched repos and their subscribers.

Events delivered to subscribers
---------------------------------
pr_opened: #N 「title」 owner/repo
pr_merged: #N 「title」 owner/repo
pr_closed: #N 「title」 owner/repo
pr_clean: #N PREV→CLEAN owner/repo
pr_dirty: #N CLEAN→NEXT owner/repo

Polling cadence
---------------
The watcher adapts its poll interval to PR activity. While any watched PR is
mid-flight (mergeStateStatus in a pre-CLEAN transient state — BLOCKED/UNKNOWN
by default), it polls at the *fast* interval so the short-lived BLOCKED→CLEAN
edge is observed before the PR is merged away. When everything has settled, it
drops back to the *slow* idle interval. This closes the discrete-sampling miss
where a PR went BLOCKED→CLEAN→merged inside one slow poll interval and the
open+CLEAN state was never sampled (so pr_clean was skipped).

Environment variables
---------------------
PR_WATCH_INTERVAL        Idle poll interval in seconds (default: 300)
PR_WATCH_FAST_INTERVAL   Poll interval while a PR is mid-flight (default: 25)
PR_WATCH_FAST_STATES     Comma-separated mergeStateStatus values treated as
                         mid-flight (default: BLOCKED,UNKNOWN)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field

from plugin_base import BrokerClient, BrokerPlugin, command

logger = logging.getLogger("broker.plugin.github_pr_watcher")

_PR_POLL_S = int(os.environ.get("PR_WATCH_INTERVAL", "300"))
_PR_FAST_S = int(os.environ.get("PR_WATCH_FAST_INTERVAL", "25"))
_FAST_STATES = frozenset(
    s.strip().upper()
    for s in os.environ.get("PR_WATCH_FAST_STATES", "BLOCKED,UNKNOWN").split(",")
    if s.strip()
)


@dataclass
class PRState:
    number: int
    title: str
    merge_state: str  # mergeStateStatus: CLEAN | BLOCKED | DIRTY | UNKNOWN


@dataclass
class WatchedRepo:
    repo: str
    subscribers: set[str] = field(default_factory=set)
    known_prs: dict[int, PRState] = field(default_factory=dict)


def _gh_list_open_prs(repo: str) -> list[dict] | None:
    try:
        result = subprocess.run(
            ["gh", "pr", "list", "--repo", repo, "--state", "open",
             "--json", "number,title,isDraft,mergeStateStatus", "--limit", "100"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning("gh pr list failed for %s: %s", repo, result.stderr.strip())
            return None
        return json.loads(result.stdout)
    except Exception as exc:
        logger.warning("gh pr list error for %s: %s", repo, exc)
        return None


def _gh_pr_detail(repo: str, pr_number: int) -> dict | None:
    try:
        result = subprocess.run(
            ["gh", "pr", "view", str(pr_number), "--repo", repo,
             "--json", "state,mergedAt,title"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except Exception as exc:
        logger.warning("gh pr view #%d error for %s: %s", pr_number, repo, exc)
        return None


class GitHubPRWatcher(BrokerPlugin):
    session_id = "github-pr-watcher"
    role = "GitHub PR state monitor → broker relay"

    def __init__(self) -> None:
        self._watched: dict[str, WatchedRepo] = {}
        self._lock = asyncio.Lock()

    @command(description="Subscribe to PR events for a repo (e.g. watch:tya5/reyn)")
    async def watch(self, repo: str, broker: BrokerClient, sender: str = "") -> str:
        if not repo or "/" not in repo:
            return "usage: watch:<owner/repo>  (e.g. watch:tya5/reyn)"
        prs_raw = await asyncio.to_thread(_gh_list_open_prs, repo)
        initial: dict[int, PRState] = {}
        if prs_raw:
            for pr in prs_raw:
                initial[pr["number"]] = PRState(
                    number=pr["number"],
                    title=pr.get("title", ""),
                    merge_state=pr.get("mergeStateStatus", "UNKNOWN"),
                )
        async with self._lock:
            if repo not in self._watched:
                self._watched[repo] = WatchedRepo(repo=repo, known_prs=initial)
            if sender:
                self._watched[repo].subscribers.add(sender)
            broker.start_poll(self._next_interval_locked())
        pr_count = len(initial)
        logger.info("watch: %s by %s (snapshot: %d open PRs)", repo, sender, pr_count)
        return f"watching {repo} — {pr_count} open PRs snapshotted, poll every {_PR_POLL_S}s"

    @command(description="Unsubscribe from a repo's PR events")
    async def unwatch(self, repo: str, broker: BrokerClient, sender: str = "") -> str:
        async with self._lock:
            if repo in self._watched:
                self._watched[repo].subscribers.discard(sender)
                if not self._watched[repo].subscribers:
                    del self._watched[repo]
            if not self._watched:
                broker.stop_poll()
        return f"unwatched {repo}"

    @command(description="List watched repos and their subscribers")
    async def list(self, broker: BrokerClient) -> str:
        async with self._lock:
            if not self._watched:
                return "no repos watched"
            lines = []
            for repo, w in self._watched.items():
                lines.append(f"{repo}: {sorted(w.subscribers)} ({len(w.known_prs)} open PRs)")
            return "\n".join(lines)

    def _next_interval_locked(self) -> float:
        """Pick the next poll cadence based on PR activity.

        Returns the fast interval while any watched PR sits in a pre-CLEAN
        transient state (CI running / mergeability still computing), so the
        short-lived BLOCKED→CLEAN edge is sampled before the PR merges away.
        Falls back to the slow idle interval once everything has settled.

        Caller MUST hold ``self._lock``.
        """
        for w in self._watched.values():
            for pr in w.known_prs.values():
                if pr.merge_state in _FAST_STATES:
                    return _PR_FAST_S
        return _PR_POLL_S

    async def on_poll(self, broker: BrokerClient) -> None:
        async with self._lock:
            repos_snapshot = list(self._watched.keys())

        for repo in repos_snapshot:
            prs_raw = await asyncio.to_thread(_gh_list_open_prs, repo)
            if prs_raw is None:
                continue

            new_open: dict[int, PRState] = {
                pr["number"]: PRState(
                    number=pr["number"],
                    title=pr.get("title", ""),
                    merge_state=pr.get("mergeStateStatus", "UNKNOWN"),
                )
                for pr in prs_raw
            }

            async with self._lock:
                if repo not in self._watched:
                    continue
                old_open = dict(self._watched[repo].known_prs)
                subscribers = set(self._watched[repo].subscribers)

            events: list[str] = []

            for num, pr in new_open.items():
                if num not in old_open:
                    events.append(f"pr_opened: #{num} 「{pr.title}」 {repo}")

            for num, pr in old_open.items():
                if num not in new_open:
                    detail = await asyncio.to_thread(_gh_pr_detail, repo, num)
                    if detail and detail.get("mergedAt"):
                        events.append(f"pr_merged: #{num} 「{pr.title}」 {repo}")
                    else:
                        events.append(f"pr_closed: #{num} 「{pr.title}」 {repo}")

            for num, pr in new_open.items():
                if num in old_open:
                    old_state = old_open[num].merge_state
                    new_state = pr.merge_state
                    if old_state != new_state:
                        if new_state == "CLEAN":
                            events.append(f"pr_clean: #{num} {old_state}→CLEAN {repo}")
                        elif old_state == "CLEAN":
                            events.append(f"pr_dirty: #{num} CLEAN→{new_state} {repo}")

            async with self._lock:
                if repo in self._watched:
                    self._watched[repo].known_prs = new_open

            for event in events:
                logger.info("%s → %s", event, subscribers)
                for sub in subscribers:
                    await broker.post(to=sub, message=event)

        # Adapt cadence: speed up while any PR is mid-flight, idle otherwise.
        async with self._lock:
            broker.start_poll(self._next_interval_locked())


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        asyncio.run(GitHubPRWatcher().run())
    except KeyboardInterrupt:
        print("[github-pr-watcher] stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
