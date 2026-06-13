"""GitHub Actions CI status watcher plugin for reyn-broker.

Monitors GitHub Pull Request check results (GitHub Actions, required status
checks, etc.) via the ``gh`` CLI and relays CI completion events as broker
messages to requesting sessions.

Two subscription modes
----------------------
**Per-PR** (original): send ``watch:#N`` to watch a single PR by number.
  Events: ``✅ CI #N: SUCCESS`` / ``❌ CI #N: FAILURE``

**Repo-level** (recommended): send ``watch-repo:owner/repo`` to subscribe to
  CI completion events for *all* open PRs in that repo.
  Events: ``✅ ci_result: #N success owner/repo``
          ``❌ ci_result: #N failure owner/repo``

  This is the preferred mode. It eliminates the need for per-PR subscription
  and ensures that CI failures on already-BLOCKED PRs (which do not produce a
  pr_clean/pr_dirty edge from github-pr-watcher) are still reported.

Usage
-----
Register via broker MCP::

    add_plugin(
        name="github-ci",
        command="/path/to/.venv/bin/reyn-broker-github-ci",
        session_id="ci-watcher",
        auto_start=True,
    )

Commands (send via post_message to "ci-watcher")
-------------------------------------------------
watch-repo:owner/repo   Subscribe to CI completion events for all open PRs.
unwatch-repo:owner/repo Unsubscribe from repo CI events.
watch:#N                Watch a single PR's CI checks.
unwatch:#N              Stop watching a single PR.
list                    List watched PRs and repos.

Environment variables
---------------------
BROKER_URL    Broker MCP URL (default: http://127.0.0.1:8765/mcp).
CI_POLL_S     Seconds between CI poll cycles (default: 60).
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

logger = logging.getLogger("broker.plugin.ci_watcher")

_CI_POLL_S = int(os.environ.get("CI_POLL_S", "60"))

# Statuses that indicate CI is still running or has no meaningful result yet.
_SKIP_STATUSES = frozenset({"pending", "no-checks", "unknown"})


@dataclass
class WatchedPR:
    pr_number: str
    requesters: set[str] = field(default_factory=set)
    last_status: str | None = None


@dataclass
class WatchedRepo:
    repo: str
    subscribers: set[str] = field(default_factory=set)
    # pr_number (str) → last emitted terminal status
    pr_last_status: dict[str, str] = field(default_factory=dict)


def _gh_pr_status(pr_number: str, repo: str | None = None) -> str | None:
    """Return overall CI status for a PR via ``gh`` CLI. Returns None on error."""
    cmd = ["gh", "pr", "checks", pr_number, "--json", "name,status,conclusion"]
    if repo:
        cmd += ["--repo", repo]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return None
        checks = json.loads(result.stdout)
        if not checks:
            return "no-checks"
        statuses = {c.get("status") for c in checks}
        if "IN_PROGRESS" in statuses or "QUEUED" in statuses or "WAITING" in statuses:
            return "pending"
        conclusions = {c.get("conclusion") for c in checks}
        if "FAILURE" in conclusions or "TIMED_OUT" in conclusions or "CANCELLED" in conclusions:
            return "failure"
        if conclusions <= {"SUCCESS", "NEUTRAL", "SKIPPED", None}:
            return "success"
        return "unknown"
    except Exception as exc:
        logger.warning("gh pr checks failed for #%s: %s", pr_number, exc)
        return None


def _gh_list_open_pr_numbers(repo: str) -> list[str] | None:
    try:
        result = subprocess.run(
            ["gh", "pr", "list", "--repo", repo, "--state", "open",
             "--json", "number", "--limit", "100"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning("gh pr list failed for %s: %s", repo, result.stderr.strip())
            return None
        return [str(pr["number"]) for pr in json.loads(result.stdout)]
    except Exception as exc:
        logger.warning("gh pr list error for %s: %s", repo, exc)
        return None


class CiWatcherPlugin(BrokerPlugin):
    session_id = "ci-watcher"
    role = "GitHub CI poll → broker relay"
    # poll_interval=None: polling starts only when something is watched

    def __init__(self) -> None:
        self._watched: dict[str, WatchedPR] = {}
        self._watched_repos: dict[str, WatchedRepo] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Per-PR commands (original mode)
    # ------------------------------------------------------------------

    @command(description="Watch a specific PR's CI checks (e.g. watch:#1268)")
    async def watch(self, pr_number: str, broker: BrokerClient, sender: str = "") -> str:
        async with self._lock:
            if pr_number not in self._watched:
                self._watched[pr_number] = WatchedPR(pr_number=pr_number)
            if sender:
                self._watched[pr_number].requesters.add(sender)
            broker.start_poll(_CI_POLL_S)
        logger.info("watching PR %s for %s", pr_number, sender)
        return f"watching PR {pr_number} (poll every {_CI_POLL_S}s)"

    @command(description="Stop watching a specific PR's CI checks")
    async def unwatch(self, pr_number: str, broker: BrokerClient, sender: str = "") -> str:
        async with self._lock:
            if pr_number in self._watched:
                self._watched[pr_number].requesters.discard(sender)
                if not self._watched[pr_number].requesters:
                    del self._watched[pr_number]
            if not self._watched and not self._watched_repos:
                broker.stop_poll()
        return f"unwatched PR {pr_number}"

    # ------------------------------------------------------------------
    # Repo-level commands (recommended mode)
    # ------------------------------------------------------------------

    @command(
        description="Subscribe to CI completion events for all open PRs in a repo "
                    "(e.g. watch-repo:tya5/reyn)",
        name="watch-repo",
    )
    async def watch_repo(self, repo: str, broker: BrokerClient, sender: str = "") -> str:
        if not repo or "/" not in repo:
            return "usage: watch-repo:<owner/repo>  (e.g. watch-repo:tya5/reyn)"
        async with self._lock:
            if repo not in self._watched_repos:
                self._watched_repos[repo] = WatchedRepo(repo=repo)
            if sender:
                self._watched_repos[repo].subscribers.add(sender)
            broker.start_poll(_CI_POLL_S)
        logger.info("repo CI watch: %s by %s", repo, sender)
        return f"watching CI for all open PRs in {repo} (poll every {_CI_POLL_S}s)"

    @command(description="Unsubscribe from repo-level CI events", name="unwatch-repo")
    async def unwatch_repo(self, repo: str, broker: BrokerClient, sender: str = "") -> str:
        async with self._lock:
            if repo in self._watched_repos:
                self._watched_repos[repo].subscribers.discard(sender)
                if not self._watched_repos[repo].subscribers:
                    del self._watched_repos[repo]
            if not self._watched and not self._watched_repos:
                broker.stop_poll()
        return f"unwatched repo {repo}"

    # ------------------------------------------------------------------
    # Shared
    # ------------------------------------------------------------------

    @command(description="List all currently watched PRs and repos")
    async def list(self, broker: BrokerClient) -> str:
        async with self._lock:
            prs = list(self._watched.keys())
            repos = list(self._watched_repos.keys())
        parts = []
        if prs:
            parts.append(f"PRs: {prs}")
        if repos:
            parts.append(f"repos: {repos}")
        return ", ".join(parts) or "watching: none"

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------

    async def on_poll(self, broker: BrokerClient) -> None:
        await self._poll_watched_prs(broker)
        await self._poll_watched_repos(broker)

    async def _poll_watched_prs(self, broker: BrokerClient) -> None:
        async with self._lock:
            snapshot = list(self._watched.items())
        for pr_num, watch in snapshot:
            status = await asyncio.to_thread(_gh_pr_status, pr_num)
            if status is None or status == watch.last_status:
                continue
            watch.last_status = status
            emoji = {"success": "✅", "failure": "❌", "pending": "⏳"}.get(status, "ℹ️")
            async with self._lock:
                requesters = set(watch.requesters)
            for requester in requesters:
                await broker.post(
                    to=requester,
                    message=f"{emoji} CI #{pr_num}: {status.upper()}",
                )
            logger.info("PR #%s status → %s, notified %s", pr_num, status, requesters)

    async def _poll_watched_repos(self, broker: BrokerClient) -> None:
        async with self._lock:
            repos_snapshot = list(self._watched_repos.keys())
        for repo in repos_snapshot:
            pr_numbers = await asyncio.to_thread(_gh_list_open_pr_numbers, repo)
            if pr_numbers is None:
                continue

            async with self._lock:
                if repo not in self._watched_repos:
                    continue
                watched = self._watched_repos[repo]
                # drop PRs that have closed/merged (no longer in open list)
                stale = set(watched.pr_last_status) - set(pr_numbers)
                for n in stale:
                    del watched.pr_last_status[n]
                subscribers = set(watched.subscribers)

            for pr_num in pr_numbers:
                status = await asyncio.to_thread(_gh_pr_status, pr_num, repo)
                if status is None or status in _SKIP_STATUSES:
                    continue
                async with self._lock:
                    if repo not in self._watched_repos:
                        break
                    last = self._watched_repos[repo].pr_last_status.get(pr_num)
                    if status == last:
                        continue
                    self._watched_repos[repo].pr_last_status[pr_num] = status
                emoji = "✅" if status == "success" else "❌"
                event = f"ci_result: #{pr_num} {status} {repo}"
                for sub in subscribers:
                    await broker.post(to=sub, message=f"{emoji} {event}")
                logger.info("%s → %s", event, subscribers)


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        asyncio.run(CiWatcherPlugin().run())
    except KeyboardInterrupt:
        print("[ci-watcher] stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
