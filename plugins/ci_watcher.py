"""GitHub CI watcher plugin for reyn-broker.

Polls GitHub PR check status via ``gh`` CLI and relays state changes
as broker messages to requesting sessions.

Usage
-----
Register and start via broker MCP::

    plugin_add(
        name="ci-watcher",
        command="/path/to/.venv/bin/reyn-broker-ci",
        session_id="ci-watcher",
        auto_start=True,
    )
    plugin_start("ci-watcher")

Or run directly::

    /path/to/.venv/bin/python /path/to/broker/plugins/ci_watcher.py

Broker protocol
---------------
To watch a PR, send a message to the ``ci-watcher`` session::

    post_message(to="ci-watcher", from_session="my-session", message="watch:#1268")
    post_message(to="ci-watcher", from_session="my-session", message="unwatch:#1268")
    post_message(to="ci-watcher", from_session="my-session", message="list")

When CI status changes, ``ci-watcher`` posts back to all requesting sessions::

    {"from": "ci-watcher", "message": "✅ CI #1268: SUCCESS"}

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
from dataclasses import dataclass, field
from typing import Any

from plugin_base import BrokerClient, BrokerPlugin

logger = logging.getLogger("broker.plugin.ci_watcher")

_CI_POLL_S = int(os.environ.get("CI_POLL_S", "60"))


@dataclass
class WatchedPR:
    pr_number: str
    requesters: set[str] = field(default_factory=set)  # fan-out: all sessions to notify
    last_status: str | None = None


def _gh_pr_status(pr_number: str) -> str | None:
    """Return overall CI status for a PR via ``gh`` CLI. Returns None on error."""
    try:
        result = subprocess.run(
            ["gh", "pr", "checks", pr_number, "--json", "name,status,conclusion"],
            capture_output=True, text=True, timeout=30,
        )
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


class CiWatcherPlugin(BrokerPlugin):
    session_id = "ci-watcher"
    role = "GitHub CI poll → broker relay"
    # poll_interval=None: polling starts only when a PR is watched

    def __init__(self) -> None:
        self._watched: dict[str, WatchedPR] = {}
        self._lock = asyncio.Lock()

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        """Handle watch/unwatch/list commands from other sessions."""
        text = msg.get("message", "").strip()
        requester = msg.get("from", "")

        if text.startswith("watch:#"):
            pr_num = text[7:].strip()
            async with self._lock:
                if pr_num not in self._watched:
                    self._watched[pr_num] = WatchedPR(pr_number=pr_num)
                self._watched[pr_num].requesters.add(requester)
                if not broker._poll.running:
                    broker.start_poll(_CI_POLL_S)
            logger.info("watching PR #%s for %s", pr_num, requester)
            await broker.post(
                to=requester,
                message=f"CI watching: PR #{pr_num} (poll every {_CI_POLL_S}s)",
            )

        elif text.startswith("unwatch:#"):
            pr_num = text[9:].strip()
            async with self._lock:
                if pr_num in self._watched:
                    self._watched[pr_num].requesters.discard(requester)
                    if not self._watched[pr_num].requesters:
                        del self._watched[pr_num]
                if not self._watched:
                    broker.stop_poll()
            await broker.post(to=requester, message=f"CI unwatched: PR #{pr_num}")

        elif text == "list":
            async with self._lock:
                watching = list(self._watched.keys())
            await broker.post(to=requester, message=f"CI watching: {watching or 'none'}")

    async def on_poll(self, broker: BrokerClient) -> None:
        """Check CI status for all watched PRs and notify requesters on change."""
        async with self._lock:
            snapshot = list(self._watched.items())
        for pr_num, watch in snapshot:
            # asyncio.to_thread keeps the event loop unblocked during subprocess call
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


def main() -> None:
    import sys
    logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        asyncio.run(CiWatcherPlugin().run())
    except KeyboardInterrupt:
        print("[ci-watcher] stopped", file=sys.stderr)
