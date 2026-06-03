#!/usr/bin/env python3
"""Peer stall detector plugin for reyn-broker.

Subscribes to ``posted`` / ``registered`` / ``unregistered`` session events
and posts a PEER_STALL message when any peer has been silent for >=
PEER_STALL_THRESHOLD_S seconds.  Zero list_sessions polling — entirely
event-driven with a lightweight periodic staleness check.

Runs as a broker-managed plugin (auto_start=True recommended).

Environment variables
---------------------
PEER_STALL_POLL_S        Staleness check interval in seconds (default: 60)
PEER_STALL_THRESHOLD_S   Silence threshold in seconds (default: 900)
PEER_STALL_NOTIFY        Target session for alerts (default: backlog-watcher)
PEER_STALL_WATCH         Comma-separated session ids to monitor.
                         If unset, all sessions except broker and self are watched.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from plugin_base import BrokerClient, BrokerPlugin

logger = logging.getLogger("broker.plugin.peer_stall_watcher")

_POLL_S = int(os.environ.get("PEER_STALL_POLL_S", "60"))
_STALL_THRESHOLD_S = int(os.environ.get("PEER_STALL_THRESHOLD_S", "900"))
_NOTIFY_TARGET = os.environ.get("PEER_STALL_NOTIFY", "backlog-watcher")
# Explicit watch list — if set, only these sessions are monitored.
# If unset/empty, all sessions except broker and self are monitored.
_WATCH_LIST: frozenset[str] | None = (
    frozenset(filter(None, os.environ.get("PEER_STALL_WATCH", "").split(","))) or None
)


class PeerStallWatcher(BrokerPlugin):
    session_id = "peer-stall-watcher"
    role = "peer stall detection — event-driven, zero list_sessions polling"
    poll_interval = _POLL_S

    def __init__(self) -> None:
        self._last_seen: dict[str, datetime] = {}

    def _should_watch(self, sid: str) -> bool:
        if sid in {"broker", self.session_id}:
            return False
        if _WATCH_LIST is not None:
            return sid in _WATCH_LIST
        return True

    async def on_start(self, broker: BrokerClient) -> None:
        # Seed last_seen from current session state (one-time snapshot at startup)
        sessions = await broker.list_sessions(compact=False)
        now = datetime.now(timezone.utc)
        if isinstance(sessions, list):
            for s in sessions:
                sid = s.get("session_id", "")
                if not self._should_watch(sid):
                    continue
                last_post = s.get("last_post_at")
                if last_post:
                    try:
                        self._last_seen[sid] = datetime.fromisoformat(last_post)
                    except ValueError:
                        self._last_seen[sid] = now

        # Switch to push-based updates — no repeated list_sessions polling
        await broker.subscribe_events(["posted", "registered", "unregistered"])
        logger.info("[%s] subscribed to session events; seeded %d peers",
                    self.session_id, len(self._last_seen))

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        event = msg.get("event")
        sid = msg.get("session_id", "")
        if not event or not sid or not self._should_watch(sid):
            return

        if event == "posted":
            at = msg.get("at", "")
            try:
                self._last_seen[sid] = datetime.fromisoformat(at)
            except ValueError:
                self._last_seen[sid] = datetime.now(timezone.utc)
        elif event == "registered":
            self._last_seen[sid] = datetime.now(timezone.utc)
        elif event == "unregistered":
            self._last_seen.pop(sid, None)

    async def on_poll(self, broker: BrokerClient) -> None:
        now = datetime.now(timezone.utc)
        for sid, last in list(self._last_seen.items()):
            silent_s = (now - last).total_seconds()
            if silent_s >= _STALL_THRESHOLD_S:
                minutes = int(silent_s // 60)
                await broker.post(
                    to=_NOTIFY_TARGET,
                    message=f"PEER_STALL: {sid} silent={minutes}min last_post={last.isoformat()}",
                )


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        asyncio.run(PeerStallWatcher().run())
    except KeyboardInterrupt:
        print("[peer-stall-watcher] stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
