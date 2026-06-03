#!/usr/bin/env python3
"""Peer stall detector plugin for reyn-broker.

Subscribes to ``status_changed`` / ``registered`` / ``unregistered`` session
events and posts a PEER_STALL message when a monitored session has been in
``idle`` or ``waiting`` state for >= PEER_STALL_THRESHOLD_S seconds.

Sessions must call ``update_session_status`` (or the ``reyn-broker-status``
CLI from a stop hook) to drive state transitions explicitly.

Runs as a broker-managed plugin (auto_start=True recommended).

Environment variables
---------------------
PEER_STALL_POLL_S        Staleness check interval in seconds (default: 60)
PEER_STALL_THRESHOLD_S   Silence threshold in seconds (default: 900)
PEER_STALL_NOTIFY        Target session for alerts (default: backlog-watcher)
PEER_STALL_WATCH         Comma-separated session ids to monitor.
                         If unset, all sessions except broker and self are watched.
PEER_STALL_STATES        Comma-separated status values considered "stalled"
                         (default: idle,waiting)
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
_WATCH_LIST: frozenset[str] | None = (
    frozenset(filter(None, os.environ.get("PEER_STALL_WATCH", "").split(","))) or None
)
_STALL_STATES: frozenset[str] = frozenset(
    filter(None, os.environ.get("PEER_STALL_STATES", "idle,waiting").split(","))
)


class PeerStallWatcher(BrokerPlugin):
    session_id = "peer-stall-watcher"
    role = "peer stall detection — status_changed event driven"
    poll_interval = _POLL_S

    def __init__(self) -> None:
        # sid → (status, datetime when status was set)
        self._state: dict[str, tuple[str, datetime]] = {}
        # sessions for which a PEER_STALL alert has already been sent this cycle
        # cleared when the session transitions to a non-stall status
        self._alerted: set[str] = set()

    def _should_watch(self, sid: str) -> bool:
        if sid in {"broker", self.session_id}:
            return False
        if _WATCH_LIST is not None:
            return sid in _WATCH_LIST
        return True

    async def on_start(self, broker: BrokerClient) -> None:
        # Seed current state from list_sessions (one-time snapshot)
        sessions = await broker.list_sessions(compact=False)
        now = datetime.now(timezone.utc)
        if isinstance(sessions, list):
            for s in sessions:
                sid = s.get("session_id", "")
                if not self._should_watch(sid):
                    continue
                status = s.get("status") or "unknown"
                self._state[sid] = (status, now)

        await broker.subscribe_events(["status_changed", "registered", "unregistered"])
        logger.info("[%s] watching %d sessions", self.session_id, len(self._state))

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        event = msg.get("event")
        sid = msg.get("session_id", "")
        if not event or not sid or not self._should_watch(sid):
            return

        now = datetime.now(timezone.utc)
        if event == "status_changed":
            status = msg.get("status") or "unknown"
            self._state[sid] = (status, now)
            if status not in _STALL_STATES:
                self._alerted.discard(sid)
        elif event == "registered":
            self._state[sid] = ("active", now)
            self._alerted.discard(sid)
        elif event == "unregistered":
            self._state.pop(sid, None)
            self._alerted.discard(sid)

    async def on_poll(self, broker: BrokerClient) -> None:
        now = datetime.now(timezone.utc)
        for sid, (status, since) in list(self._state.items()):
            if status not in _STALL_STATES:
                continue
            if sid in self._alerted:
                continue  # already notified this stall cycle
            elapsed_s = (now - since).total_seconds()
            if elapsed_s >= _STALL_THRESHOLD_S:
                minutes = int(elapsed_s // 60)
                await broker.post(
                    to=_NOTIFY_TARGET,
                    message=(
                        f"PEER_STALL: {sid} status={status} "
                        f"for={minutes}min since={since.isoformat()}"
                    ),
                )
                self._alerted.add(sid)


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
