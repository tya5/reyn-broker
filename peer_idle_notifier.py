#!/usr/bin/env python3
"""Peer idle notifier plugin for reyn-broker.

Subscribes to ``status_changed`` events and immediately notifies
PEER_IDLE_NOTIFY when a monitored session transitions to an idle state.

Use this to detect when a session finishes its current task and is ready
for new work — the notification fires the moment the session calls
``update_session_status(..., status="idle")``.

Runs as a broker-managed plugin (auto_start=True recommended).

Environment variables
---------------------
PEER_IDLE_NOTIFY     Target session for notifications (default: backlog-watcher)
PEER_IDLE_WATCH      Comma-separated session ids to monitor.
                     If unset, all sessions except broker and self are watched.
PEER_IDLE_STATES     Comma-separated status values that trigger notification
                     (default: idle)
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Any

from plugin_base import BrokerClient, BrokerPlugin

logger = logging.getLogger("broker.plugin.peer_idle_notifier")

_NOTIFY_TARGET = os.environ.get("PEER_IDLE_NOTIFY", "backlog-watcher")
_WATCH_LIST: frozenset[str] | None = (
    frozenset(filter(None, os.environ.get("PEER_IDLE_WATCH", "").split(","))) or None
)
_IDLE_STATES: frozenset[str] = frozenset(
    filter(None, os.environ.get("PEER_IDLE_STATES", "idle").split(","))
)


class PeerIdleNotifier(BrokerPlugin):
    session_id = "peer-idle-notifier"
    role = "peer idle detection — notifies immediately on status → idle"

    def _should_watch(self, sid: str) -> bool:
        if sid in {"broker", self.session_id}:
            return False
        return sid in _WATCH_LIST if _WATCH_LIST is not None else True

    async def on_start(self, broker: BrokerClient) -> None:
        await broker.subscribe_events(["status_changed"])
        logger.info("[%s] subscribed to status_changed events", self.session_id)

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        if msg.get("event") != "status_changed":
            return
        sid = msg.get("session_id", "")
        if not sid or not self._should_watch(sid):
            return
        status = msg.get("status", "")
        prev = msg.get("prev_status")
        # Only notify on the *transition* into an idle state. A change that
        # stays within idle states (e.g. an idle detail edit: idle→idle) must
        # not re-fire, otherwise a detail update looks like a fresh idle.
        if status in _IDLE_STATES and prev not in _IDLE_STATES:
            detail = msg.get("detail") or ""
            text = f"PEER_IDLE: {sid} → {status}"
            if detail:
                text += f" ({detail})"
            await broker.post(to=_NOTIFY_TARGET, message=text)
            logger.info("%s", text)


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        asyncio.run(PeerIdleNotifier().run())
    except KeyboardInterrupt:
        print("[peer-idle-notifier] stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
