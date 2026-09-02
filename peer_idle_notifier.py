#!/usr/bin/env python3
"""Peer idle notifier plugin for reyn-broker.

Subscribes to ``active_changed`` events and immediately notifies
PEER_IDLE_NOTIFY when a monitored session goes idle (its in-turn bit flips
to False — typically driven by the session's Stop hook calling
``set_active(id, False)``). #31: a session whose hooks never report (so
``active`` stays ``None``, never True/False) never fires this event at
all — structurally out of reach here, not something this plugin needs to
filter for.

The ``active`` bool is the *authoritative* idle signal (deterministic,
hook-driven). The session's semantic ``status`` / ``detail`` (e.g. "waiting"
+ "ci:#1268") is carried along the event as best-effort enrichment so the
notification can say *why* the session is idle when that info exists.

Runs as a broker-managed plugin (auto_start=True recommended).

Notification recipients
------------------------
Recipients are whoever has called ``subscribe_plugin_notifications(plugin=
"peer-idle-notifier", session_id=...)`` on the broker — self-service, no
maintainer/restart needed to add or drop a recipient (reyn-broker#26).
``PEER_IDLE_NOTIFY`` is now only a fallback used when nobody has opted in.

Environment variables
---------------------
PEER_IDLE_NOTIFY     Fallback target used only while the subscriber list is
                     empty (default: backlog-watcher).
PEER_IDLE_WATCH      Comma-separated session ids to monitor.
                     If unset, all sessions except broker and self are watched.
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


class PeerIdleNotifier(BrokerPlugin):
    session_id = "peer-idle-notifier"
    role = "peer idle detection — notifies immediately on active → False"

    def _should_watch(self, sid: str) -> bool:
        if sid in {"broker", self.session_id}:
            return False
        return sid in _WATCH_LIST if _WATCH_LIST is not None else True

    async def on_start(self, broker: BrokerClient) -> None:
        await broker.subscribe_events(["active_changed"])
        logger.info("[%s] subscribed to active_changed events", self.session_id)

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        if msg.get("event") != "active_changed":
            return
        sid = msg.get("session_id", "")
        if not sid or not self._should_watch(sid):
            return
        # active_changed only fires on an actual flip, so active=False here is
        # always the genuine work→idle edge. No edge-detection bookkeeping needed.
        if msg.get("active") is False:
            status = msg.get("status") or ""
            detail = msg.get("detail") or ""
            text = f"PEER_IDLE: {sid} is ready for new work"
            if status:
                text += f" (status={status}" + (f": {detail}" if detail else "") + ")"
            targets = await broker.notification_subscribers()
            if not targets:
                targets = [_NOTIFY_TARGET]
            for target in targets:
                await broker.post(to=target, message=text)
            logger.info("%s -> %s", text, targets)


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
