#!/usr/bin/env python3
"""Broker MCP server for inter-session messaging.

Each Claude Code session connects to this broker over Streamable HTTP,
registers itself with a session_id, and can post messages to other
registered sessions. Incoming messages are pushed to the target session
via the MCP-standard ``notifications/message`` (logging) notification.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

logger = logging.getLogger("broker")


@dataclass
class SessionEntry:
    session_id: str
    working_dir: str
    mcp_session: ServerSession | None  # None for entries restored from disk
    role: str | None = None


sessions: dict[str, SessionEntry] = {}
pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
registry_lock = asyncio.Lock()

_DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "reyn-broker" / "state.json"
_STATE_PATH = Path(os.environ.get("BROKER_STATE_FILE", _DEFAULT_STATE_PATH))

mcp = FastMCP("broker")


def _save_state() -> None:
    """Persist sessions metadata + pending queue atomically.

    Called under ``registry_lock`` after each mutation. The ``mcp_session``
    ref is intentionally not persisted — it is tied to a live connection
    and is irrelevant for restored entries (push notifications are
    best-effort and skipped for entries without a live session).
    """
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "sessions": [
                {
                    "session_id": e.session_id,
                    "working_dir": e.working_dir,
                    "role": e.role,
                }
                for e in sessions.values()
            ],
            "pending": {k: v for k, v in pending.items() if v},
        }
        tmp = _STATE_PATH.with_suffix(_STATE_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False))
        tmp.replace(_STATE_PATH)
    except OSError as exc:
        logger.warning("state persistence failed: %s", exc)


def _load_state() -> None:
    """Load persisted state at startup. Safe to call when no file exists."""
    if not _STATE_PATH.exists():
        return
    try:
        data = json.loads(_STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("state load failed (%s); starting fresh", exc)
        return
    for entry in data.get("sessions", []):
        sid = entry["session_id"]
        sessions[sid] = SessionEntry(
            session_id=sid,
            working_dir=entry["working_dir"],
            mcp_session=None,
            role=entry.get("role"),
        )
    for sid, msgs in data.get("pending", {}).items():
        pending[sid].extend(msgs)
    logger.info(
        "restored state: %d sessions, %d queued messages across %d inboxes",
        len(sessions),
        sum(len(v) for v in pending.values()),
        len(pending),
    )


def _drain_inbox_locked(session_id: str) -> list[dict[str, Any]]:
    """Pop ``session_id``'s inbox, processing any ``_ack_to`` markers.

    Caller MUST hold ``registry_lock``. Returns messages with internal
    fields (``_ack_to``) stripped. For each stripped marker, queues a
    ``read-ack`` notification into the original sender's inbox.
    """
    msgs = pending.pop(session_id, [])
    ack_targets: list[str] = []
    for m in msgs:
        ack_to = m.pop("_ack_to", None)
        if ack_to:
            ack_targets.append(ack_to)
    for ack_to in ack_targets:
        pending[ack_to].append({
            "from": "broker",
            "message": f"read-ack: '{session_id}' drained your message",
        })
    return msgs


async def _deliver(target_id: str, payload: dict[str, Any]) -> bool:
    entry = sessions.get(target_id)
    if entry is None or entry.mcp_session is None:
        return False
    try:
        await entry.mcp_session.send_log_message(
            level="info",
            data=payload,
            logger="broker",
        )
        return True
    except Exception as exc:
        logger.warning("delivery to %s failed: %s", target_id, exc)
        return False


@mcp.tool()
async def register_session(
    session_id: str,
    working_dir: str,
    ctx: Context,
    role: str | None = None,
) -> dict[str, Any]:
    """Register this Claude Code session with the broker.

    Call this once at session startup. Pass your directory name as
    ``session_id`` and the absolute path as ``working_dir``. Optionally
    pass ``role`` as a short free-text description of what this session
    does (e.g. ``"PR review"``, ``"e2e tests"``) so peers can find you
    via ``list_sessions`` without guessing from naming conventions.

    Returns any messages that were queued while this session was offline.
    """
    async with registry_lock:
        sessions[session_id] = SessionEntry(
            session_id=session_id,
            working_dir=working_dir,
            mcp_session=ctx.session,
            role=role,
        )
        backlog = _drain_inbox_locked(session_id)
        _save_state()

    return {
        "status": f"registered '{session_id}' at {working_dir}",
        "pending_messages": backlog,
    }


@mcp.tool()
async def unregister_session(session_id: str) -> str:
    """Unregister a session from the broker."""
    async with registry_lock:
        existed = sessions.pop(session_id, None)
        if existed is not None:
            _save_state()
    if existed is None:
        return f"'{session_id}' was not registered"
    return f"unregistered '{session_id}'"


@mcp.tool()
async def list_sessions() -> list[dict[str, Any]]:
    """List currently registered sessions.

    Each entry contains ``session_id``, ``working_dir``, and ``role``
    (``None`` if the session did not declare one).
    """
    async with registry_lock:
        return [
            {
                "session_id": e.session_id,
                "working_dir": e.working_dir,
                "role": e.role,
            }
            for e in sessions.values()
        ]


@mcp.tool()
async def post_message(
    to: str,
    from_session: str,
    message: str,
    request_read_ack: bool = False,
) -> str:
    """Send a message to another session.

    The message is always queued in the recipient's inbox. The recipient
    picks it up by calling ``receive_messages``. A best-effort log
    notification is also pushed as a hint, but recipients must not rely
    on it — Claude Code does not always surface log notifications to
    the agent.

    If ``request_read_ack=True``, the broker will automatically queue a
    ``read-ack`` message back to ``from_session`` when the recipient
    drains this message via ``receive_messages``. Use sparingly for
    confirm-required coordination signals (block raised / pause / etc.).
    The ack confirms the message was drained, not necessarily acted on.
    """
    payload: dict[str, Any] = {"from": from_session, "message": message}
    if request_read_ack:
        payload["_ack_to"] = from_session

    async with registry_lock:
        pending[to].append(payload)
        target_online = to in sessions
        _save_state()

    if target_online:
        # Don't push the internal ack marker over the notification channel.
        push_payload = {k: v for k, v in payload.items() if k != "_ack_to"}
        await _deliver(to, push_payload)

    return f"queued for '{to}' (online={target_online})"


@mcp.tool()
async def broadcast_message(
    from_session: str,
    message: str,
    exclude_self: bool = True,
) -> str:
    """Queue ``message`` in every registered session's inbox.

    Same semantics as ``post_message`` but addressed to all registered
    sessions at once. By default the sender's own inbox is skipped
    (``exclude_self=True``). Use for announcements (broker restarts,
    protocol changes) or "anyone available?" calls — the addressed-inbox
    model is preserved (each recipient drains its own inbox via
    ``receive_messages``).
    """
    payload = {"from": from_session, "message": message}

    async with registry_lock:
        targets = [sid for sid in sessions if not (exclude_self and sid == from_session)]
        for sid in targets:
            pending[sid].append(payload)
        _save_state()

    for sid in targets:
        await _deliver(sid, payload)

    return f"broadcast to {len(targets)} sessions"


@mcp.tool()
async def receive_messages(session_id: str) -> list[dict[str, Any]]:
    """Drain and return all queued messages addressed to ``session_id``.

    Each Claude Code session should call this proactively — at startup
    after ``register_session``, at the start of each turn, after
    long-running tasks, and whenever the user asks "check your inbox".
    The returned list is removed from the queue once handed back.

    For any drained message whose sender requested a read-ack (via
    ``post_message(..., request_read_ack=True)``), a ``read-ack``
    notification is automatically queued back to the original sender's
    inbox before this call returns.
    """
    async with registry_lock:
        msgs = _drain_inbox_locked(session_id)
        if msgs:
            _save_state()
    return msgs


@mcp.tool()
async def inbox_stats(session_id: str) -> dict[str, Any]:
    """Return non-destructive stats about the inbox of ``session_id``.

    Lets a caller confirm whether messages are queued (and who from)
    without draining them. Useful for sanity-checking that a watcher
    has not raced ahead of the caller, or for "have I been heard?"
    diagnostics. Does NOT remove messages from the queue.
    """
    async with registry_lock:
        msgs = pending.get(session_id, [])
        senders = sorted({str(m.get("from", "unknown")) for m in msgs})
        count = len(msgs)
    return {
        "session_id": session_id,
        "pending_count": count,
        "senders": senders,
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="reyn-broker",
        description="MCP broker for inter-session messaging between Claude Code sessions.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("BROKER_HOST", "127.0.0.1"),
        help="Bind address (default: 127.0.0.1, env: BROKER_HOST)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("BROKER_PORT", "8765")),
        help="Bind port (default: 8765, env: BROKER_PORT)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("BROKER_LOG_LEVEL", "INFO"),
        help="Python log level (default: INFO, env: BROKER_LOG_LEVEL)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    logger.info(
        "starting broker on %s:%s (state file: %s)",
        mcp.settings.host,
        mcp.settings.port,
        _STATE_PATH,
    )
    _load_state()
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
