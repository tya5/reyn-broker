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
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

logger = logging.getLogger("broker")

_VERSION = "0.10.0"
_STARTED_AT_TS: float = time.time()
_tool_call_counts: dict[str, int] = defaultdict(int)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SessionEntry:
    session_id: str
    working_dir: str
    mcp_session: ServerSession | None  # None for entries restored from disk
    role: str | None = None
    last_post_at: str | None = None
    last_receive_at: str | None = None
    session_expires_at: float | None = None  # epoch timestamp; None = no TTL


sessions: dict[str, SessionEntry] = {}
pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
registry_lock = asyncio.Lock()

_DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "reyn-broker" / "state.json"
_STATE_PATH = Path(os.environ.get("BROKER_STATE_FILE", _DEFAULT_STATE_PATH))


async def _background_purge() -> None:
    """Periodically purge expired messages and expired sessions (every 5 min)."""
    while True:
        await asyncio.sleep(300)
        async with registry_lock:
            # purge expired messages from all inboxes
            for sid in list(pending.keys()):
                pending[sid] = _purge_expired(pending[sid])
                if not pending[sid]:
                    del pending[sid]
            # purge expired sessions
            now = time.time()
            expired = [
                sid for sid, e in sessions.items()
                if e.session_expires_at is not None and e.session_expires_at < now
            ]
            for sid in expired:
                del sessions[sid]
                logger.info("session TTL expired, removed: %s", sid)
            if expired:
                _save_state()


@asynccontextmanager
async def _lifespan(app: Any):
    task = asyncio.create_task(_background_purge())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


mcp = FastMCP("broker", lifespan=_lifespan)


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
                    "last_post_at": e.last_post_at,
                    "last_receive_at": e.last_receive_at,
                    "session_expires_at": e.session_expires_at,
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
            last_post_at=entry.get("last_post_at"),
            last_receive_at=entry.get("last_receive_at"),
            session_expires_at=entry.get("session_expires_at"),
        )
    for sid, msgs in data.get("pending", {}).items():
        pending[sid].extend(msgs)
    logger.info(
        "restored state: %d sessions, %d queued messages across %d inboxes",
        len(sessions),
        sum(len(v) for v in pending.values()),
        len(pending),
    )


def _purge_expired(msgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop messages whose ``_expires_at`` epoch timestamp is in the past."""
    now = time.time()
    return [m for m in msgs if not (m.get("_expires_at") and m["_expires_at"] < now)]


def _strip_internal(msg: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``msg`` with all internal ``_*`` fields removed."""
    return {k: v for k, v in msg.items() if not k.startswith("_")}


def _drain_inbox_locked(session_id: str) -> list[dict[str, Any]]:
    """Pop ``session_id``'s inbox, processing any ``_ack_to`` markers.

    Caller MUST hold ``registry_lock``. Expired messages (``_expires_at``
    in the past) are silently dropped before processing. Returns messages
    with all internal fields (``_ack_to``, ``_expires_at``) stripped. For
    each stripped ``_ack_to`` marker, queues a ``read-ack`` notification
    into the original sender's inbox.  Also updates ``last_receive_at``
    on the session entry when messages are present.
    """
    raw = _purge_expired(pending.pop(session_id, []))
    if raw and session_id in sessions:
        sessions[session_id].last_receive_at = _now_iso()
    ack_targets: list[str] = []
    out: list[dict[str, Any]] = []
    for m in raw:
        ack_to = m.get("_ack_to")
        if ack_to:
            ack_targets.append(ack_to)
        out.append(_strip_internal(m))
    for ack_to in ack_targets:
        pending[ack_to].append({
            "from": "broker",
            "message": f"read-ack: '{session_id}' drained your message",
        })
    return out


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


def _register_locked(
    session_id: str,
    working_dir: str,
    mcp_session: Any,
    role: str | None,
    ttl_hours: float | None,
) -> tuple[str, list[dict[str, Any]]]:
    """Register a session and drain its inbox.  Caller MUST hold ``registry_lock``."""
    session_expires_at: float | None = None
    if ttl_hours is not None:
        session_expires_at = time.time() + ttl_hours * 3600
    sessions[session_id] = SessionEntry(
        session_id=session_id,
        working_dir=working_dir,
        mcp_session=mcp_session,
        role=role,
        session_expires_at=session_expires_at,
    )
    backlog = _drain_inbox_locked(session_id)
    return f"registered '{session_id}' at {working_dir}", backlog


def _session_list_locked(compact: bool) -> list[dict[str, Any]]:
    """Serialise the session registry.  Caller MUST hold ``registry_lock``."""
    if compact:
        return [
            {"session_id": e.session_id, "role": e.role}
            for e in sessions.values()
        ]
    return [
        {
            "session_id": e.session_id,
            "working_dir": e.working_dir,
            "role": e.role,
            "last_post_at": e.last_post_at,
            "last_receive_at": e.last_receive_at,
            "inbox_unread_count": len(_purge_expired(pending.get(e.session_id, []))),
        }
        for e in sessions.values()
    ]


@mcp.tool()
async def register_session(
    session_id: str,
    working_dir: str,
    ctx: Context,
    role: str | None = None,
    ttl_hours: float | None = None,
) -> dict[str, Any]:
    """Register this Claude Code session with the broker.

    Call this once at session startup. Pass your directory name as
    ``session_id`` and the absolute path as ``working_dir``. Optionally
    pass ``role`` as a short free-text description of what this session
    does (e.g. ``"PR review"``, ``"e2e tests"``) so peers can find you
    via ``list_sessions`` without guessing from naming conventions.

    ``ttl_hours`` — optional session lifetime in hours. If set, the broker
    will automatically remove this session after that many hours. Useful
    for short-lived task sessions that may not call ``unregister_session``
    before exiting. Omit for permanent sessions.

    Returns any messages that were queued while this session was offline.
    Prefer ``startup_summary`` at startup to combine this call with
    ``list_sessions`` in one round-trip.
    """
    _tool_call_counts["register_session"] += 1
    async with registry_lock:
        status, backlog = _register_locked(session_id, working_dir, ctx.session, role, ttl_hours)
        _save_state()

    return {"status": status, "pending_messages": backlog}


@mcp.tool()
async def startup_summary(
    session_id: str,
    working_dir: str,
    ctx: Context,
    role: str | None = None,
    compact: bool = True,
    ttl_hours: float | None = None,
) -> dict[str, Any]:
    """Register this session and return the peer list in a single round-trip.

    Replaces the common startup pattern of calling ``register_session``
    followed by ``list_sessions``.  Returns the same data as both combined:

    - ``status`` — registration confirmation string.
    - ``pending_messages`` — backlog drained from this session's inbox
      (same as ``register_session`` return value).
    - ``sessions`` — list of currently registered sessions; compact by
      default (``session_id`` + ``role`` only).  Pass ``compact=False``
      for the full shape including activity timestamps.

    ``ttl_hours`` — optional session lifetime in hours (same as
    ``register_session``).

    Using this instead of two separate calls reduces MCP tool invocations
    and the token overhead of two tool results in context.
    """
    _tool_call_counts["startup_summary"] += 1
    async with registry_lock:
        status, backlog = _register_locked(session_id, working_dir, ctx.session, role, ttl_hours)
        session_list = _session_list_locked(compact)
        _save_state()

    return {"status": status, "pending_messages": backlog, "sessions": session_list}


@mcp.tool()
async def unregister_session(session_id: str) -> str:
    """Unregister a session from the broker."""
    _tool_call_counts["unregister_session"] += 1
    async with registry_lock:
        existed = sessions.pop(session_id, None)
        if existed is not None:
            _save_state()
    if existed is None:
        return f"'{session_id}' was not registered"
    return f"unregistered '{session_id}'"


@mcp.tool()
async def list_sessions(compact: bool = False) -> list[dict[str, Any]]:
    """List currently registered sessions.

    When ``compact=True`` (recommended for most callers), returns only
    ``session_id`` and ``role`` — enough to decide who to send a message
    to, at roughly 60 % lower token cost than the full shape.

    When ``compact=False`` (default for backward compatibility), each
    entry also includes ``working_dir``, ``last_post_at``,
    ``last_receive_at``, and ``inbox_unread_count``.

    Tip: at startup use ``startup_summary`` instead — it combines
    ``register_session`` + ``list_sessions`` into one round-trip.
    """
    _tool_call_counts["list_sessions"] += 1
    async with registry_lock:
        return _session_list_locked(compact)


@mcp.tool()
async def post_message(
    to: str,
    from_session: str,
    message: str,
    request_read_ack: bool = False,
    recipients: list[str] | None = None,
    ttl_seconds: int | None = None,
) -> str:
    """Send a message to one or more sessions.

    The message is always queued in each recipient's inbox. Recipients
    pick it up by calling ``receive_messages``. A best-effort log
    notification is also pushed as a hint, but recipients must not rely
    on it — Claude Code does not always surface log notifications to
    the agent.

    **Single recipient** (default): pass ``to`` as the target session id.

    **Multiple recipients**: pass ``recipients=[...]`` with a list of
    session ids. When ``recipients`` is provided it takes precedence over
    ``to``; ``to`` is then ignored. Returns a summary of how many
    recipients were online/offline.

    ``request_read_ack=True`` makes the broker automatically queue a
    ``read-ack`` message back to ``from_session`` when *each* recipient
    drains this message via ``receive_messages``. Use sparingly for
    confirm-required coordination signals (block raised / pause / etc.).
    The ack confirms the message was drained, not necessarily acted on.

    ``ttl_seconds`` sets an expiry time on the message. If a recipient
    has not drained the message before the TTL elapses, the message is
    silently dropped on the next drain or ``inbox_stats`` call. Use for
    time-sensitive coordination signals where a stale message would cause
    confusion (e.g. "deploy window open for the next 5 minutes").
    """
    _tool_call_counts["post_message"] += 1
    sent_at = _now_iso()
    targets = recipients if recipients is not None else [to]

    payload: dict[str, Any] = {
        "from": from_session,
        "message": message,
        "sent_at_iso": sent_at,
    }
    if request_read_ack:
        payload["_ack_to"] = from_session
    if ttl_seconds is not None:
        payload["_expires_at"] = time.time() + ttl_seconds

    online: list[str] = []
    offline: list[str] = []

    async with registry_lock:
        for target in targets:
            pending[target].append(dict(payload))
            (online if target in sessions else offline).append(target)
        if from_session in sessions:
            sessions[from_session].last_post_at = sent_at
        _save_state()

    for target in online:
        push_payload = _strip_internal(payload)
        await _deliver(target, push_payload)

    if len(targets) == 1:
        return f"queued for '{targets[0]}' (online={targets[0] in online})"
    return (
        f"queued for {len(targets)} recipients"
        f" (online: {', '.join(online) or 'none'}"
        f"; offline: {', '.join(offline) or 'none'})"
    )


@mcp.tool()
async def broadcast_message(
    from_session: str,
    message: str,
    exclude_self: bool = True,
    recipients: list[str] | None = None,
) -> str:
    """Queue ``message`` in every registered session's inbox (or a subset).

    Same semantics as ``post_message`` but addressed to all registered
    sessions at once. By default the sender's own inbox is skipped
    (``exclude_self=True``). Use for announcements (broker restarts,
    protocol changes) or "anyone available?" calls — the addressed-inbox
    model is preserved (each recipient drains its own inbox via
    ``receive_messages``).

    ``recipients`` — optional list of session ids to limit the broadcast
    to a specific subset. Only sessions in this list (and currently
    registered) receive the message. ``exclude_self`` still applies.
    When omitted all registered sessions receive the message.
    """
    _tool_call_counts["broadcast_message"] += 1
    sent_at = _now_iso()
    payload: dict[str, Any] = {
        "from": from_session,
        "message": message,
        "is_broadcast": True,
        "sent_at_iso": sent_at,
    }

    async with registry_lock:
        if recipients is not None:
            targets = [
                sid for sid in recipients
                if sid in sessions and not (exclude_self and sid == from_session)
            ]
        else:
            targets = [sid for sid in sessions if not (exclude_self and sid == from_session)]
        payload["recipient_count"] = len(targets)
        for sid in targets:
            pending[sid].append(payload)
        if from_session in sessions:
            sessions[from_session].last_post_at = sent_at
        _save_state()

    for sid in targets:
        await _deliver(sid, payload)

    return f"broadcast to {len(targets)} sessions"


@mcp.tool()
async def receive_messages(
    session_id: str,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Drain and return all queued messages addressed to ``session_id``.

    Each Claude Code session should call this proactively — at startup
    after ``register_session``, at the start of each turn, after
    long-running tasks, and whenever the user asks "check your inbox".
    The returned list is removed from the queue once handed back.

    For any drained message whose sender requested a read-ack (via
    ``post_message(..., request_read_ack=True)``), a ``read-ack``
    notification is automatically queued back to the original sender's
    inbox before this call returns.

    ``fields`` — optional list of keys to include in each returned
    message (e.g. ``["from", "message"]``).  Omitting metadata fields
    such as ``sent_at_iso``, ``is_broadcast``, and ``recipient_count``
    can significantly reduce token overhead when those fields are not
    needed.  Defaults to ``None`` (all fields returned).
    """
    _tool_call_counts["receive_messages"] += 1
    async with registry_lock:
        msgs = _drain_inbox_locked(session_id)
        if msgs:
            _save_state()
    if fields is not None:
        msgs = [{k: v for k, v in m.items() if k in fields} for m in msgs]
    return msgs


@mcp.tool()
async def inbox_stats(session_id: str) -> dict[str, Any]:
    """Return non-destructive stats about the inbox of ``session_id``.

    Lets a caller confirm whether messages are queued (and who from)
    without draining them. Useful for sanity-checking that a watcher
    has not raced ahead of the caller, or for "have I been heard?"
    diagnostics. Does NOT remove messages from the queue.
    """
    _tool_call_counts["inbox_stats"] += 1
    async with registry_lock:
        msgs = _purge_expired(pending.get(session_id, []))
        senders = sorted({str(m.get("from", "unknown")) for m in msgs})
        count = len(msgs)
    return {
        "session_id": session_id,
        "pending_count": count,
        "senders": senders,
    }


@mcp.tool()
async def peek_messages(
    session_id: str,
    limit: int = 10,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Preview queued messages without draining the inbox.

    Returns the oldest ``limit`` messages (default 10) from ``session_id``'s
    inbox without removing them. Useful for triage decisions (do I need to
    interrupt my current task?) and debugging (what's actually in my inbox?)
    without committing to a drain that triggers read-acks and clears the queue.

    ``fields`` — optional key selector, same semantics as ``receive_messages``.
    Pass e.g. ``["from", "message"]`` to strip metadata and reduce token cost.

    Unlike ``inbox_stats``, ``peek_messages`` shows message content rather
    than just counts and senders.  Unlike ``receive_messages``, the messages
    stay in the queue after this call.
    """
    _tool_call_counts["peek_messages"] += 1
    async with registry_lock:
        msgs = _purge_expired(pending.get(session_id, []))
    result = [_strip_internal(m) for m in msgs[:limit]]
    if fields is not None:
        result = [{k: v for k, v in m.items() if k in fields} for m in result]
    return result


@mcp.tool()
async def health_check() -> dict[str, Any]:
    """Return broker health and runtime statistics.

    Useful for monitoring, smoke-testing after a broker restart, and
    confirming which version is running before refreshing tool schemas.

    Returns:
    - ``version`` — broker version string.
    - ``started_at_iso`` — ISO-8601 UTC timestamp of when the broker
      process started.
    - ``uptime_seconds`` — integer seconds since startup.
    - ``session_count`` — number of currently registered sessions.
    - ``total_pending`` — total messages queued across all inboxes.
    """
    _tool_call_counts["health_check"] += 1
    async with registry_lock:
        sc = len(sessions)
        tp = sum(len(v) for v in pending.values())
    return {
        "version": _VERSION,
        "started_at_iso": datetime.fromtimestamp(_STARTED_AT_TS, tz=timezone.utc).isoformat(),
        "uptime_seconds": int(time.time() - _STARTED_AT_TS),
        "session_count": sc,
        "total_pending": tp,
    }


@mcp.tool()
async def tool_stats() -> dict[str, Any]:
    """Return per-tool call counts since broker startup.

    Useful for identifying which tools are called most frequently so you
    can prioritise token-reduction efforts. Counts reset on broker restart.

    Returns a dict with:
    - ``counts`` — mapping of tool name → call count, sorted by count desc.
    - ``total_calls`` — sum of all tool invocations.
    - ``uptime_seconds`` — seconds since broker started (for normalisation).
    """
    _tool_call_counts["tool_stats"] += 1
    counts = dict(sorted(_tool_call_counts.items(), key=lambda x: x[1], reverse=True))
    return {
        "counts": counts,
        "total_calls": sum(counts.values()),
        "uptime_seconds": int(time.time() - _STARTED_AT_TS),
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
