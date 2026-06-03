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
import shlex
import time
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

logger = logging.getLogger("broker")

_VERSION = "0.13.0"
_STARTED_AT_TS: float = time.time()
_tool_call_counts: dict[str, int] = defaultdict(int)
# Optional session that receives a copy of every posted/broadcast message.
# Set via BROKER_MONITOR_SESSION env var (e.g. "telegram").
_MONITOR_SID: str | None = os.environ.get("BROKER_MONITOR_SESSION")


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


@dataclass
class PluginEntry:
    name: str
    command: str                              # shell command to launch the plugin
    session_id: str                           # broker session_id the plugin registers as
    env: dict[str, str] = field(default_factory=dict)  # extra env vars (merged with os.environ)
    auto_start: bool = False                  # start automatically when broker boots
    pid: int | None = None                    # last known PID (None = never started)


@dataclass
class EventSubscription:
    subscriber_id: str
    event_types: set[str]                      # "registered" | "unregistered" | "posted"
    session_filter: list[str] | None = None    # None = all sessions


sessions: dict[str, SessionEntry] = {}
pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
plugins: dict[str, PluginEntry] = {}
# subscriber_id → EventSubscription
event_subscriptions: dict[str, EventSubscription] = {}
# session_id → list of command dicts ({name, description, args})
plugin_commands: dict[str, list[dict[str, Any]]] = {}
registry_lock = asyncio.Lock()
# Live asyncio subprocess handles (not persisted — lost on broker restart)
_plugin_procs: dict[str, Any] = {}

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


def _plugin_is_running(entry: PluginEntry) -> bool:
    """Return True if the plugin process is alive."""
    proc = _plugin_procs.get(entry.name)
    if proc is not None and proc.returncode is None:
        return True
    if entry.pid is not None:
        with suppress(OSError):
            os.kill(entry.pid, 0)
            return True
    return False


async def _launch_plugin(entry: PluginEntry) -> bool:
    """Spawn a plugin subprocess. Returns True on success."""
    env = {**os.environ, **entry.env}
    args = shlex.split(entry.command)
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        _plugin_procs[entry.name] = proc
        entry.pid = proc.pid
        logger.info("plugin '%s' started (pid %d)", entry.name, proc.pid)
        return True
    except Exception as exc:
        logger.warning("failed to start plugin '%s': %s", entry.name, exc)
        return False


async def _terminate_plugin(entry: PluginEntry) -> None:
    """Send SIGTERM to a plugin process and wait briefly."""
    proc = _plugin_procs.pop(entry.name, None)
    if proc is not None and proc.returncode is None:
        with suppress(ProcessLookupError):
            proc.terminate()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=5)
    elif entry.pid is not None:
        with suppress(ProcessLookupError, OSError):
            os.kill(entry.pid, 15)  # SIGTERM
    entry.pid = None


@asynccontextmanager
async def _lifespan(app: Any):
    purge_task = asyncio.create_task(_background_purge())
    # Auto-start plugins registered before broker boot
    for entry in list(plugins.values()):
        if entry.auto_start and not _plugin_is_running(entry):
            await _launch_plugin(entry)
    try:
        yield
    finally:
        purge_task.cancel()
        for entry in list(plugins.values()):
            if _plugin_is_running(entry):
                await _terminate_plugin(entry)
        await asyncio.gather(purge_task, return_exceptions=True)


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
            "plugins": [
                {
                    "name": p.name,
                    "command": p.command,
                    "session_id": p.session_id,
                    "env": p.env,
                    "auto_start": p.auto_start,
                    "pid": p.pid,
                }
                for p in plugins.values()
            ],
            "event_subscriptions": [
                {
                    "subscriber_id": s.subscriber_id,
                    "event_types": sorted(s.event_types),
                    "session_filter": s.session_filter,
                }
                for s in event_subscriptions.values()
            ],
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
    for p in data.get("plugins", []):
        plugins[p["name"]] = PluginEntry(
            name=p["name"],
            command=p["command"],
            session_id=p["session_id"],
            env=p.get("env", {}),
            auto_start=p.get("auto_start", False),
            pid=p.get("pid"),
        )
    for sub in data.get("event_subscriptions", []):
        sid = sub["subscriber_id"]
        event_subscriptions[sid] = EventSubscription(
            subscriber_id=sid,
            event_types=set(sub.get("event_types", [])),
            session_filter=sub.get("session_filter"),
        )
    logger.info(
        "restored state: %d sessions, %d queued messages, %d plugins, %d event subs",
        len(sessions),
        sum(len(v) for v in pending.values()),
        len(plugins),
        len(event_subscriptions),
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


def _push_session_event_locked(event_type: str, session_id: str) -> None:
    """Queue a session activity event to all matching subscribers.

    Caller MUST hold ``registry_lock``. The event is queued as a regular
    broker message so subscribers receive it via their normal inbox drain.
    Events are never delivered to the session that triggered them.
    """
    now = _now_iso()
    for sub in event_subscriptions.values():
        if sub.subscriber_id == session_id:
            continue
        if event_type not in sub.event_types:
            continue
        if sub.session_filter is not None and session_id not in sub.session_filter:
            continue
        pending[sub.subscriber_id].append({
            "from": "broker",
            "event": event_type,
            "session_id": session_id,
            "at": now,
        })


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
    _push_session_event_locked("registered", session_id)
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
            _push_session_event_locked("unregistered", session_id)
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
        if _MONITOR_SID and _MONITOR_SID not in targets and from_session != _MONITOR_SID:
            monitor_to = targets[0] if len(targets) == 1 else targets
            pending[_MONITOR_SID].append({**_strip_internal(payload), "monitor_to": monitor_to})
        _push_session_event_locked("posted", from_session)
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
        if _MONITOR_SID and _MONITOR_SID not in targets and from_session != _MONITOR_SID:
            pending[_MONITOR_SID].append({**_strip_internal(payload), "monitor_to": targets})
        _push_session_event_locked("posted", from_session)
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
        "monitor_session": _MONITOR_SID,
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


@mcp.tool()
async def set_monitor_session(session_id: str | None = None) -> str:
    """Enable or disable the monitor session at runtime.

    When enabled, every ``post_message`` and ``broadcast_message`` queues a
    stripped copy to ``session_id`` (with an added ``monitor_to`` field
    showing the original target). This lets a dedicated session (e.g. the
    Telegram bridge) observe all inter-session traffic without being the
    intended recipient.

    Pass ``session_id`` to enable monitoring (replaces any current setting).
    Omit or pass ``None`` to disable monitoring.

    Note: this change is in-memory only and resets on broker restart.
    To persist, set ``BROKER_MONITOR_SESSION`` in the broker's environment.
    """
    global _MONITOR_SID
    _tool_call_counts["set_monitor_session"] += 1
    _MONITOR_SID = session_id if session_id else None
    if _MONITOR_SID:
        return f"monitor enabled: all messages copied to '{_MONITOR_SID}'"
    return "monitor disabled"


# ---------------------------------------------------------------------------
# Plugin command registry tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def register_plugin_commands(
    session_id: str,
    commands: list[dict[str, Any]],
) -> str:
    """Register a plugin's command schema with the broker.

    Called automatically by :class:`BrokerPlugin` on startup — plugin
    authors do not need to call this directly.

    ``commands`` is a list of dicts with keys ``name``, ``description``,
    and ``args`` (list of positional argument names).
    """
    _tool_call_counts["register_plugin_commands"] += 1
    plugin_commands[session_id] = commands
    return f"registered {len(commands)} command(s) for '{session_id}'"


@mcp.tool()
async def get_plugin_commands(session_id: str) -> list[dict[str, Any]]:
    """Return the command schema for a plugin session.

    Use this to discover what commands a plugin accepts before sending
    it a message.  Returns an empty list if the session has not registered
    any commands (e.g. it is a plain broker session, not a plugin).

    Each entry has:
    - ``name``        — command name used as the message prefix.
    - ``description`` — short description of what the command does.
    - ``args``        — ordered list of positional argument names.

    Example::

        get_plugin_commands("github-ci")
        # → [
        #     {"name": "watch",   "args": ["pr_number"], "description": "Watch a PR"},
        #     {"name": "unwatch", "args": ["pr_number"], "description": "Stop watching"},
        #     {"name": "list",    "args": [],            "description": "List watched PRs"},
        #   ]

    To invoke a command, send a message in the format
    ``"<name>:<arg1> <arg2> ..."``:

        post_message(to="github-ci", from_session="me", message="watch:#1268")
    """
    _tool_call_counts["get_plugin_commands"] += 1
    return plugin_commands.get(session_id, [])


# ---------------------------------------------------------------------------
# Session event subscription tools
# ---------------------------------------------------------------------------

_VALID_EVENT_TYPES = frozenset({"registered", "unregistered", "posted"})


@mcp.tool()
async def subscribe_session_events(
    subscriber_id: str,
    event_types: list[str],
    session_filter: list[str] | None = None,
) -> str:
    """Subscribe to session activity events.

    When a matching event occurs, the broker queues a lightweight notification
    to ``subscriber_id``'s inbox::

        {"from": "broker", "event": "<type>", "session_id": "<who>", "at": "<iso>"}

    Drain via ``receive_messages`` as usual. The event is never delivered to
    the session that triggered it (no self-notification).

    ``event_types`` — one or more of:

    - ``"registered"`` — a session called ``register_session`` or
      ``startup_summary``.
    - ``"unregistered"`` — a session called ``unregister_session`` or was
      removed by TTL expiry.
    - ``"posted"`` — a session called ``post_message`` or
      ``broadcast_message`` (i.e. their ``last_post_at`` was updated).

    ``session_filter`` — optional list of session ids to watch. ``None``
    (default) means watch all sessions.

    Subscriptions survive broker restarts (persisted to state file).
    Call ``unsubscribe_session_events`` to cancel.
    """
    _tool_call_counts["subscribe_session_events"] += 1
    unknown = set(event_types) - _VALID_EVENT_TYPES
    if unknown:
        return f"unknown event type(s): {sorted(unknown)}; valid: {sorted(_VALID_EVENT_TYPES)}"
    async with registry_lock:
        existing = event_subscriptions.get(subscriber_id)
        if existing:
            existing.event_types.update(event_types)
            if session_filter is not None:
                existing.session_filter = (
                    list(set((existing.session_filter or []) + session_filter))
                )
        else:
            event_subscriptions[subscriber_id] = EventSubscription(
                subscriber_id=subscriber_id,
                event_types=set(event_types),
                session_filter=session_filter,
            )
        _save_state()
    filter_desc = f" (filter: {session_filter})" if session_filter else " (all sessions)"
    return f"'{subscriber_id}' subscribed to {sorted(event_types)}{filter_desc}"


@mcp.tool()
async def unsubscribe_session_events(
    subscriber_id: str,
    event_types: list[str] | None = None,
) -> str:
    """Unsubscribe from session activity events.

    If ``event_types`` is provided, only those event types are removed.
    If omitted, the entire subscription is cancelled.
    """
    _tool_call_counts["unsubscribe_session_events"] += 1
    async with registry_lock:
        sub = event_subscriptions.get(subscriber_id)
        if sub is None:
            return f"'{subscriber_id}' has no active subscription"
        if event_types is None:
            del event_subscriptions[subscriber_id]
            msg = f"'{subscriber_id}' fully unsubscribed"
        else:
            sub.event_types -= set(event_types)
            if not sub.event_types:
                del event_subscriptions[subscriber_id]
                msg = f"'{subscriber_id}' fully unsubscribed (no event types remaining)"
            else:
                remaining = sorted(sub.event_types)
                msg = f"'{subscriber_id}' removed {event_types}; still watching {remaining}"
        _save_state()
    return msg


# ---------------------------------------------------------------------------
# Plugin lifecycle tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def plugin_add(
    name: str,
    command: str,
    session_id: str,
    env: dict[str, str] | None = None,
    auto_start: bool = False,
) -> str:
    """Register a plugin in the persistent plugin registry.

    ``name``       — unique identifier for this plugin (e.g. ``"telegram"``).
    ``command``    — shell command used to launch the plugin process (e.g.
                     ``"/path/to/.venv/bin/reyn-broker-telegram"``).
    ``session_id`` — the broker session id the plugin will register as.
    ``env``        — optional dict of extra environment variables merged with
                     the broker's own environment when the plugin is launched.
                     Do NOT put secrets here if they are already in the
                     broker's environment; pass them via the system env instead.
    ``auto_start`` — if ``True``, the plugin is started automatically whenever
                     the broker boots.

    The registration is persisted to the state file. Call ``plugin_start`` to
    launch the process immediately.
    """
    _tool_call_counts["plugin_add"] += 1
    if name in plugins:
        return f"plugin '{name}' already registered; use plugin_remove first to replace"
    plugins[name] = PluginEntry(
        name=name, command=command, session_id=session_id,
        env=env or {}, auto_start=auto_start,
    )
    async with registry_lock:
        _save_state()
    return f"plugin '{name}' registered (auto_start={auto_start})"


@mcp.tool()
async def plugin_remove(name: str) -> str:
    """Stop (if running) and remove a plugin from the registry."""
    _tool_call_counts["plugin_remove"] += 1
    entry = plugins.get(name)
    if entry is None:
        return f"plugin '{name}' not found"
    if _plugin_is_running(entry):
        await _terminate_plugin(entry)
    del plugins[name]
    async with registry_lock:
        _save_state()
    return f"plugin '{name}' removed"


@mcp.tool()
async def plugin_start(name: str) -> str:
    """Start a registered plugin by spawning its subprocess."""
    _tool_call_counts["plugin_start"] += 1
    entry = plugins.get(name)
    if entry is None:
        return f"plugin '{name}' not registered; call plugin_add first"
    if _plugin_is_running(entry):
        return f"plugin '{name}' is already running (pid {entry.pid})"
    ok = await _launch_plugin(entry)
    async with registry_lock:
        _save_state()
    if ok:
        return f"plugin '{name}' started (pid {entry.pid})"
    return f"plugin '{name}' failed to start"


@mcp.tool()
async def plugin_stop(name: str) -> str:
    """Send SIGTERM to a running plugin process."""
    _tool_call_counts["plugin_stop"] += 1
    entry = plugins.get(name)
    if entry is None:
        return f"plugin '{name}' not registered"
    if not _plugin_is_running(entry):
        return f"plugin '{name}' is not running"
    await _terminate_plugin(entry)
    async with registry_lock:
        _save_state()
    return f"plugin '{name}' stopped"


@mcp.tool()
async def plugin_restart(name: str) -> str:
    """Stop and restart a plugin."""
    _tool_call_counts["plugin_restart"] += 1
    entry = plugins.get(name)
    if entry is None:
        return f"plugin '{name}' not registered"
    if _plugin_is_running(entry):
        await _terminate_plugin(entry)
    ok = await _launch_plugin(entry)
    async with registry_lock:
        _save_state()
    if ok:
        return f"plugin '{name}' restarted (pid {entry.pid})"
    return f"plugin '{name}' failed to restart"


@mcp.tool()
async def plugin_list() -> list[dict[str, Any]]:
    """List all registered plugins with their current status.

    Each entry includes:
    - ``name`` — plugin identifier.
    - ``command`` — launch command.
    - ``session_id`` — expected broker session id.
    - ``auto_start`` — whether the plugin starts on broker boot.
    - ``pid`` — last known process id (``None`` if never started).
    - ``running`` — ``True`` if the process is currently alive.
    - ``connected`` — ``True`` if the plugin's session is registered on the broker.
    """
    _tool_call_counts["plugin_list"] += 1
    async with registry_lock:
        registered_sessions = set(sessions.keys())
    return [
        {
            "name": e.name,
            "command": e.command,
            "session_id": e.session_id,
            "auto_start": e.auto_start,
            "pid": e.pid,
            "running": _plugin_is_running(e),
            "connected": e.session_id in registered_sessions,
        }
        for e in plugins.values()
    ]


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
