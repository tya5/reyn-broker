#!/usr/bin/env python3
"""Broker MCP server for inter-session messaging.

Each Claude Code session connects to this broker over Streamable HTTP,
registers itself with a session_id, and can post messages to other
registered sessions. Incoming messages are pushed to the target session
via the MCP-standard ``notifications/message`` (logging) notification.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import shlex
import signal
import time
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:  # mcp 2.0+
    from mcp.server.mcpserver import Context, MCPServer as FastMCP
except ModuleNotFoundError:  # mcp 1.x
    from mcp.server.fastmcp import Context, FastMCP  # type: ignore[assignment,no-redef]
from mcp.server.session import ServerSession
from pydantic import AnyUrl

logger = logging.getLogger("broker")

_VERSION = "0.16.0"
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
    # Two orthogonal axes (see set_active / update_session_status):
    active: bool = True                     # mechanical liveness (hook-driven); True = working
    status: str | None = None               # semantic status (LLM-driven), e.g. "waiting"
    status_detail: str | None = None        # optional free-form status detail


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
    event_types: set[str]  # "registered"|"unregistered"|"posted"|"status_changed"
    session_filter: list[str] | None = None    # None = all sessions


sessions: dict[str, SessionEntry] = {}
pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
plugins: dict[str, PluginEntry] = {}
# subscriber_id → list of independent subscriptions. A session may hold several
# subscriptions with different event_types / session_filter; an event is
# delivered at most once per subscriber even if multiple of its subs match.
event_subscriptions: dict[str, list[EventSubscription]] = {}
# session_id → list of command dicts ({name, description, args})
plugin_commands: dict[str, list[dict[str, Any]]] = {}
# session_id (parsed out of broker://inbox/<id>) → live ServerSessions that
# subscribed to that inbox resource. Deliberately not persisted: a subscription
# is a property of an open connection, so after a restart there is nothing to
# restore — clients re-subscribe on reconnect and re-read to catch up.
resource_subscribers: dict[str, list[ServerSession]] = defaultdict(list)
registry_lock = asyncio.Lock()
# Live asyncio subprocess handles (not persisted — lost on broker restart)
_plugin_procs: dict[str, Any] = {}

_DEFAULT_STATE_PATH = Path.home() / ".local" / "state" / "reyn-broker" / "state.json"
_STATE_PATH = Path(os.environ.get("BROKER_STATE_FILE", _DEFAULT_STATE_PATH))
_PLUGIN_LOG_DIR = _STATE_PATH.parent / "plugins"


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
                _forget_session_locked(sid)
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
        _PLUGIN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = _PLUGIN_LOG_DIR / f"{entry.name}.log"
        log_fh = log_path.open("a")
        proc = await asyncio.create_subprocess_exec(
            *args,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=log_fh,
        )
        _plugin_procs[entry.name] = proc
        entry.pid = proc.pid
        logger.info("plugin '%s' started (pid %d, log %s)", entry.name, proc.pid, log_path)
        return True
    except Exception as exc:
        logger.warning("failed to start plugin '%s': %s", entry.name, exc)
        return False


async def _terminate_plugin(entry: PluginEntry) -> None:
    """Send SIGTERM then SIGKILL to a plugin process."""
    proc = _plugin_procs.pop(entry.name, None)
    if proc is not None and proc.returncode is None:
        with suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("plugin '%s' did not exit after SIGTERM, sending SIGKILL", entry.name)
            with suppress(ProcessLookupError):
                proc.kill()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=2)
    elif entry.pid is not None:
        with suppress(ProcessLookupError, OSError):
            os.kill(entry.pid, signal.SIGTERM)
    entry.pid = None


async def _plugin_supervisor() -> None:
    """Restart auto_start plugins that have crashed (checked every 10 s)."""
    while True:
        await asyncio.sleep(10)
        for entry in list(plugins.values()):
            if not entry.auto_start:
                continue
            if _plugin_is_running(entry):
                continue
            proc = _plugin_procs.get(entry.name)
            if proc is not None and proc.returncode is not None:
                logger.warning(
                    "plugin '%s' exited (rc %d), restarting", entry.name, proc.returncode
                )
                _plugin_procs.pop(entry.name, None)
                await _launch_plugin(entry)


_bg_started = False


@asynccontextmanager
async def _lifespan(app: Any):
    # IMPORTANT: FastMCP / the Streamable-HTTP session manager may enter and
    # exit this lifespan context MORE THAN ONCE during a single broker process
    # (observed: a fresh enter/exit cycle every few minutes). We must therefore:
    #   1. start background tasks + auto-launch plugins exactly ONCE, and
    #   2. NOT terminate plugins on context exit — doing so killed every plugin
    #      on each spurious cycle (~6 min), which silently broke stall/idle
    #      detection.
    # Plugin processes are cleaned up via the atexit hook on real process exit.
    global _bg_started
    if not _bg_started:
        _bg_started = True
        asyncio.create_task(_background_purge())
        for entry in list(plugins.values()):
            if entry.auto_start and not _plugin_is_running(entry):
                await _launch_plugin(entry)
        asyncio.create_task(_plugin_supervisor())
    yield


def _terminate_all_plugins_atexit() -> None:
    """Best-effort SIGTERM to every plugin process when the broker exits.

    Runs synchronously at interpreter shutdown (the asyncio loop is gone by
    then, so we cannot use _terminate_plugin). Plugins handle SIGTERM by
    exiting their asyncio.run, so this is a clean stop.
    """
    for entry in list(plugins.values()):
        if entry.pid:
            with suppress(ProcessLookupError, OSError):
                os.kill(entry.pid, signal.SIGTERM)


atexit.register(_terminate_all_plugins_atexit)


mcp = FastMCP("broker", lifespan=_lifespan)


def _lowlevel_server() -> Any:
    """Return the SDK's low-level server object behind ``mcp``.

    Both names are private, and the rename between them is exactly the kind
    of break this indirection exists to localise: ``_mcp_server`` (1.x)
    became ``_lowlevel_server`` (2.0), and the old name failed at import
    time rather than degrading. There is no public accessor in either
    version, so the dependency cannot be removed — only kept in one place
    so the next rename is a one-line fix instead of a scavenger hunt.
    """
    for name in ("_lowlevel_server", "_mcp_server"):
        server = getattr(mcp, name, None)
        if server is not None:
            return server
    raise RuntimeError(
        "MCP SDK exposes neither _lowlevel_server nor _mcp_server — the "
        "private accessor was renamed again; find the lowlevel Server on "
        "the FastMCP/MCPServer instance and add its name above."
    )


def _advertise_resource_subscribe() -> None:
    """Advertise ``resources.subscribe: true`` in the initialize response.

    mcp 1.x hardcodes ``subscribe=False`` when building
    ``ResourcesCapability`` (``lowlevel/server.py: get_capabilities``), and
    ``NotificationOptions`` exposes no knob for it — only ``*_changed``
    flags. Registering a ``SubscribeRequest`` handler does not change the
    advertisement either. So a server can honour subscriptions while
    telling every client it cannot, and a client that trusts the
    advertisement will never subscribe.

    reyn refuses to connect when the capability is absent rather than
    silently degrading (its ``mcp/client.py`` requires it), so without this
    the whole feature fails at the handshake. We wrap ``get_capabilities``
    rather than rebuilding the capability set, so anything else the SDK
    decides (tools, prompts, listChanged) keeps flowing through unchanged.

    On mcp 2.0 the SDK derives the flag from whether a
    ``resources/subscribe`` handler is registered, so this wrapper finds
    it already true and leaves it alone. Kept (rather than deleted) only
    because the floor is still ``mcp>=1.27``; drop it when that moves
    past 1.x.
    """
    server = _lowlevel_server()
    inner = server.get_capabilities

    def get_capabilities(*args: Any, **kwargs: Any) -> Any:
        caps = inner(*args, **kwargs)
        resources = caps.resources
        if resources is not None and not resources.subscribe:
            resources.subscribe = True
        return caps

    server.get_capabilities = get_capabilities  # type: ignore[method-assign]


_INBOX_URI_PREFIX = "broker://inbox/"


def _session_id_from_inbox_uri(uri: Any) -> str | None:
    """Return the session id in ``broker://inbox/<id>``, or None if not one."""
    text = str(uri)
    if not text.startswith(_INBOX_URI_PREFIX):
        return None
    return text[len(_INBOX_URI_PREFIX) :].strip("/") or None


async def _add_inbox_subscriber(uri: Any, session: ServerSession) -> None:
    """Record ``session`` as a subscriber of the inbox named by ``uri``."""
    sid = _session_id_from_inbox_uri(uri)
    if sid is None:
        logger.warning("ignoring subscribe to unknown resource: %s", uri)
        return
    async with registry_lock:
        subs = resource_subscribers[sid]
        if session not in subs:
            subs.append(session)
        count = len(subs)
    logger.info("resource subscribe: %s (subscribers=%d)", uri, count)


async def _drop_inbox_subscriber(uri: Any, session: ServerSession) -> None:
    """Forget ``session`` as a subscriber of the inbox named by ``uri``."""
    sid = _session_id_from_inbox_uri(uri)
    if sid is None:
        return
    async with registry_lock:
        subs = resource_subscribers.get(sid)
        if subs is not None:
            if session in subs:
                subs.remove(session)
            if not subs:
                resource_subscribers.pop(sid, None)
    logger.info("resource unsubscribe: %s", uri)


def _register_resource_subscription_handlers() -> None:
    """Honour ``resources/subscribe`` for ``broker://inbox/<session_id>``.

    Neither FastMCP (1.x) nor MCPServer (2.0) serves these by default, so
    without them a subscribe request is answered with "method not found"
    even though we advertise the capability.

    ``resources/subscribe`` is how pre-2026-07-28 clients ask to be woken,
    and every live watcher is one of those today — measured, not assumed:
    all 7 run ``session_watcher.py`` under *this* repo's venv, so they
    speak whatever mcp version the broker pins, regardless of what the
    peer's own venv holds (several peers are already on 2.0). Upgrading
    the pin therefore moves every watcher at once, which is also why this
    handler cannot be dropped on the strength of "the peers are modern".

    mcp 2.0 still routes the method for exactly this reason, and serves
    ``subscriptions/listen`` alongside it for newer clients (which we do
    not have to implement: the SDK does it).

    The two SDKs register handlers differently, and neither exposes a
    public way to do it after construction:

    - 1.x: ``@server.subscribe_resource()`` decorator
    - 2.0: decorator removed; handlers are ``Server(...)`` kwargs, kept in
      ``_request_handlers`` as ``HandlerEntry(params_type, handler)``

    We already hold the instance, so on 2.0 we install into that table
    directly. Both paths end at the same two functions above.
    """
    server = _lowlevel_server()

    if hasattr(server, "subscribe_resource"):  # mcp 1.x
        @server.subscribe_resource()
        async def _subscribe(uri: Any) -> None:
            await _add_inbox_subscriber(uri, server.request_context.session)

        @server.unsubscribe_resource()
        async def _unsubscribe(uri: Any) -> None:
            await _drop_inbox_subscriber(uri, server.request_context.session)
        return

    # mcp 2.0+
    import mcp.types as _types
    from mcp.server.lowlevel.server import HandlerEntry

    async def _on_subscribe(ctx: Any, params: Any) -> Any:
        await _add_inbox_subscriber(params.uri, ctx.session)
        return _types.EmptyResult()

    async def _on_unsubscribe(ctx: Any, params: Any) -> Any:
        await _drop_inbox_subscriber(params.uri, ctx.session)
        return _types.EmptyResult()

    server._request_handlers.update(
        {
            "resources/subscribe": HandlerEntry(
                _types.SubscribeRequestParams, _on_subscribe
            ),
            "resources/unsubscribe": HandlerEntry(
                _types.UnsubscribeRequestParams, _on_unsubscribe
            ),
        }
    )


async def _notify_inbox_updated(session_id: str) -> None:
    """Tell subscribers that ``session_id``'s inbox resource changed.

    Best-effort: a subscriber whose connection has gone is dropped rather
    than allowed to break delivery for the others. Because the resource
    read is non-destructive, a client that misses this notification still
    finds the message waiting — wake-ups are at-least-once, not
    exactly-once.
    """
    async with registry_lock:
        subs = list(resource_subscribers.get(session_id, ()))
    if not subs:
        return
    uri = AnyUrl(f"{_INBOX_URI_PREFIX}{session_id}")
    dead: list[ServerSession] = []
    for session in subs:
        try:
            await session.send_resource_updated(uri)
        except Exception as exc:
            logger.warning("resource-updated for %s failed: %s", session_id, exc)
            dead.append(session)
    if not dead:
        return
    async with registry_lock:
        subs_now = resource_subscribers.get(session_id)
        if subs_now is None:
            return
        for session in dead:
            if session in subs_now:
                subs_now.remove(session)
        if not subs_now:
            resource_subscribers.pop(session_id, None)


_advertise_resource_subscribe()
_register_resource_subscription_handlers()


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
                    "active": e.active,
                    "status": e.status,
                    "status_detail": e.status_detail,
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
                for subs in event_subscriptions.values()
                for s in subs
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
            active=entry.get("active", True),
            status=entry.get("status"),
            status_detail=entry.get("status_detail"),
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
        event_subscriptions.setdefault(sid, []).append(EventSubscription(
            subscriber_id=sid,
            event_types=set(sub.get("event_types", [])),
            session_filter=sub.get("session_filter"),
        ))
    logger.info(
        "restored state: %d sessions, %d queued messages, %d plugins, %d event subs",
        len(sessions),
        sum(len(v) for v in pending.values()),
        len(plugins),
        sum(len(v) for v in event_subscriptions.values()),
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


def _forget_session_locked(session_id: str) -> None:
    """Drop a gone session's event subscriptions and command schema.

    Caller MUST hold ``registry_lock``. Without this, unregistering (or TTL-
    expiring) a session left its ``event_subscriptions`` / ``plugin_commands``
    entries behind as ghosts — they kept matching events and surfacing in
    ``list_plugin_commands`` for a session that no longer exists.
    """
    event_subscriptions.pop(session_id, None)
    plugin_commands.pop(session_id, None)


def _push_session_event_locked(
    event_type: str,
    session_id: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """Queue a session activity event to all matching subscribers.

    Caller MUST hold ``registry_lock``. The event is queued as a regular
    broker message so subscribers receive it via their normal inbox drain.
    Events are never delivered to the session that triggered them.

    ``extra`` is merged into the event payload (used for ``status_changed``
    to carry ``status`` and ``detail`` fields).
    """
    now = _now_iso()
    payload: dict[str, Any] = {"from": "broker", "event": event_type,
                                "session_id": session_id, "at": now}
    if extra:
        payload.update(extra)
    for subscriber_id, subs in event_subscriptions.items():
        if subscriber_id == session_id:
            continue
        # Deliver at most once per subscriber, even if several of its
        # subscriptions match this event.
        for sub in subs:
            if event_type not in sub.event_types:
                continue
            if sub.session_filter is not None and session_id not in sub.session_filter:
                continue
            pending[subscriber_id].append(payload)
            break


def _register_locked(
    session_id: str,
    working_dir: str,
    mcp_session: Any,
    role: str | None,
    ttl_hours: float | None,
    commands: list[dict[str, Any]] | None = None,
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
    if commands is not None:
        plugin_commands[session_id] = commands
    backlog = _drain_inbox_locked(session_id)
    _push_session_event_locked("registered", session_id)
    return f"registered '{session_id}' at {working_dir}", backlog


def _session_list_locked(compact: bool) -> list[dict[str, Any]]:
    """Serialise the session registry.  Caller MUST hold ``registry_lock``."""
    if compact:
        return [
            {"session_id": e.session_id, "role": e.role,
             "active": e.active, "status": e.status}
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
            "active": e.active,
            "status": e.status,
            "status_detail": e.status_detail,
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
    commands: list[dict[str, Any]] | None = None,
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

    ``commands`` — optional list of command dicts (each with ``name``,
    ``description``, ``args``) to register alongside the session.  Plugins
    pass their command schema here so peers can discover it via
    ``list_plugin_commands`` without a separate round-trip.

    Returns any messages that were queued while this session was offline.
    Prefer ``startup_summary`` at startup to combine this call with
    ``list_sessions`` in one round-trip.
    """
    _tool_call_counts["register_session"] += 1
    async with registry_lock:
        status, backlog = _register_locked(
            session_id, working_dir, ctx.session, role, ttl_hours, commands
        )
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
    commands: list[dict[str, Any]] | None = None,
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
        status, backlog = _register_locked(
            session_id, working_dir, ctx.session, role, ttl_hours, commands
        )
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
            _forget_session_locked(session_id)
            _push_session_event_locked("unregistered", session_id)
            _save_state()
    if existed is None:
        return f"'{session_id}' was not registered"
    return f"unregistered '{session_id}'"


@mcp.tool()
async def set_active(session_id: str, active: bool) -> str:
    """Set a session's mechanical liveness flag (the DETERMINISTIC axis).

    This axis is meant to be driven by Claude Code hooks at zero LLM cost:
    - work-start hook (UserPromptSubmit / PreToolUse) → ``set_active(id, True)``
    - Stop hook                                       → ``set_active(id, False)``

    It is orthogonal to ``update_session_status`` (the semantic axis: *what*
    the session is waiting for). ``set_active`` never touches ``status`` /
    ``detail``, so a mechanical idle from a Stop hook cannot clobber an
    LLM-declared waiting reason — and vice versa. This is the authoritative
    signal monitors should use for stall/idle detection; ``status`` is
    best-effort enrichment.

    Fires an ``active_changed`` event (carrying ``active``, ``prev_active``,
    and the current ``status`` / ``detail`` for enrichment) only when the
    value actually changes — so a PreToolUse hook firing ``True`` repeatedly
    is a no-op and never spams subscribers.

    Hooks should use the ``reyn-broker-active`` CLI (no MCP round-trip needed).
    """
    _tool_call_counts["set_active"] += 1
    async with registry_lock:
        entry = sessions.get(session_id)
        if entry is None:
            return f"'{session_id}' is not registered"
        if entry.active == active:
            return f"active unchanged: '{session_id}' already active={active}"
        prev_active = entry.active
        entry.active = active
        _push_session_event_locked(
            "active_changed", session_id,
            extra={"active": active, "prev_active": prev_active,
                   "status": entry.status, "detail": entry.status_detail},
        )
        _save_state()
    return f"active updated: '{session_id}' → {active}"


@mcp.tool()
async def update_session_status(
    session_id: str,
    status: str,
    detail: str | None = None,
) -> str:
    """Report this session's SEMANTIC status (the best-effort axis).

    This is the LLM-driven axis describing *what* the session is doing or
    waiting for (e.g. ``"waiting"`` + ``detail="ci:#1268"``). It is orthogonal
    to ``set_active`` (the mechanical liveness bool): this call never touches
    ``active``, so it cannot be clobbered by a Stop hook's ``set_active(False)``.

    Monitors treat ``status`` as enrichment, not authority — stall/idle
    detection keys off the ``active`` bool. Setting ``status`` lets monitors
    show *why* a session is idle/blocked, but is never required.

    Recommended status values (any string is accepted):
    - ``"waiting"`` — blocked on an external event (describe in ``detail``).
    - ``"idle"``    — nothing to do (usually the active bool already conveys this).

    The broker fires a ``status_changed`` event to any session subscribed
    via ``subscribe_session_events``.  The event carries the new ``status``,
    ``detail``, and the ``prev_status`` (the value before this call), so
    subscribers can detect edges (e.g. active→idle) rather than re-firing on
    every detail-only update. The updated status is also visible in
    ``list_sessions``.

    Callers that cannot make MCP calls (e.g. stop hooks) can use the
    ``reyn-broker-status`` CLI instead::

        reyn-broker-status SESSION_ID STATUS [DETAIL]
    """
    _tool_call_counts["update_session_status"] += 1
    async with registry_lock:
        entry = sessions.get(session_id)
        if entry is None:
            return f"'{session_id}' is not registered"
        if entry.status == status and entry.status_detail == detail:
            return f"status unchanged: '{session_id}' already {status}"
        prev_status = entry.status
        entry.status = status
        entry.status_detail = detail
        _push_session_event_locked(
            "status_changed", session_id,
            extra={"status": status, "detail": detail, "prev_status": prev_status},
        )
        _save_state()
    return f"status updated: '{session_id}' → {status}"


@mcp.tool()
async def get_session_status(session_id: str) -> dict[str, Any]:
    """Return the current status of a single session.

    Provides an authoritative snapshot from the broker registry.  Use this
    to confirm a session's status before acting on it — particularly useful
    in delayed checks where in-memory state may be stale due to missed events.

    Returns a dict with ``session_id``, ``registered`` (bool), ``active``
    (bool — the mechanical liveness axis), ``status`` and ``status_detail``
    (the semantic axis). If the session is not registered, ``registered`` is
    ``False`` and the other fields are ``None``.
    """
    _tool_call_counts["get_session_status"] += 1
    async with registry_lock:
        entry = sessions.get(session_id)
    if entry is None:
        return {"session_id": session_id, "registered": False,
                "active": None, "status": None, "status_detail": None}
    return {
        "session_id": session_id,
        "registered": True,
        "active": entry.active,
        "status": entry.status,
        "status_detail": entry.status_detail,
    }


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
    unregistered: list[str] = []

    async with registry_lock:
        for target in targets:
            pending[target].append(dict(payload))
            if target in sessions:
                (online if sessions[target].active else offline).append(target)
            else:
                # Queued, but nobody will ever drain it unless a session
                # registers under this exact id. Reported separately so the
                # sender can tell a typo'd/unknown id from a real peer that
                # happens to be idle — the two are indistinguishable
                # otherwise (reyn-broker#14).
                unregistered.append(target)
        if from_session in sessions:
            sessions[from_session].last_post_at = sent_at
        if _MONITOR_SID and _MONITOR_SID not in targets and from_session != _MONITOR_SID:
            monitor_to = targets[0] if len(targets) == 1 else targets
            pending[_MONITOR_SID].append({**_strip_internal(payload), "monitor_to": monitor_to})
        _push_session_event_locked("posted", from_session)
        _save_state()

    for target in (*online, *offline):
        push_payload = _strip_internal(payload)
        await _deliver(target, push_payload)

    # Wake resource subscribers regardless of registration: a client can
    # subscribe to an inbox before (or without) registering a session.
    for target in targets:
        await _notify_inbox_updated(target)

    if len(targets) == 1:
        target = targets[0]
        if target in unregistered:
            return (
                f"queued for '{target}' (NOT REGISTERED — no session has ever"
                f" registered this id; check the session_id, it is a different"
                f" namespace from ListAgents names)"
            )
        return f"queued for '{target}' (online={target in online})"
    parts = [
        f"queued for {len(targets)} recipients",
        f" (online: {', '.join(online) or 'none'}",
        f"; offline: {', '.join(offline) or 'none'}",
    ]
    if unregistered:
        parts.append(f"; NOT REGISTERED: {', '.join(unregistered)}")
    parts.append(")")
    return "".join(parts)


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
        await _notify_inbox_updated(sid)

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


@mcp.resource(
    "broker://inbox/{session_id}",
    name="broker-inbox",
    title="Broker inbox",
    description=(
        "Non-destructive view of one session's inbox. Subscribe to this URI to"
        " be woken by notifications/resources/updated when mail arrives, then"
        " drain with the receive_messages tool. Reading never removes messages,"
        " so a missed notification is not a lost message."
    ),
    mime_type="application/json",
)
async def inbox_resource(session_id: str) -> str:
    """Return ``session_id``'s queued messages without draining them.

    One resource per session (rather than one global feed) so a subscriber
    is woken only by its own mail — a single shared URI would wake every
    peer on every message.

    Deliberately non-destructive: reading is what a woken client does
    *before* it decides to act, so a client that misses an ``updated``
    notification still finds the message here — wake-ups are
    at-least-once rather than exactly-once.

    Reading never removes anything. Two things do: ``receive_messages``
    (the intended drain) and the TTL sweep in ``_background_purge``,
    which drops messages posted with ``ttl_seconds`` once they expire.
    The sweep is opt-in per message, so an inbox of ordinary messages is
    only emptied by its owner — but "only receive_messages drains" would
    be false, and a subscriber relying on it could lose expiring mail
    between the wake-up and the read.
    """
    _tool_call_counts["inbox_resource"] += 1
    async with registry_lock:
        msgs = [_strip_internal(m) for m in _purge_expired(pending.get(session_id, []))]
        registered = session_id in sessions
    return json.dumps(
        {
            "session_id": session_id,
            "registered": registered,
            "pending_count": len(msgs),
            "messages": msgs,
        },
        ensure_ascii=False,
    )


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
async def list_plugin_commands(session_id: str) -> list[dict[str, Any]]:
    """Return the command schema for a plugin session.

    Use this to discover what commands a plugin accepts before sending
    it a message.  Returns an empty list if the session has not registered
    any commands (e.g. it is a plain broker session, not a plugin).

    Each entry has:
    - ``name``        — command name used as the message prefix.
    - ``description`` — short description of what the command does.
    - ``args``        — ordered list of positional argument names.

    Example::

        list_plugin_commands("ci-watcher")
        # → [
        #     {"name": "watch",   "args": ["pr_number"], "description": "Watch a PR"},
        #     {"name": "unwatch", "args": ["pr_number"], "description": "Stop watching"},
        #     {"name": "list",    "args": [],            "description": "List watched PRs"},
        #   ]

    To invoke a command, send a message in the format
    ``"<name>:<arg1> <arg2> ..."``:

        post_message(to="ci-watcher", from_session="me", message="watch:#1268")
    """
    _tool_call_counts["list_plugin_commands"] += 1
    return plugin_commands.get(session_id, [])


# ---------------------------------------------------------------------------
# Session event subscription tools
# ---------------------------------------------------------------------------

_VALID_EVENT_TYPES = frozenset(
    {"registered", "unregistered", "posted", "status_changed", "active_changed"}
)


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
      ``broadcast_message``.
    - ``"status_changed"`` — a session called ``update_session_status``
      with a new value (semantic axis). Payload carries ``status``,
      ``detail``, and ``prev_status``.
    - ``"active_changed"`` — a session's mechanical liveness bool flipped
      via ``set_active``. Payload carries ``active``, ``prev_active``, and
      the current ``status`` / ``detail`` for enrichment. This is the
      authoritative edge for stall/idle detection (active False = idle).

    ``session_filter`` — optional list of session ids to watch. ``None``
    (default) means watch all sessions.

    A subscriber may hold multiple independent subscriptions: calling this
    again with a different ``(event_types, session_filter)`` pair adds a
    separate subscription rather than merging. For example you can watch
    ``posted`` for session A and ``status_changed`` for session B without the
    two filters bleeding into each other. An identical pair is not duplicated
    (idempotent). An event is delivered at most once per subscriber even when
    several of its subscriptions match.

    Subscriptions survive broker restarts (persisted to state file).
    Call ``unsubscribe_session_events`` to cancel.
    """
    _tool_call_counts["subscribe_session_events"] += 1
    unknown = set(event_types) - _VALID_EVENT_TYPES
    if unknown:
        return f"unknown event type(s): {sorted(unknown)}; valid: {sorted(_VALID_EVENT_TYPES)}"
    new_sub = EventSubscription(
        subscriber_id=subscriber_id,
        event_types=set(event_types),
        session_filter=session_filter,
    )

    def _filter_key(f: list[str] | None) -> frozenset[str] | None:
        return None if f is None else frozenset(f)

    async with registry_lock:
        subs = event_subscriptions.setdefault(subscriber_id, [])
        # Idempotent: an identical (event_types, session_filter) subscription
        # is not duplicated. Distinct ones are kept as independent entries.
        already = any(
            s.event_types == new_sub.event_types
            and _filter_key(s.session_filter) == _filter_key(new_sub.session_filter)
            for s in subs
        )
        if not already:
            subs.append(new_sub)
        _save_state()
    filter_desc = f" (filter: {session_filter})" if session_filter else " (all sessions)"
    return f"'{subscriber_id}' subscribed to {sorted(event_types)}{filter_desc}"


@mcp.tool()
async def unsubscribe_session_events(
    subscriber_id: str,
    event_types: list[str] | None = None,
) -> str:
    """Unsubscribe from session activity events.

    If ``event_types`` is provided, those event types are removed from ALL of
    this subscriber's subscriptions. If omitted, every subscription for this
    subscriber is cancelled.
    """
    _tool_call_counts["unsubscribe_session_events"] += 1
    async with registry_lock:
        subs = event_subscriptions.get(subscriber_id)
        if not subs:
            return f"'{subscriber_id}' has no active subscription"
        if event_types is None:
            count = len(subs)
            del event_subscriptions[subscriber_id]
            msg = f"'{subscriber_id}' fully unsubscribed ({count} subscription(s) removed)"
        else:
            remove = set(event_types)
            for s in subs:
                s.event_types -= remove
            # drop subscriptions left with no event types
            subs[:] = [s for s in subs if s.event_types]
            if not subs:
                del event_subscriptions[subscriber_id]
                msg = f"'{subscriber_id}' fully unsubscribed (no event types remaining)"
            else:
                remaining = sorted({et for s in subs for et in s.event_types})
                msg = f"'{subscriber_id}' removed {event_types}; still watching {remaining}"
        _save_state()
    return msg


# ---------------------------------------------------------------------------
# Plugin lifecycle tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def add_plugin(
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

    The registration is persisted to the state file. Call ``start_plugin`` to
    launch the process immediately.
    """
    _tool_call_counts["add_plugin"] += 1
    if name in plugins:
        return f"plugin '{name}' already registered; use remove_plugin first to replace"
    plugins[name] = PluginEntry(
        name=name, command=command, session_id=session_id,
        env=env or {}, auto_start=auto_start,
    )
    async with registry_lock:
        _save_state()
    return f"plugin '{name}' registered (auto_start={auto_start})"


@mcp.tool()
async def remove_plugin(name: str) -> str:
    """Stop (if running) and remove a plugin from the registry."""
    _tool_call_counts["remove_plugin"] += 1
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
async def start_plugin(name: str) -> str:
    """Start a registered plugin by spawning its subprocess."""
    _tool_call_counts["start_plugin"] += 1
    entry = plugins.get(name)
    if entry is None:
        return f"plugin '{name}' not registered; call add_plugin first"
    if _plugin_is_running(entry):
        return f"plugin '{name}' is already running (pid {entry.pid})"
    ok = await _launch_plugin(entry)
    async with registry_lock:
        _save_state()
    if ok:
        return f"plugin '{name}' started (pid {entry.pid})"
    return f"plugin '{name}' failed to start"


@mcp.tool()
async def stop_plugin(name: str) -> str:
    """Send SIGTERM to a running plugin process."""
    _tool_call_counts["stop_plugin"] += 1
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
async def restart_plugin(name: str) -> str:
    """Stop and restart a plugin."""
    _tool_call_counts["restart_plugin"] += 1
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
async def list_plugins() -> list[dict[str, Any]]:
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
    _tool_call_counts["list_plugins"] += 1
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
    # Where the bind address lives moved between SDK versions: 1.x reads it
    # off ``settings``, 2.0 dropped those fields and takes host/port as
    # run() kwargs. Passing them to a 1.x run() is not accepted, and
    # assigning to settings on 2.0 silently creates an attribute nobody
    # reads — the server would come up on the default port instead of the
    # one asked for, which looks like "started fine" until nothing can
    # reach it. So branch on which one the installed SDK actually has.
    run_kwargs: dict[str, Any] = {}
    settings = getattr(mcp, "settings", None)
    if settings is not None and hasattr(settings, "host"):  # mcp 1.x
        settings.host = args.host
        settings.port = args.port
    else:  # mcp 2.0+
        run_kwargs = {"host": args.host, "port": args.port}

    logger.info(
        "starting broker on %s:%s (state file: %s)",
        args.host,
        args.port,
        _STATE_PATH,
    )
    _load_state()
    mcp.run(transport="streamable-http", **run_kwargs)


if __name__ == "__main__":
    main()
