"""Base class for reyn-broker plugins.

A plugin is a long-running process that connects to the broker as a regular
MCP session. It registers itself with a ``session_id``, drains its inbox,
and can run periodic work via ``on_poll``.

Implementing a plugin
---------------------
Subclass :class:`BrokerPlugin`, set :attr:`session_id` / :attr:`role`, and
declare commands with the :func:`command` decorator. Call
``asyncio.run(MyPlugin().run())`` in ``main()``.

Example::

    class EchoPlugin(BrokerPlugin):
        session_id = "echo"
        role = "echo back every message"

        @command(description="Echo a message back to the sender")
        async def echo(self, text: str, broker: BrokerClient) -> str:
            return f"echo: {text}"

    def main():
        asyncio.run(EchoPlugin().run())

Commands are invoked by other sessions via ``post_message``::

    post_message(to="echo", from_session="alice", message="echo:hello world")
    # → alice receives: "echo: hello world"

The broker stores the plugin's command schema at registration time.
Other sessions can discover it via the ``get_plugin_commands`` broker tool.

Lifecycle
---------
::

    broker connects
        ↓
    on_start(broker)           ← one-time setup
        ↓
    ┌──────────────────────────────────────────────┐
    │  inbox drain (every inbox_interval seconds)   │
    │    → routes to @command handlers              │
    │    → falls through to on_broker_message       │
    │  poll loop  (every N seconds, if enabled)     │
    │    → on_poll(broker)                          │
    └──────────────────────────────────────────────┘
        ↓ (on broker disconnect)
    reconnect after reconnect_seconds, restart from on_start
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

logger = logging.getLogger("broker.plugin")

_BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8765/mcp")

# Attribute name used to mark command methods
_COMMAND_ATTR = "_broker_command"


# ---------------------------------------------------------------------------
# @command decorator
# ---------------------------------------------------------------------------

@dataclass
class CommandSpec:
    name: str
    description: str
    args: list[str]          # ordered positional arg names (excluding self, broker, sender)
    method_name: str
    has_sender: bool = False  # True when method declares a ``sender: str`` parameter


def command(description: str = "", name: str = "") -> Callable:
    """Decorator to declare a plugin method as a callable broker command.

    The decorated method must have the signature::

        async def my_command(self, arg1: str, ..., broker: BrokerClient) -> str:
            ...

    To receive the caller's session id, add an optional ``sender`` parameter::

        async def my_command(self, arg1: str, broker: BrokerClient, sender: str = "") -> str:
            ...

    ``sender`` is injected by the framework and is NOT treated as a
    user-supplied argument (it does not appear in ``args`` or the command
    schema seen by callers).

    All other positional parameters (except ``self``, ``broker``, ``sender``)
    are extracted from the incoming message text.

    Message format expected by callers::

        post_message(to="<plugin>", ..., message="command_name:arg1 arg2 ...")

    The method's return value (a string) is automatically posted back to the
    sender.  Return ``None`` or ``""`` to suppress the auto-reply.

    Parameters
    ----------
    description : str
        Short description shown in ``get_plugin_commands`` output.
    name : str
        Override the command name (defaults to the method name).
    """
    def decorator(fn: Callable) -> Callable:
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
        has_sender = "sender" in params
        # positional args = everything except self, broker, sender
        arg_names = [p for p in params if p not in ("self", "broker", "sender")]
        cmd_name = name or fn.__name__
        setattr(fn, _COMMAND_ATTR, CommandSpec(
            name=cmd_name,
            description=description,
            args=arg_names,
            method_name=fn.__name__,
            has_sender=has_sender,
        ))
        return fn
    return decorator


# ---------------------------------------------------------------------------
# BrokerClient
# ---------------------------------------------------------------------------

class BrokerClient:
    """The broker interface passed to every plugin lifecycle hook.

    Wraps the raw MCP ``ClientSession`` so plugin authors interact with
    broker concepts directly — no MCP internals, no ``from_session`` boilerplate.

    Polling control
    ---------------
    Call :meth:`start_poll` / :meth:`stop_poll` to dynamically enable or
    disable the ``on_poll`` callback at runtime.
    """

    def __init__(self, cs: ClientSession, session_id: str, poll_handle: _PollHandle) -> None:
        self._cs = cs
        self._session_id = session_id
        self._poll = poll_handle

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    async def get_status(self, session_id: str) -> dict[str, Any]:
        """Return the authoritative status of a session from the broker registry.

        Use this before acting on a delayed state check to guard against
        missed events that could leave local state stale.
        """
        result = await self._cs.call_tool("get_session_status", {"session_id": session_id})
        return _parse_result(result) or {}

    async def subscribe_events(
        self,
        event_types: list[str],
        session_filter: list[str] | None = None,
    ) -> str:
        """Subscribe this plugin to session lifecycle events via its inbox.

        Events are delivered as broker messages with an ``event`` field::

            {"from": "broker", "event": "posted", "session_id": "...", "at": "..."}

        Handle them in :meth:`on_broker_message` by checking ``msg.get("event")``.

        Parameters
        ----------
        event_types : list[str]
            Any subset of ``["registered", "unregistered", "posted"]``.
        session_filter : list[str] | None
            Limit events to these session ids.  ``None`` = all sessions.
        """
        args: dict[str, Any] = {
            "subscriber_id": self._session_id,
            "event_types": event_types,
        }
        if session_filter is not None:
            args["session_filter"] = session_filter
        result = await self._cs.call_tool("subscribe_session_events", args)
        return str(_parse_result(result))

    async def post(self, to: str, message: str, **kwargs: Any) -> str:
        """Send a message to one session. ``from_session`` is set automatically."""
        result = await self._cs.call_tool("post_message", {
            "to": to,
            "from_session": self._session_id,
            "message": message,
            **kwargs,
        })
        return str(_parse_result(result))

    async def broadcast(self, message: str, **kwargs: Any) -> str:
        """Send a message to all registered sessions."""
        result = await self._cs.call_tool("broadcast_message", {
            "from_session": self._session_id,
            "message": message,
            **kwargs,
        })
        return str(_parse_result(result))

    async def list_sessions(self, compact: bool = True) -> list[dict[str, Any]]:
        """Return registered sessions. ``compact=True`` returns id + role only."""
        result = await self._cs.call_tool("list_sessions", {"compact": compact})
        return _parse_result(result) or []

    async def peek(self, limit: int = 10) -> list[dict[str, Any]]:
        """Preview inbox messages without draining them."""
        result = await self._cs.call_tool(
            "peek_messages",
            {"session_id": self._session_id, "limit": limit, "fields": ["from", "message"]},
        )
        return _parse_result(result) or []

    # ------------------------------------------------------------------
    # Poll control
    # ------------------------------------------------------------------

    def start_poll(self, interval: float) -> None:
        """Enable ``on_poll`` and set its call interval in seconds.

        Safe to call even if polling is already active — updates the interval.
        """
        self._poll.start(interval)

    def stop_poll(self) -> None:
        """Disable ``on_poll`` until :meth:`start_poll` is called again."""
        self._poll.stop()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_result(result: Any) -> Any:
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and "result" in structured:
        return structured["result"]
    parts: list[Any] = []
    for block in getattr(result, "content", []) or []:
        text = getattr(block, "text", None)
        if text:
            try:
                parts.append(json.loads(text))
            except json.JSONDecodeError:
                parts.append(text)
    return parts[0] if len(parts) == 1 else parts


class _PollHandle:
    def __init__(self) -> None:
        self._interval: float = 60.0
        self._active = asyncio.Event()

    def start(self, interval: float) -> None:
        self._interval = interval
        self._active.set()

    def stop(self) -> None:
        self._active.clear()

    @property
    def interval(self) -> float:
        return self._interval

    @property
    def running(self) -> bool:
        return self._active.is_set()

    async def wait_active(self) -> None:
        await self._active.wait()


# ---------------------------------------------------------------------------
# BrokerPlugin
# ---------------------------------------------------------------------------

class BrokerPlugin:
    """Abstract base class for reyn-broker plugins.

    Subclasses MUST set :attr:`session_id` and :attr:`role`.

    Declare commands with the :func:`command` decorator. The base class
    routes incoming messages automatically and registers the command schema
    with the broker so other sessions can discover it via
    ``get_plugin_commands``.

    Override lifecycle hooks as needed:

    - :meth:`on_start` — one-time setup after connection.
    - :meth:`on_broker_message` — fallback for messages that don't match
      any declared command.
    - :meth:`on_poll` — periodic work (enable via ``broker.start_poll(N)``).

    Attributes
    ----------
    session_id : str
        Broker session id this plugin registers as.
    role : str
        Short description shown in ``list_sessions``.
    broker_url : str
        Override to connect to a non-default broker.
    inbox_interval : float
        Seconds between inbox drain calls (default 30).
    poll_interval : float | None
        If set, ``on_poll`` starts automatically at this interval.
        ``None`` means polling is off until ``broker.start_poll(N)`` is called.
    reconnect_seconds : float
        Wait after a connection failure before reconnecting (default 10).
    """

    session_id: str = ""
    role: str = ""
    broker_url: str = _BROKER_URL
    inbox_interval: float = 30
    poll_interval: float | None = None
    reconnect_seconds: float = 10

    # ------------------------------------------------------------------
    # Command introspection
    # ------------------------------------------------------------------

    @classmethod
    def _command_specs(cls) -> list[CommandSpec]:
        """Return all CommandSpec objects declared on this class."""
        seen: dict[str, CommandSpec] = {}
        for klass in reversed(cls.__mro__):
            for attr in vars(klass).values():
                spec = getattr(attr, _COMMAND_ATTR, None)
                if isinstance(spec, CommandSpec):
                    seen[spec.name] = spec
        return list(seen.values())

    def commands(self) -> list[dict[str, Any]]:
        """Return the plugin's command schema as a list of dicts.

        Each entry has ``name``, ``description``, and ``args`` (list of
        positional argument names).  This is the format returned by the
        ``get_plugin_commands`` broker tool.
        """
        return [
            {"name": s.name, "description": s.description, "args": s.args}
            for s in self._command_specs()
        ]

    # ------------------------------------------------------------------
    # Override in subclass
    # ------------------------------------------------------------------

    async def on_start(self, broker: BrokerClient) -> None:
        """Called once after broker connection and session registration."""

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        """Fallback handler for messages that do not match any @command.

        Override to handle free-form messages or implement a custom help
        response for unknown commands.
        """

    async def on_poll(self, broker: BrokerClient) -> None:
        """Called periodically while polling is active.

        Do NOT loop inside — the base class calls this repeatedly.
        Enable polling via ``broker.start_poll(interval)`` or set
        the class attribute ``poll_interval``.
        """

    # ------------------------------------------------------------------
    # Infrastructure
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect to broker, register, and run the lifecycle loops."""
        if not self.session_id:
            raise ValueError(f"{type(self).__name__}.session_id must be set")

        while True:
            try:
                async with (
                    streamable_http_client(self.broker_url) as (read, write, _close),
                    ClientSession(read, write) as cs,
                ):
                    await cs.initialize()
                    await cs.call_tool("register_session", {
                        "session_id": self.session_id,
                        "working_dir": os.getcwd(),
                        "role": self.role,
                    })
                    # Register command schema with broker for discoverability
                    schema = self.commands()
                    if schema:
                        await cs.call_tool("register_plugin_commands", {
                            "session_id": self.session_id,
                            "commands": schema,
                        })
                    logger.info(
                        "[%s] registered on broker (%d commands)", self.session_id, len(schema)
                    )

                    poll_handle = _PollHandle()
                    if self.poll_interval is not None:
                        poll_handle.start(self.poll_interval)

                    broker = BrokerClient(cs, self.session_id, poll_handle)
                    await self.on_start(broker)

                    await asyncio.gather(
                        self._inbox_loop(broker, cs),
                        self._poll_loop(broker, poll_handle),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[%s] broker error: %s; reconnecting in %ss",
                    self.session_id, exc, self.reconnect_seconds,
                )
                await asyncio.sleep(self.reconnect_seconds)

    async def _dispatch(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        """Route an incoming message to a @command handler or on_broker_message."""
        text = msg.get("message", "").strip()
        sender = msg.get("from", "")

        # Try to match "command_name:args" or "command_name args"
        match = re.match(r"^(\w[\w-]*)[:]\s*(.*)", text, re.DOTALL) or \
                re.match(r"^(\w[\w-]*)\s+(.*)", text, re.DOTALL) or \
                re.match(r"^(\w[\w-]*)$", text)

        cmd_name = match.group(1) if match else None
        rest = (match.group(2).strip() if match and match.lastindex >= 2 else "")

        for spec in self._command_specs():
            if spec.name != cmd_name:
                continue
            method = getattr(self, spec.method_name)
            # Split rest into positional args; fill missing with ""
            raw_args = rest.split(None, len(spec.args) - 1) if rest else []
            raw_args += [""] * (len(spec.args) - len(raw_args))
            call_args = raw_args[:len(spec.args)]
            try:
                kwargs: dict[str, Any] = {"broker": broker}
                if spec.has_sender:
                    kwargs["sender"] = sender
                reply = await method(*call_args, **kwargs)
                if reply and sender:
                    await broker.post(to=sender, message=str(reply))
            except Exception as exc:
                logger.warning("[%s] command '%s' error: %s", self.session_id, cmd_name, exc)
                if sender:
                    await broker.post(to=sender, message=f"error in '{cmd_name}': {exc}")
            return

        # No command matched — fall through to on_broker_message
        await self.on_broker_message(msg, broker)

    async def _inbox_loop(self, broker: BrokerClient, cs: ClientSession) -> None:
        while True:
            await asyncio.sleep(self.inbox_interval)
            try:
                result = await cs.call_tool(
                    "receive_messages",
                    {"session_id": self.session_id, "fields": ["from", "message"]},
                )
                msgs = _parse_result(result)
                if isinstance(msgs, list):
                    for msg in msgs:
                        try:
                            await self._dispatch(msg, broker)
                        except Exception as exc:
                            logger.warning("[%s] dispatch error: %s", self.session_id, exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[%s] inbox error: %s", self.session_id, exc)
                raise

    async def _poll_loop(self, broker: BrokerClient, handle: _PollHandle) -> None:
        while True:
            await handle.wait_active()
            await asyncio.sleep(handle.interval)
            if not handle.running:
                continue
            try:
                await self.on_poll(broker)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[%s] on_poll error: %s", self.session_id, exc)
