"""Base class for reyn-broker plugins.

A plugin is a long-running process that connects to the broker as a regular
MCP session. It registers itself with a ``session_id``, drains its inbox,
and can run periodic work via ``on_poll``.

Implementing a plugin
---------------------
Subclass :class:`BrokerPlugin`, set :attr:`session_id` / :attr:`role`, and
override the lifecycle hooks you need. Call ``asyncio.run(MyPlugin().run())``
in ``main()``.

Minimal example::

    class EchoPlugin(BrokerPlugin):
        session_id = "echo"
        role = "echo back every message"

        async def on_broker_message(self, msg, broker):
            await broker.post(to=msg["from"], message=f"echo: {msg['message']}")

    def main():
        asyncio.run(EchoPlugin().run())

See ``plugins/ci_watcher.py`` and ``plugins/telegram.py`` for full examples.

Lifecycle
---------
::

    broker connects
        ↓
    on_start(broker)          ← one-time setup
        ↓
    ┌─────────────────────────────────────────┐
    │  inbox drain loop (every inbox_interval) │  → on_broker_message per message
    │  poll loop       (every poll_interval)   │  → on_poll
    └─────────────────────────────────────────┘
        ↓ (on broker disconnect)
    reconnect after reconnect_seconds, restart from on_start
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

logger = logging.getLogger("broker.plugin")

_BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8765/mcp")


def _parse_result(result: Any) -> Any:
    """Extract structured content from a FastMCP ``CallToolResult``."""
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


class BrokerClient:
    """The broker interface passed to every plugin lifecycle hook.

    Wraps the raw MCP ``ClientSession`` so plugin authors work with
    broker concepts directly, without knowing MCP internals.

    ``from_session`` is always set to the plugin's own ``session_id``
    automatically — you never need to pass it.

    Polling control
    ---------------
    Call :meth:`start_poll` / :meth:`stop_poll` from inside any hook to
    dynamically enable or disable the ``on_poll`` callback.  For example,
    a CI-watcher plugin can start polling only when there are watched PRs
    and stop when the watch list is empty.
    """

    def __init__(
        self,
        cs: ClientSession,
        session_id: str,
        poll_handle: _PollHandle,
    ) -> None:
        self._cs = cs
        self._session_id = session_id
        self._poll = poll_handle

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    async def post(self, to: str, message: str, **kwargs: Any) -> str:
        """Send a message to one session. Returns broker status string."""
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
        """Return registered sessions. ``compact=True`` returns id+role only."""
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
        """Enable ``on_poll`` and set its call interval (seconds).

        Safe to call even if polling is already active — updates the interval.
        """
        self._poll.start(interval)

    def stop_poll(self) -> None:
        """Disable ``on_poll`` until :meth:`start_poll` is called again."""
        self._poll.stop()


class _PollHandle:
    """Internal: manages the on_poll timer task."""

    def __init__(self) -> None:
        self._interval: float | None = None
        self._active = asyncio.Event()

    def start(self, interval: float) -> None:
        self._interval = interval
        self._active.set()

    def stop(self) -> None:
        self._active.clear()

    @property
    def interval(self) -> float:
        return self._interval or 60.0

    @property
    def running(self) -> bool:
        return self._active.is_set()

    async def wait_active(self) -> None:
        await self._active.wait()


class BrokerPlugin:
    """Abstract base class for reyn-broker plugins.

    Subclasses MUST set :attr:`session_id` and :attr:`role`.

    Override lifecycle hooks as needed:

    - :meth:`on_start` — called once after broker connection established.
    - :meth:`on_broker_message` — called for each message in the inbox.
    - :meth:`on_poll` — called periodically (enable via ``broker.start_poll(N)``
      or by setting :attr:`poll_interval`).

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
        If set, ``on_poll`` starts automatically at this interval on startup.
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
    # Override in subclass
    # ------------------------------------------------------------------

    async def on_start(self, broker: BrokerClient) -> None:
        """Called once after broker connection and registration.
        Use for one-time setup (send a hello message, load initial state, etc.)."""

    async def on_broker_message(self, msg: dict[str, Any], broker: BrokerClient) -> None:
        """Called for each message drained from this plugin's broker inbox.

        ``msg`` contains at minimum ``"from"`` and ``"message"`` keys.
        """

    async def on_poll(self, broker: BrokerClient) -> None:
        """Called periodically while polling is active.

        Enable polling by calling ``broker.start_poll(interval_seconds)`` in
        ``on_start``, or by setting the class attribute ``poll_interval``.

        Do NOT loop inside this method — the base class calls it repeatedly.
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
                    logger.info("[%s] registered on broker", self.session_id)

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
                            await self.on_broker_message(msg, broker)
                        except Exception as exc:
                            logger.warning(
                                "[%s] on_broker_message error: %s", self.session_id, exc
                            )
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
