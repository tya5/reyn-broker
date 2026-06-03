"""Base class for reyn-broker plugins.

A plugin is a long-running process that connects to the broker as a regular
MCP session. It registers itself with a ``session_id``, drains its inbox
periodically, and can run additional background tasks (e.g. polling an
external API and forwarding results as broker messages).

Implementing a plugin
---------------------
Subclass :class:`BrokerPlugin`, override the three methods below, and call
``asyncio.run(MyPlugin().run())`` in ``main()``.

Minimal example::

    class EchoPlugin(BrokerPlugin):
        session_id = "echo"
        role = "echo back every message"

        async def on_message(self, msg: dict, cs: ClientSession) -> None:
            await cs.call_tool("post_message", {
                "to": msg["from"],
                "from_session": self.session_id,
                "message": f"echo: {msg['message']}",
            })

    def main() -> None:
        asyncio.run(EchoPlugin().run())

See ``plugins/telegram.py`` and ``plugins/ci_watcher.py`` for full examples.
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
_POLL_S = 30
_RECONNECT_S = 10


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


class BrokerPlugin:
    """Abstract base class for reyn-broker plugins.

    Subclasses MUST set :attr:`session_id` and :attr:`role`, and SHOULD
    override :meth:`on_message`.  Override :meth:`run_tasks` to add
    concurrent background coroutines (e.g. polling an external API).
    Override :meth:`on_connected` for one-time setup after registration.

    Attributes
    ----------
    session_id : str
        The broker session id this plugin registers as.
    role : str
        Short description shown in ``list_sessions`` output.
    broker_url : str
        Override to connect to a non-default broker.
    poll_seconds : float
        Interval between inbox drain calls.
    reconnect_seconds : float
        Wait time before reconnecting after a connection failure.
    """

    session_id: str = ""
    role: str = ""
    broker_url: str = _BROKER_URL
    poll_seconds: float = _POLL_S
    reconnect_seconds: float = _RECONNECT_S

    # ------------------------------------------------------------------
    # Override in subclass
    # ------------------------------------------------------------------

    async def on_connected(self, cs: ClientSession) -> None:
        """Called once after the broker connection is established and the
        session is registered. Use for one-time setup (e.g. show a welcome
        message, load initial state)."""

    async def on_message(self, msg: dict[str, Any], cs: ClientSession) -> None:
        """Called for each message drained from this plugin's inbox.

        ``msg`` is a dict with at minimum ``"from"`` and ``"message"`` keys.
        Implement this to react to incoming broker messages.
        """

    async def run_tasks(self, cs: ClientSession) -> list[asyncio.Task]:
        """Return a list of asyncio Tasks to run concurrently with the inbox
        loop. Tasks are cancelled when the broker connection drops.

        Example::

            async def run_tasks(self, cs):
                return [asyncio.create_task(self._poll_external_api(cs))]
        """
        return []

    # ------------------------------------------------------------------
    # Infrastructure (do not override unless you know what you're doing)
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main loop: connect → register → run tasks → reconnect on failure."""
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
                    await self.on_connected(cs)
                    extra = await self.run_tasks(cs)
                    try:
                        await asyncio.gather(self._inbox_loop(cs), *extra)
                    finally:
                        for t in extra:
                            t.cancel()
                        await asyncio.gather(*extra, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[%s] broker error: %s; reconnecting in %ss",
                    self.session_id, exc, self.reconnect_seconds,
                )
                await asyncio.sleep(self.reconnect_seconds)

    async def _inbox_loop(self, cs: ClientSession) -> None:
        while True:
            await asyncio.sleep(self.poll_seconds)
            try:
                result = await cs.call_tool(
                    "receive_messages",
                    {"session_id": self.session_id, "fields": ["from", "message"]},
                )
                msgs = _parse_result(result)
                if isinstance(msgs, list):
                    for msg in msgs:
                        try:
                            await self.on_message(msg, cs)
                        except Exception as exc:
                            logger.warning("[%s] on_message error: %s", self.session_id, exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[%s] inbox poll error: %s", self.session_id, exc)
                raise  # bubble up to trigger reconnect
