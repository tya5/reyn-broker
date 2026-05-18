#!/usr/bin/env python3
"""Broker MCP server for inter-session messaging.

Each Claude Code session connects to this broker over Streamable HTTP,
registers itself with a session_id, and can post messages to other
registered sessions. Incoming messages are pushed to the target session
via the MCP-standard ``notifications/message`` (logging) notification.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

logger = logging.getLogger("broker")


@dataclass
class SessionEntry:
    session_id: str
    working_dir: str
    mcp_session: ServerSession


sessions: dict[str, SessionEntry] = {}
pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
registry_lock = asyncio.Lock()

mcp = FastMCP("broker")


async def _deliver(target_id: str, payload: dict[str, Any]) -> bool:
    entry = sessions.get(target_id)
    if entry is None:
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
) -> dict[str, Any]:
    """Register this Claude Code session with the broker.

    Call this once at session startup. Pass your directory name as
    ``session_id`` and the absolute path as ``working_dir``.

    Returns any messages that were queued while this session was offline.
    """
    async with registry_lock:
        sessions[session_id] = SessionEntry(
            session_id=session_id,
            working_dir=working_dir,
            mcp_session=ctx.session,
        )
        backlog = pending.pop(session_id, [])

    return {
        "status": f"registered '{session_id}' at {working_dir}",
        "pending_messages": backlog,
    }


@mcp.tool()
async def unregister_session(session_id: str) -> str:
    """Unregister a session from the broker."""
    async with registry_lock:
        existed = sessions.pop(session_id, None)
    if existed is None:
        return f"'{session_id}' was not registered"
    return f"unregistered '{session_id}'"


@mcp.tool()
async def list_sessions() -> list[dict[str, str]]:
    """List currently registered sessions."""
    async with registry_lock:
        return [
            {"session_id": e.session_id, "working_dir": e.working_dir}
            for e in sessions.values()
        ]


@mcp.tool()
async def post_message(to: str, from_session: str, message: str) -> str:
    """Send a message to another session.

    The message is always queued in the recipient's inbox. The recipient
    picks it up by calling ``receive_messages``. A best-effort log
    notification is also pushed as a hint, but recipients must not rely
    on it — Claude Code does not always surface log notifications to
    the agent.
    """
    payload = {"from": from_session, "message": message}

    async with registry_lock:
        pending[to].append(payload)
        target_online = to in sessions

    if target_online:
        await _deliver(to, payload)

    return f"queued for '{to}' (online={target_online})"


@mcp.tool()
async def receive_messages(session_id: str) -> list[dict[str, Any]]:
    """Drain and return all queued messages addressed to ``session_id``.

    Each Claude Code session should call this proactively — at startup
    after ``register_session``, at the start of each turn, after
    long-running tasks, and whenever the user asks "check your inbox".
    The returned list is removed from the queue once handed back.
    """
    async with registry_lock:
        msgs = pending.pop(session_id, [])
    return msgs


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
    logger.info("starting broker on %s:%s", mcp.settings.host, mcp.settings.port)
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
