#!/usr/bin/env python3
"""One-shot CLI to update a session's status on the broker.

Designed for use in Claude Code hooks (Stop, PreToolUse, etc.) where
no LLM turn is involved and token cost must be zero.

Usage
-----
    session_status.py SESSION_ID STATUS [DETAIL]

Arguments
---------
SESSION_ID   Broker session id to update.
STATUS       New status string (e.g. "active", "idle", "waiting").
DETAIL       Optional free-form description (e.g. "waiting for PR review").

Environment
-----------
BROKER_URL   Override broker URL (default: http://127.0.0.1:8765/mcp).

Examples
--------
Stop hook (settings.json)::

    {
      "hooks": {
        "Stop": [{
          "matcher": "",
          "hooks": [{
            "type": "command",
            "command": "/path/to/.venv/bin/reyn-broker-status lead-coder idle"
          }]
        }]
      }
    }
"""
from __future__ import annotations

import asyncio
import os
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

_BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8765/mcp")


async def _call(tool: str, args: dict) -> None:
    async with (
        streamable_http_client(_BROKER_URL) as (read, write, _close),
        ClientSession(read, write) as cs,
    ):
        await cs.initialize()
        await cs.call_tool(tool, args)


def main() -> None:
    """reyn-broker-status SESSION_ID STATUS [DETAIL] — set the semantic axis."""
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} SESSION_ID STATUS [DETAIL]", file=sys.stderr)
        sys.exit(1)
    session_id, status = sys.argv[1], sys.argv[2]
    args: dict = {"session_id": session_id, "status": status}
    if len(sys.argv) > 3:
        args["detail"] = sys.argv[3]
    try:
        asyncio.run(_call("update_session_status", args))
    except Exception as exc:
        print(f"[session_status] error: {exc}", file=sys.stderr)
        sys.exit(1)


def main_active() -> None:
    """reyn-broker-active SESSION_ID true|false — set the in-turn bit (#31).

    Intended for Claude Code hooks (zero LLM cost):
      work-start hook → reyn-broker-active <id> true
      Stop hook       → reyn-broker-active <id> false
    """
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} SESSION_ID true|false", file=sys.stderr)
        sys.exit(1)
    session_id, raw = sys.argv[1], sys.argv[2].strip().lower()
    if raw not in ("true", "false"):
        print(f"[session_active] active must be 'true' or 'false', got {raw!r}", file=sys.stderr)
        sys.exit(1)
    args = {"session_id": session_id, "active": raw == "true"}
    try:
        asyncio.run(_call("set_active", args))
    except Exception as exc:
        print(f"[session_active] error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
