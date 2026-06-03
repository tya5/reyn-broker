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


async def _update(session_id: str, status: str, detail: str | None) -> None:
    async with (
        streamable_http_client(_BROKER_URL) as (read, write, _close),
        ClientSession(read, write) as cs,
    ):
        await cs.initialize()
        args: dict = {"session_id": session_id, "status": status}
        if detail:
            args["detail"] = detail
        await cs.call_tool("update_session_status", args)


def main() -> None:
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} SESSION_ID STATUS [DETAIL]", file=sys.stderr)
        sys.exit(1)
    session_id = sys.argv[1]
    status = sys.argv[2]
    detail = sys.argv[3] if len(sys.argv) > 3 else None
    try:
        asyncio.run(_update(session_id, status, detail))
    except Exception as exc:
        print(f"[session_status] error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
