#!/usr/bin/env python3
"""Inbox watcher for broker-mediated Claude Code sessions.

Polls ``receive_messages`` every ``POLL_S`` seconds and emits one stdout
line per arrived message (JSON-encoded). Designed to be wrapped by the
Claude Code Monitor tool so each new broker message arrives as a
``<task-notification>`` event — i.e. the LLM context gets woken up even
when the Claude Code session is otherwise idle.

Usage (typically as a Monitor command):

    python3 /path/to/broker/session_watcher.py --session=<id>

Output (per arrived message, one JSON line):

    {"from": "<sender>", "message": "<body>"}

Operational notes:
  - The watcher does NOT call register_session — that's the Claude Code
    session's job. The watcher only drains via receive_messages.
  - Multiple concurrent watchers for the same session_id would race over
    the inbox. Run at most one per session.
  - Transient broker outages back off ``ERROR_BACKOFF_S`` and stay quiet
    until 5 consecutive failures, then emit a single ``_watcher_error``
    line so the Claude Code session can see something is wrong.
  - **Batching**: when one poll returns N messages, this script emits N
    stdout lines back-to-back. Claude Code's Monitor tool batches stdout
    lines arriving within ~200ms into a single ``<task-notification>``
    event, so the receiving LLM may see multiple JSON lines concatenated
    in one event body. Recipients should parse the event body
    line-by-line (each line is one complete message JSON).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

BROKER_URL = "http://127.0.0.1:8765/mcp"
POLL_S = 30
ERROR_BACKOFF_S = 10
ERROR_QUIET_THRESHOLD = 5  # stay quiet for first N consecutive errors


def _emit(payload: dict) -> None:
    """One JSON line to stdout, flushed."""
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _parse_messages(call_result) -> list[dict]:
    """Extract the list[dict] from a CallToolResult.

    FastMCP exposes two routes for tool return values:
      1. ``structuredContent={"result": [...]}`` — canonical for newer
         versions; preferred when present.
      2. ``content=[TextContent(text="..."), ...]`` — each block is one
         element of the returned list, JSON-encoded. Older fallback.

    Defensive: tolerate empty content / non-JSON / non-dict.
    """
    msgs: list[dict] = []
    structured = getattr(call_result, "structuredContent", None)
    if isinstance(structured, dict):
        inner = structured.get("result")
        if isinstance(inner, list):
            for m in inner:
                if isinstance(m, dict):
                    msgs.append(m)
            return msgs
    # Fallback: parse each TextContent block as a standalone dict.
    content = getattr(call_result, "content", None) or []
    for item in content:
        text = getattr(item, "text", None)
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if isinstance(parsed, dict):
            msgs.append(parsed)
        elif isinstance(parsed, list):
            for m in parsed:
                if isinstance(m, dict):
                    msgs.append(m)
    return msgs


async def _poll_loop(session_id: str) -> None:
    consecutive_errors = 0
    while True:
        try:
            async with (
                streamablehttp_client(BROKER_URL) as (read, write, _close),
                ClientSession(read, write) as cs,
            ):
                await cs.initialize()
                consecutive_errors = 0
                while True:
                    result = await cs.call_tool(
                        "receive_messages",
                        {"session_id": session_id},
                    )
                    for msg in _parse_messages(result):
                        _emit(msg)
                    await asyncio.sleep(POLL_S)
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors == ERROR_QUIET_THRESHOLD:
                _emit({
                    "_watcher_error": (
                        f"broker poll failed {ERROR_QUIET_THRESHOLD}x: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                })
            await asyncio.sleep(ERROR_BACKOFF_S)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Poll broker inbox and emit each new message as a stdout line."
    )
    parser.add_argument("--session", required=True, help="Your session_id (= basename of cwd)")
    args = parser.parse_args()
    try:
        asyncio.run(_poll_loop(args.session))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
