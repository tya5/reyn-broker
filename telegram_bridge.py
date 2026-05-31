#!/usr/bin/env python3
"""Telegram ↔ broker bridge for reyn-broker.

Bidirectional bridge between a Telegram Bot and the broker MCP server:

  Telegram → broker  Send commands from your smartphone to broker sessions.
  broker → Telegram  Receive broker messages in your Telegram chat.

When ``BROKER_MONITOR_SESSION=telegram`` (or whatever BRIDGE_SESSION_ID is)
is set on the broker, all inter-session traffic is also mirrored to this
Telegram chat (prefixed with 📊).

Setup
-----
1. Message @BotFather on Telegram → /newbot → follow prompts → get TOKEN.
2. Send any message to your new bot, then run::

       curl https://api.telegram.org/bot<TOKEN>/getUpdates

   Find ``"chat": {"id": <CHAT_ID>}`` in the response.

3. Start the bridge::

       TELEGRAM_BOT_TOKEN=<token> TELEGRAM_CHAT_ID=<chat_id> \\
       /path/to/broker/.venv/bin/python /path/to/broker/telegram_bridge.py

4. To enable monitoring of all broker traffic, restart the broker with::

       BROKER_MONITOR_SESSION=telegram reyn-broker

Environment variables
---------------------
TELEGRAM_BOT_TOKEN    (required) Bot token from @BotFather.
TELEGRAM_CHAT_ID      (required) Your Telegram user ID (from getUpdates).
BROKER_URL            Broker MCP URL (default: http://127.0.0.1:8765/mcp).
BRIDGE_SESSION_ID     Session id to register on broker (default: telegram).

Telegram commands
-----------------
/list                    List registered broker sessions.
/send <session> <msg>    Post a message to a session.
/broadcast <msg>         Broadcast to all sessions.
/stats                   Broker health + tool call counts.
/help                    Show this help.

Plain text (non-command) is forwarded to the last session you used /send with.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
BROKER_URL = os.environ.get("BROKER_URL", "http://127.0.0.1:8765/mcp")
BRIDGE_SID = os.environ.get("BRIDGE_SESSION_ID", "telegram")

_TG_API = f"https://api.telegram.org/bot{TOKEN}"
_TG_LONG_POLL = 30   # seconds for Telegram getUpdates timeout
_BROKER_POLL_S = 30  # seconds between broker inbox drains
_RECONNECT_S = 10


# ---------------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------------

def _tg_call_sync(
    method: str,
    payload: dict | None = None,
    params: dict | None = None,
) -> Any:
    """Synchronous Telegram Bot API call (intended for asyncio.to_thread)."""
    if payload is not None:
        url = f"{_TG_API}/{method}"
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
    else:
        url = f"{_TG_API}/{method}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=_TG_LONG_POLL + 5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        raise RuntimeError(f"Telegram HTTP {exc.code}: {body_text}") from exc


async def _tg(method: str, payload: dict | None = None, **params: Any) -> Any:
    return await asyncio.to_thread(_tg_call_sync, method, payload, params or None)


async def send(text: str) -> None:
    """Send a Markdown message to the configured Telegram chat."""
    if not text.strip():
        return
    try:
        await _tg("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
    except Exception:
        # Fallback: retry without Markdown in case of parse error
        try:
            await _tg("sendMessage", {"chat_id": CHAT_ID, "text": text})
        except Exception as exc2:
            print(f"[telegram-bridge] send failed: {exc2}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Broker result parsing
# ---------------------------------------------------------------------------

def _parse_result(result: Any) -> Any:
    """Extract structured content from a FastMCP CallToolResult."""
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


# ---------------------------------------------------------------------------
# Telegram → broker command handling
# ---------------------------------------------------------------------------

async def handle_command(text: str, cs: ClientSession, state: dict) -> None:
    """Parse a Telegram message and call the appropriate broker tool."""
    text = text.strip()

    if text.startswith("/help") or text == "/start":
        await send(
            "*reyn\\-broker Telegram bridge*\n\n"
            "`/list` — registered sessions\n"
            "`/send <session> <msg>` — post to a session\n"
            "`/broadcast <msg>` — broadcast to all sessions\n"
            "`/stats` — broker health \\+ call counts\n"
            "`/help` — this message\n\n"
            "Plain text → forwarded to last `/send` target\\."
        )
        return

    if text.startswith("/list"):
        result = _parse_result(await cs.call_tool("list_sessions", {"compact": True}))
        if not result:
            await send("No sessions registered.")
            return
        lines = "\n".join(
            f"• `{s['session_id']}`" + (f" — {s['role']}" if s.get("role") else "")
            for s in result
        )
        await send(f"*Registered sessions:*\n{lines}")
        return

    if text.startswith("/send "):
        parts = text[6:].strip().split(" ", 1)
        if len(parts) < 2:
            await send("Usage: `/send <session_id> <message>`")
            return
        target, message = parts[0], parts[1]
        state["last_session"] = target
        result = _parse_result(await cs.call_tool("post_message", {
            "to": target,
            "from_session": BRIDGE_SID,
            "message": message,
        }))
        await send(f"✉️ {result}")
        return

    if text.startswith("/broadcast "):
        message = text[11:].strip()
        if not message:
            await send("Usage: `/broadcast <message>`")
            return
        result = _parse_result(await cs.call_tool("broadcast_message", {
            "from_session": BRIDGE_SID,
            "message": message,
        }))
        await send(f"📡 {result}")
        return

    if text.startswith("/stats"):
        health = _parse_result(await cs.call_tool("health_check", {}))
        stats_section = ""
        try:
            stats = _parse_result(await cs.call_tool("tool_stats", {}))
            top = list(stats["counts"].items())[:8]
            rows = "\n".join(f"  {k}: {v}" for k, v in top)
            stats_section = f"\n\n*Top tool calls:*\n`{rows}`"
        except Exception:
            pass
        await send(
            f"*Broker health*\n"
            f"version: `{health['version']}`\n"
            f"uptime: `{health['uptime_seconds']}s`\n"
            f"sessions: `{health['session_count']}`\n"
            f"pending: `{health['total_pending']}`"
            + stats_section
        )
        return

    if text.startswith("/"):
        await send(f"Unknown command: `{text.split()[0]}`\nTry `/help`.")
        return

    # Plain text → forward to last known session
    last = state.get("last_session")
    if not last:
        await send(
            "No active session\\. Use `/send <session\\_id> <message>` first\\."
        )
        return
    result = _parse_result(await cs.call_tool("post_message", {
        "to": last,
        "from_session": BRIDGE_SID,
        "message": text,
    }))
    await send(f"✉️ → `{last}`: {result}")


# ---------------------------------------------------------------------------
# Main polling loops
# ---------------------------------------------------------------------------

async def telegram_loop(cs: ClientSession, state: dict) -> None:
    """Long-poll Telegram for new messages and dispatch to broker."""
    offset = 0
    while True:
        try:
            data = await asyncio.to_thread(
                _tg_call_sync,
                "getUpdates",
                None,
                {"offset": offset, "timeout": _TG_LONG_POLL, "allowed_updates": ["message"]},
            )
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != CHAT_ID:
                    continue  # ignore messages from other chats / groups
                text = msg.get("text", "")
                if not text:
                    continue
                try:
                    await handle_command(text, cs, state)
                except Exception as exc:
                    await send(f"⚠️ Error: {exc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[telegram-bridge] Telegram poll error: {exc}", file=sys.stderr)
            await asyncio.sleep(5)


async def broker_loop(cs: ClientSession) -> None:
    """Periodically drain the broker inbox and forward messages to Telegram."""
    while True:
        await asyncio.sleep(_BROKER_POLL_S)
        try:
            result = _parse_result(
                await cs.call_tool("receive_messages", {"session_id": BRIDGE_SID})
            )
            if not isinstance(result, list):
                continue
            for msg in result:
                sender = msg.get("from", "?")
                body = msg.get("message", "")
                monitor_to = msg.get("monitor_to")

                if monitor_to:
                    # Monitoring copy: show traffic between other sessions
                    if isinstance(monitor_to, list):
                        to_str = ", ".join(f"`{t}`" for t in monitor_to)
                    else:
                        to_str = f"`{monitor_to}`"
                    bc_icon = "📡" if msg.get("is_broadcast") else "→"
                    await send(f"📊 `{sender}` {bc_icon} {to_str}:\n{body}")
                else:
                    # Normal message addressed to this session
                    await send(f"💬 *{sender}*:\n{body}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[telegram-bridge] broker poll error: {exc}", file=sys.stderr)


async def main() -> None:
    if not TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is not set.", file=sys.stderr)
        sys.exit(1)
    if not CHAT_ID:
        print("Error: TELEGRAM_CHAT_ID is not set.", file=sys.stderr)
        sys.exit(1)

    print(f"[telegram-bridge] starting (session={BRIDGE_SID}, chat_id={CHAT_ID})")
    await send(
        f"🟢 *broker Telegram bridge started*\n"
        f"Session: `{BRIDGE_SID}`\nType /help for commands\\."
    )

    while True:
        try:
            async with (
                streamable_http_client(BROKER_URL) as (read, write, _close),
                ClientSession(read, write) as cs,
            ):
                await cs.initialize()
                await cs.call_tool("register_session", {
                    "session_id": BRIDGE_SID,
                    "working_dir": os.getcwd(),
                    "role": "Telegram bridge — smartphone ↔ broker gateway",
                })
                print(f"[telegram-bridge] registered as '{BRIDGE_SID}', polling...")
                state: dict = {}
                await asyncio.gather(
                    telegram_loop(cs, state),
                    broker_loop(cs),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(
                f"[telegram-bridge] broker connection error: {exc}; "
                f"reconnecting in {_RECONNECT_S}s...",
                file=sys.stderr,
            )
            await asyncio.sleep(_RECONNECT_S)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[telegram-bridge] stopped")
