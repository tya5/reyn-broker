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


async def send(text: str, keyboard: dict | None = None) -> None:
    """Send a Markdown message to the configured Telegram chat."""
    if not text.strip():
        return
    payload: dict = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = keyboard
    try:
        await _tg("sendMessage", payload)
    except Exception:
        # Fallback: retry without Markdown in case of parse error
        try:
            plain: dict = {"chat_id": CHAT_ID, "text": text}
            if keyboard:
                plain["reply_markup"] = keyboard
            await _tg("sendMessage", plain)
        except Exception as exc2:
            print(f"[telegram-bridge] send failed: {exc2}", file=sys.stderr)


async def answer_callback(callback_id: str, text: str = "") -> None:
    """Acknowledge an inline keyboard button tap (required by Telegram)."""
    import contextlib
    with contextlib.suppress(Exception):
        await _tg("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})


def _session_keyboard(sessions: list[dict]) -> dict:
    """Build an InlineKeyboardMarkup from a session list."""
    buttons = []
    for s in sessions:
        sid = s["session_id"]
        label = sid if not s.get("role") else f"{sid} ({s['role'][:20]})"
        buttons.append([{"text": label, "callback_data": f"select:{sid}"}])
    return {"inline_keyboard": buttons}


_NAV_KEYBOARD = {
    "keyboard": [[
        {"text": "📋 Sessions"},
        {"text": "📊 Stats"},
        {"text": "📡 Broadcast"},
    ]],
    "resize_keyboard": True,
    "persistent": True,
}


async def show_home(cs: ClientSession, state: dict) -> None:
    """Show the InlineKeyboard session picker (+ persistent nav keyboard)."""
    sessions = _parse_result(await cs.call_tool("list_sessions", {"compact": True}))
    if not sessions:
        await send("セッションが登録されていません。", keyboard=_NAV_KEYBOARD)
        return
    current = state.get("last_session")
    label = "送信先を選んでください："
    if current:
        label += f"\n現在: `{current}`"
    await send(label, keyboard=_session_keyboard(sessions))


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

async def _send_to_session(target: str, text: str, cs: ClientSession) -> None:
    result = _parse_result(await cs.call_tool("post_message", {
        "to": target,
        "from_session": BRIDGE_SID,
        "message": text,
    }))
    await send(f"✉️ → `{target}`: {result}")


async def handle_command(text: str, cs: ClientSession, state: dict) -> None:
    """Parse a Telegram message and call the appropriate broker tool."""
    text = text.strip()

    # --- Utility buttons from persistent nav keyboard ---
    if text in ("📋 Sessions", "📋 一覧更新"):
        await show_home(cs, state)
        return

    if text == "📊 Stats":
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

    if text == "📡 Broadcast":
        state["_mode"] = "broadcast"
        await send("broadcast するメッセージを入力してください：")
        return

    # --- Slash commands ---
    if text.startswith("/help") or text == "/start":
        await show_home(cs, state)
        return

    if text.startswith("/list"):
        await show_home(cs, state)
        return

    if text.startswith("/send "):
        parts = text[6:].strip().split(" ", 1)
        if len(parts) < 2:
            await send("Usage: `/send <session_id> <message>`")
            return
        target, message = parts[0], parts[1]
        state["last_session"] = target
        await _send_to_session(target, message, cs)
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
        await handle_command("📊 Stats", cs, state)
        return

    if text.startswith("/"):
        await send(f"Unknown command: `{text.split()[0]}`")
        return

    # --- Plain text ---

    # broadcast モード
    if state.pop("_mode", None) == "broadcast":
        result = _parse_result(await cs.call_tool("broadcast_message", {
            "from_session": BRIDGE_SID,
            "message": text,
        }))
        await send(f"📡 {result}")
        return

    # 送信先が設定済みならそのまま転送
    last = state.get("last_session")
    if last:
        await _send_to_session(last, text, cs)
        return

    # 送信先未設定 → InlineKeyboard ピッカーを出してテキストを保留
    sessions = _parse_result(await cs.call_tool("list_sessions", {"compact": True}))
    state["_pending_text"] = text
    await send("送信先を選んでください：", keyboard=_session_keyboard(sessions or []))


# ---------------------------------------------------------------------------
# Main polling loops
# ---------------------------------------------------------------------------

async def telegram_loop(cs: ClientSession, state: dict) -> None:
    """Long-poll Telegram for new messages and callback queries."""
    offset = 0
    allowed = ["message", "callback_query"]
    while True:
        try:
            data = await asyncio.to_thread(
                _tg_call_sync,
                "getUpdates",
                None,
                {"offset": offset, "timeout": _TG_LONG_POLL, "allowed_updates": allowed},
            )
            for update in data.get("result", []):
                offset = update["update_id"] + 1

                # Inline keyboard button tap
                cb = update.get("callback_query")
                if cb:
                    cb_chat_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
                    if cb_chat_id != CHAT_ID:
                        continue
                    data_str = cb.get("data", "")
                    await answer_callback(cb["id"])
                    if data_str.startswith("select:"):
                        sid = data_str[7:]
                        state["last_session"] = sid
                        pending_text = state.pop("_pending_text", None)
                        if pending_text:
                            await _send_to_session(sid, pending_text, cs)
                        else:
                            await send(f"✅ 送信先: `{sid}`\nメッセージを入力してください。")
                    continue

                # Regular text message
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != CHAT_ID:
                    continue
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
        f"🟢 *broker Telegram bridge started*\nSession: `{BRIDGE_SID}`",
        keyboard=_NAV_KEYBOARD,
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
                # Show session picker immediately on startup
                await show_home(cs, state)
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
