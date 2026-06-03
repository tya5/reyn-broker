"""Telegram ↔ broker bridge plugin.

Refactored from ``telegram_bridge.py`` to use :class:`BrokerPlugin`.
All Telegram logic lives here; ``telegram_bridge.py`` is a backward-compat shim.

Environment variables
---------------------
TELEGRAM_BOT_TOKEN    (required) Bot token from @BotFather.
TELEGRAM_CHAT_ID      (required) Your Telegram user ID (from getUpdates).
BROKER_URL            Broker MCP URL (default: http://127.0.0.1:8765/mcp).
BRIDGE_SESSION_ID     Session id to register on broker (default: telegram).
"""
from __future__ import annotations

import asyncio
import sys

from mcp import ClientSession

from plugin_base import BrokerPlugin

# The full Telegram implementation lives in telegram_bridge.py so existing
# users launching it directly continue to work. TelegramPlugin wraps it.


class TelegramPlugin(BrokerPlugin):
    """Thin BrokerPlugin wrapper around telegram_bridge logic.

    The actual Telegram polling and command handling is delegated to
    ``telegram_bridge`` so both entry points share one implementation.
    """

    import os as _os
    session_id: str = _os.environ.get("BRIDGE_SESSION_ID", "telegram")
    role: str = "Telegram bridge — smartphone ↔ broker gateway"

    async def on_connected(self, cs: ClientSession) -> None:
        # Import here to avoid circular imports at module level
        import telegram_bridge as _tb
        await _tb.show_home(cs, self._state)

    async def on_message(self, msg: dict, cs: ClientSession) -> None:
        import telegram_bridge as _tb
        sender = msg.get("from", "?")
        body = msg.get("message", "")
        monitor_to = msg.get("monitor_to")
        if monitor_to:
            if isinstance(monitor_to, list):
                to_str = ", ".join(f"`{t}`" for t in monitor_to)
            else:
                to_str = f"`{monitor_to}`"
            bc_icon = "📡" if msg.get("is_broadcast") else "→"
            await _tb.send(f"📊 `{sender}` {bc_icon} {to_str}:\n{body}")
        else:
            await _tb.send(f"💬 *{sender}*:\n{body}")

    async def run_tasks(self, cs: ClientSession) -> list[asyncio.Task]:
        import telegram_bridge as _tb
        return [asyncio.create_task(_tb.telegram_loop(cs, self._state))]

    def __init__(self) -> None:
        self._state: dict = {}

    async def run(self) -> None:
        import telegram_bridge as _tb
        if not _tb.TOKEN:
            print("Error: TELEGRAM_BOT_TOKEN is not set.", file=sys.stderr)
            sys.exit(1)
        if not _tb.CHAT_ID:
            print("Error: TELEGRAM_CHAT_ID is not set.", file=sys.stderr)
            sys.exit(1)
        import os
        self.session_id = os.environ.get("BRIDGE_SESSION_ID", "telegram")
        await _tb.send(
            f"🟢 *broker Telegram bridge started*\nSession: `{self.session_id}`",
            keyboard=_tb._NAV_KEYBOARD,
        )
        await super().run()


def main() -> None:
    try:
        asyncio.run(TelegramPlugin().run())
    except KeyboardInterrupt:
        print("[telegram] stopped")
