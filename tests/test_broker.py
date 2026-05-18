"""Integration tests for reyn-broker MCP tools."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


@asynccontextmanager
async def _client(url: str):
    async with (
        streamablehttp_client(url) as (read, write, _close),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        yield session


def _payload(result: Any) -> Any:
    """Extract the structured payload from a CallToolResult.

    FastMCP returns results via ``structuredContent={"result": ...}`` when
    available. Falls back to JSON-decoding each TextContent block.
    """
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and "result" in structured:
        return structured["result"]
    items = []
    for block in getattr(result, "content", []) or []:
        text = getattr(block, "text", None)
        if text is None:
            continue
        try:
            items.append(json.loads(text))
        except json.JSONDecodeError:
            items.append(text)
    if len(items) == 1:
        return items[0]
    return items


@pytest.mark.asyncio
async def test_register_returns_empty_backlog_for_new_session(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = await c.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/alice"},
        )
    data = _payload(result)
    assert data["pending_messages"] == []
    assert "alice" in data["status"]


@pytest.mark.asyncio
async def test_list_sessions_reflects_registrations(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            result = await b.call_tool("list_sessions", {})
    sessions = _payload(result)
    assert isinstance(sessions, list)
    ids = {s["session_id"] for s in sessions}
    assert ids == {"alice", "bob"}


@pytest.mark.asyncio
async def test_unregister_removes_from_list(broker_url: str) -> None:
    async with _client(broker_url) as c:
        await c.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await c.call_tool("unregister_session", {"session_id": "alice"})
        result = await c.call_tool("list_sessions", {})
    assert _payload(result) == []


@pytest.mark.asyncio
async def test_unregister_unknown_session_is_safe(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = await c.call_tool("unregister_session", {"session_id": "ghost"})
    text = result.content[0].text
    assert "not registered" in text


@pytest.mark.asyncio
async def test_post_to_offline_recipient_queues(broker_url: str) -> None:
    async with _client(broker_url) as c:
        await c.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = await c.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "hi"},
        )
    text = result.content[0].text
    assert "bob" in text
    assert "False" in text  # online=False since bob hasn't registered


@pytest.mark.asyncio
async def test_pending_messages_drained_on_register(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "msg1"}
        )
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "msg2"}
        )
    async with _client(broker_url) as b:
        result = await b.call_tool(
            "register_session", {"session_id": "bob", "working_dir": "/tmp/b"}
        )
    data = _payload(result)
    backlog = data["pending_messages"]
    assert len(backlog) == 2
    assert [m["message"] for m in backlog] == ["msg1", "msg2"]
    assert all(m["from"] == "alice" for m in backlog)


@pytest.mark.asyncio
async def test_receive_messages_drains_inbox(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool(
                "register_session", {"session_id": "bob", "working_dir": "/tmp/b"}
            )
            await a.call_tool(
                "post_message",
                {"to": "bob", "from_session": "alice", "message": "live"},
            )
            first = await b.call_tool("receive_messages", {"session_id": "bob"})
            second = await b.call_tool("receive_messages", {"session_id": "bob"})
    first_payload = _payload(first)
    assert isinstance(first_payload, list)
    assert len(first_payload) == 1
    assert first_payload[0]["from"] == "alice"
    assert first_payload[0]["message"] == "live"
    assert _payload(second) == []


@pytest.mark.asyncio
async def test_receive_messages_empty_for_unknown_session(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = await c.call_tool("receive_messages", {"session_id": "ghost"})
    assert _payload(result) == []


@pytest.mark.asyncio
async def test_re_register_replaces_session_entry(broker_url: str) -> None:
    async with _client(broker_url) as a1:
        await a1.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a1"})
    async with _client(broker_url) as a2:
        await a2.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a2"})
        result = await a2.call_tool("list_sessions", {})
    sessions = _payload(result)
    alice_entries = [s for s in sessions if s["session_id"] == "alice"]
    assert len(alice_entries) == 1
    assert alice_entries[0]["working_dir"] == "/tmp/a2"
