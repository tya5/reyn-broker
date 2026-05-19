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
    # role is optional, omitted = None
    assert all(s["role"] is None for s in sessions)


@pytest.mark.asyncio
async def test_register_with_role_surfaces_in_list(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/a", "role": "PR review"},
        )
        async with _client(broker_url) as b:
            await b.call_tool(
                "register_session",
                {"session_id": "bob", "working_dir": "/tmp/b", "role": "e2e tests"},
            )
            result = await b.call_tool("list_sessions", {})
    sessions = {s["session_id"]: s for s in _payload(result)}
    assert sessions["alice"]["role"] == "PR review"
    assert sessions["bob"]["role"] == "e2e tests"


@pytest.mark.asyncio
async def test_re_register_can_update_role(broker_url: str) -> None:
    async with _client(broker_url) as a1:
        await a1.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/a", "role": "old role"},
        )
    async with _client(broker_url) as a2:
        await a2.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/a", "role": "new role"},
        )
        result = await a2.call_tool("list_sessions", {})
    [alice] = _payload(result)
    assert alice["role"] == "new role"


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


@pytest.mark.asyncio
async def test_state_persists_sessions_across_restart(broker_restart) -> None:
    url = broker_restart()
    async with _client(url) as c:
        await c.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/a", "role": "PR review"},
        )

    url = broker_restart()  # simulate broker restart
    async with _client(url) as c:
        result = await c.call_tool("list_sessions", {})
    sessions = _payload(result)
    assert len(sessions) == 1
    assert sessions[0]["session_id"] == "alice"
    assert sessions[0]["working_dir"] == "/tmp/a"
    assert sessions[0]["role"] == "PR review"


@pytest.mark.asyncio
async def test_state_persists_pending_messages_across_restart(broker_restart) -> None:
    url = broker_restart()
    async with _client(url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "msg1"}
        )
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "msg2"}
        )

    url = broker_restart()  # restart — pending should survive
    async with _client(url) as b:
        result = await b.call_tool(
            "register_session", {"session_id": "bob", "working_dir": "/tmp/b"}
        )
    backlog = _payload(result)["pending_messages"]
    assert [m["message"] for m in backlog] == ["msg1", "msg2"]


@pytest.mark.asyncio
async def test_broadcast_message_queues_to_all_others(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool(
                "register_session", {"session_id": "bob", "working_dir": "/tmp/b"}
            )
            async with _client(broker_url) as c:
                await c.call_tool(
                    "register_session", {"session_id": "carol", "working_dir": "/tmp/c"}
                )
                result = await a.call_tool(
                    "broadcast_message",
                    {"from_session": "alice", "message": "all hands"},
                )
                text = result.content[0].text
                # alice excluded by default → 2 recipients
                assert "broadcast to 2 sessions" in text

                bob_inbox = _payload(
                    await b.call_tool("receive_messages", {"session_id": "bob"})
                )
                carol_inbox = _payload(
                    await c.call_tool("receive_messages", {"session_id": "carol"})
                )
                alice_inbox = _payload(
                    await a.call_tool("receive_messages", {"session_id": "alice"})
                )
    assert bob_inbox == [{"from": "alice", "message": "all hands"}]
    assert carol_inbox == [{"from": "alice", "message": "all hands"}]
    assert alice_inbox == []  # sender excluded


@pytest.mark.asyncio
async def test_broadcast_message_include_self(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "broadcast_message",
            {"from_session": "alice", "message": "self too", "exclude_self": False},
        )
        result = await a.call_tool("receive_messages", {"session_id": "alice"})
    inbox = _payload(result)
    assert inbox == [{"from": "alice", "message": "self too"}]


@pytest.mark.asyncio
async def test_broadcast_message_no_recipients_when_alone(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = await a.call_tool(
            "broadcast_message",
            {"from_session": "alice", "message": "hello?"},
        )
    text = result.content[0].text
    assert "broadcast to 0 sessions" in text


@pytest.mark.asyncio
async def test_inbox_stats_empty_for_no_messages(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = await c.call_tool("inbox_stats", {"session_id": "ghost"})
    data = _payload(result)
    assert data == {"session_id": "ghost", "pending_count": 0, "senders": []}


@pytest.mark.asyncio
async def test_inbox_stats_counts_and_lists_senders_without_drain(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "m1"}
        )
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "m2"}
        )
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "carol", "message": "m3"}
        )
        stats = _payload(await a.call_tool("inbox_stats", {"session_id": "bob"}))
        assert stats["pending_count"] == 3
        assert stats["senders"] == ["alice", "carol"]
        # Re-running stats yields the same — non-destructive.
        stats2 = _payload(await a.call_tool("inbox_stats", {"session_id": "bob"}))
        assert stats2 == stats


@pytest.mark.asyncio
async def test_request_read_ack_delivers_ack_on_drain(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {
                "to": "bob",
                "from_session": "alice",
                "message": "block raised on #N",
                "request_read_ack": True,
            },
        )
        # Before bob drains, alice has no ack
        alice_inbox = _payload(
            await a.call_tool("receive_messages", {"session_id": "alice"})
        )
        assert alice_inbox == []
        # Bob drains
        async with _client(broker_url) as b:
            bob_inbox = _payload(
                await b.call_tool("receive_messages", {"session_id": "bob"})
            )
        # Internal _ack_to is stripped from delivered message
        assert bob_inbox == [{"from": "alice", "message": "block raised on #N"}]
        # Now alice should have a read-ack from broker
        alice_inbox = _payload(
            await a.call_tool("receive_messages", {"session_id": "alice"})
        )
    assert len(alice_inbox) == 1
    assert alice_inbox[0]["from"] == "broker"
    assert "read-ack" in alice_inbox[0]["message"]
    assert "bob" in alice_inbox[0]["message"]


@pytest.mark.asyncio
async def test_no_read_ack_when_not_requested(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "fire-and-forget"},
        )
        async with _client(broker_url) as b:
            await b.call_tool("receive_messages", {"session_id": "bob"})
        alice_inbox = _payload(
            await a.call_tool("receive_messages", {"session_id": "alice"})
        )
    assert alice_inbox == []


@pytest.mark.asyncio
async def test_request_read_ack_persists_across_restart(broker_restart) -> None:
    url = broker_restart()
    async with _client(url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {
                "to": "bob",
                "from_session": "alice",
                "message": "please pause",
                "request_read_ack": True,
            },
        )

    url = broker_restart()  # broker restart while message still pending
    async with _client(url) as b:
        bob_inbox = _payload(
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
        )["pending_messages"]
    assert bob_inbox == [{"from": "alice", "message": "please pause"}]

    async with _client(url) as a:
        alice_inbox = _payload(
            await a.call_tool("receive_messages", {"session_id": "alice"})
        )
    assert len(alice_inbox) == 1
    assert alice_inbox[0]["from"] == "broker"
    assert "read-ack" in alice_inbox[0]["message"]


@pytest.mark.asyncio
async def test_unregister_removal_persists_across_restart(broker_restart) -> None:
    url = broker_restart()
    async with _client(url) as c:
        await c.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await c.call_tool("unregister_session", {"session_id": "alice"})

    url = broker_restart()
    async with _client(url) as c:
        result = await c.call_tool("list_sessions", {})
    assert _payload(result) == []
