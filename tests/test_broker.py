"""Integration tests for reyn-broker MCP tools."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


@asynccontextmanager
async def _client(url: str):
    async with (
        streamable_http_client(url) as (read, write, _close),
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


def _msg_core(msg: dict) -> dict:
    """Return only the user-visible fields of a message payload.

    Strips metadata fields (``sent_at_iso``, ``is_broadcast``,
    ``recipient_count``) so tests that care only about content can use
    exact equality without being fragile to future metadata additions.
    """
    _META = {"sent_at_iso", "is_broadcast", "recipient_count"}
    return {k: v for k, v in msg.items() if k not in _META}


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
    assert [_msg_core(m) for m in bob_inbox] == [{"from": "alice", "message": "all hands"}]
    assert [_msg_core(m) for m in carol_inbox] == [{"from": "alice", "message": "all hands"}]
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
    assert [_msg_core(m) for m in inbox] == [{"from": "alice", "message": "self too"}]


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
        assert [_msg_core(m) for m in bob_inbox] == [
            {"from": "alice", "message": "block raised on #N"}
        ]
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
    assert [_msg_core(m) for m in bob_inbox] == [{"from": "alice", "message": "please pause"}]

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


# ---------------------------------------------------------------------------
# #9 — message metadata (sent_at_iso, is_broadcast, recipient_count)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_message_payload_has_sent_at_iso(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "hi"}
        )
        msgs = _payload(await a.call_tool("receive_messages", {"session_id": "bob"}))
    assert len(msgs) == 1
    assert "sent_at_iso" in msgs[0]
    assert msgs[0]["sent_at_iso"].startswith("20")  # ISO-8601 sanity
    assert "is_broadcast" not in msgs[0]


@pytest.mark.asyncio
async def test_broadcast_payload_has_metadata_fields(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            async with _client(broker_url) as c:
                await c.call_tool(
                    "register_session", {"session_id": "carol", "working_dir": "/tmp/c"}
                )
                await a.call_tool(
                    "broadcast_message",
                    {"from_session": "alice", "message": "hello all"},
                )
                bob_msgs = _payload(await b.call_tool("receive_messages", {"session_id": "bob"}))
    assert len(bob_msgs) == 1
    msg = bob_msgs[0]
    assert msg["is_broadcast"] is True
    assert msg["recipient_count"] == 2  # bob + carol, alice excluded
    assert "sent_at_iso" in msg
    assert msg["sent_at_iso"].startswith("20")


# ---------------------------------------------------------------------------
# #10 — list_sessions activity timestamps and inbox_unread_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sessions_includes_activity_fields(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = await a.call_tool("list_sessions", {})
    [alice] = _payload(result)
    # Fields must be present
    assert "last_post_at" in alice
    assert "last_receive_at" in alice
    assert "inbox_unread_count" in alice
    # No activity yet — all None / 0
    assert alice["last_post_at"] is None
    assert alice["last_receive_at"] is None
    assert alice["inbox_unread_count"] == 0


@pytest.mark.asyncio
async def test_last_post_at_updated_after_post_message(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        before = _payload(await a.call_tool("list_sessions", {}))
        assert before[0]["last_post_at"] is None

        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "ping"}
        )
        after = _payload(await a.call_tool("list_sessions", {}))
    assert after[0]["last_post_at"] is not None
    assert after[0]["last_post_at"].startswith("20")


@pytest.mark.asyncio
async def test_last_receive_at_updated_after_receive_messages(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            await a.call_tool(
                "post_message", {"to": "bob", "from_session": "alice", "message": "hey"}
            )
            before = {s["session_id"]: s for s in _payload(await b.call_tool("list_sessions", {}))}
            assert before["bob"]["last_receive_at"] is None

            await b.call_tool("receive_messages", {"session_id": "bob"})
            after = {s["session_id"]: s for s in _payload(await b.call_tool("list_sessions", {}))}
    assert after["bob"]["last_receive_at"] is not None
    assert after["bob"]["last_receive_at"].startswith("20")


@pytest.mark.asyncio
async def test_inbox_unread_count_reflects_pending(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "m1"}
        )
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "m2"}
        )
        sessions = _payload(await a.call_tool("list_sessions", {}))
    # bob is not registered but unread count is still tracked
    alice_entry = next(s for s in sessions if s["session_id"] == "alice")
    assert alice_entry["inbox_unread_count"] == 0  # alice sent, did not receive


@pytest.mark.asyncio
async def test_activity_timestamps_persist_across_restart(broker_restart) -> None:
    url = broker_restart()
    async with _client(url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "ping"}
        )
        sessions_before = {
            s["session_id"]: s
            for s in _payload(await a.call_tool("list_sessions", {}))
        }
    ts = sessions_before["alice"]["last_post_at"]
    assert ts is not None

    url = broker_restart()
    async with _client(url) as c:
        sessions_after = {
            s["session_id"]: s
            for s in _payload(await c.call_tool("list_sessions", {}))
        }
    assert sessions_after["alice"]["last_post_at"] == ts


# ---------------------------------------------------------------------------
# multi-recipient (post_message recipients=[...])
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_message_multi_recipient_delivers_to_all(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {
                "to": "alice",  # overridden by recipients
                "from_session": "alice",
                "message": "hello all",
                "recipients": ["bob", "carol"],
            },
        )
        bob_msgs = _payload(await a.call_tool("receive_messages", {"session_id": "bob"}))
        carol_msgs = _payload(await a.call_tool("receive_messages", {"session_id": "carol"}))
    assert len(bob_msgs) == 1
    assert _msg_core(bob_msgs[0]) == {"from": "alice", "message": "hello all"}
    assert len(carol_msgs) == 1
    assert _msg_core(carol_msgs[0]) == {"from": "alice", "message": "hello all"}


@pytest.mark.asyncio
async def test_post_message_multi_recipient_return_format(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            result = await a.call_tool(
                "post_message",
                {
                    "to": "alice",
                    "from_session": "alice",
                    "message": "ping",
                    "recipients": ["bob", "ghost"],  # bob online, ghost offline
                },
            )
    text = result.content[0].text
    assert "2 recipients" in text
    assert "bob" in text
    assert "ghost" in text


@pytest.mark.asyncio
async def test_post_message_single_recipient_backward_compat(broker_url: str) -> None:
    """recipients=None → original to= behaviour preserved."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = await a.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "hi"},
        )
    text = result.content[0].text
    assert "bob" in text
    assert "online=" in text  # original single-recipient format


# ---------------------------------------------------------------------------
# TTL / message expiry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ttl_message_expires_before_drain(broker_url: str) -> None:
    import asyncio as _asyncio

    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "time-sensitive", "ttl_seconds": 1},
        )
        # Let TTL elapse
        await _asyncio.sleep(1.5)
        msgs = _payload(await a.call_tool("receive_messages", {"session_id": "bob"}))
    assert msgs == []  # expired — should be dropped


@pytest.mark.asyncio
async def test_ttl_message_delivered_before_expiry(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {
                "to": "bob",
                "from_session": "alice",
                "message": "still fresh",
                "ttl_seconds": 60,
            },
        )
        msgs = _payload(await a.call_tool("receive_messages", {"session_id": "bob"}))
    assert len(msgs) == 1
    assert _msg_core(msgs[0]) == {"from": "alice", "message": "still fresh"}
    # Internal _expires_at must not leak to recipients
    assert "_expires_at" not in msgs[0]


@pytest.mark.asyncio
async def test_ttl_inbox_stats_excludes_expired(broker_url: str) -> None:
    import asyncio as _asyncio

    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "expires soon", "ttl_seconds": 1},
        )
        stats_before = _payload(await a.call_tool("inbox_stats", {"session_id": "bob"}))
        assert stats_before["pending_count"] == 1

        await _asyncio.sleep(1.5)
        stats_after = _payload(await a.call_tool("inbox_stats", {"session_id": "bob"}))
    assert stats_after["pending_count"] == 0
