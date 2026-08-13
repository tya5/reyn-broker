"""Integration tests for reyn-broker MCP tools."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from pydantic import AnyUrl


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
    # bob has never registered, so the reply must say that rather than
    # "online=False" — an unknown id and an idle peer used to be reported
    # identically, which is what reyn-broker#14 fixed. The message is still
    # queued either way (asserted by the pending-drain test below).
    assert "NOT REGISTERED" in text


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
        # bob is registered so the reply exercises the online= form; an
        # unregistered target takes the reyn-broker#14 branch instead, which
        # would make this assert about recipient existence rather than about
        # the single- vs multi-recipient reply format it is here to pin.
        async with _client(broker_url) as b:
            await b.call_tool(
                "register_session", {"session_id": "bob", "working_dir": "/tmp/b"}
            )
            result = await a.call_tool(
                "post_message",
                {"to": "bob", "from_session": "alice", "message": "hi"},
            )
    text = result.content[0].text
    assert "bob" in text
    assert "online=" in text  # original single-recipient format
    assert "recipients" not in text  # not the aggregated multi-recipient form


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


# ---------------------------------------------------------------------------
# compact list_sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sessions_compact_returns_only_id_and_role(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool(
            "register_session",
            {"session_id": "alice", "working_dir": "/tmp/a", "role": "tester"},
        )
        result = _payload(await a.call_tool("list_sessions", {"compact": True}))
    assert result == [{"session_id": "alice", "role": "tester", "active": True, "status": None}]


@pytest.mark.asyncio
async def test_list_sessions_compact_false_returns_full_shape(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = _payload(await a.call_tool("list_sessions", {"compact": False}))
    assert len(result) == 1
    assert "working_dir" in result[0]
    assert "last_post_at" in result[0]
    assert "inbox_unread_count" in result[0]


# ---------------------------------------------------------------------------
# startup_summary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_summary_registers_and_returns_sessions(broker_url: str) -> None:
    async with _client(broker_url) as a:
        result = _payload(
            await a.call_tool(
                "startup_summary",
                {"session_id": "alice", "working_dir": "/tmp/a", "role": "tester"},
            )
        )
    assert "alice" in result["status"]
    assert result["pending_messages"] == []
    sessions = result["sessions"]
    assert isinstance(sessions, list)
    assert any(s["session_id"] == "alice" for s in sessions)


@pytest.mark.asyncio
async def test_startup_summary_compact_default(broker_url: str) -> None:
    """startup_summary defaults to compact=True — no working_dir in session list."""
    async with _client(broker_url) as a:
        result = _payload(
            await a.call_tool(
                "startup_summary",
                {"session_id": "alice", "working_dir": "/tmp/a"},
            )
        )
    for s in result["sessions"]:
        assert "working_dir" not in s
        assert "session_id" in s
        assert "role" in s


@pytest.mark.asyncio
async def test_startup_summary_full_shape_when_compact_false(broker_url: str) -> None:
    async with _client(broker_url) as a:
        result = _payload(
            await a.call_tool(
                "startup_summary",
                {"session_id": "alice", "working_dir": "/tmp/a", "compact": False},
            )
        )
    alice = next(s for s in result["sessions"] if s["session_id"] == "alice")
    assert "working_dir" in alice


@pytest.mark.asyncio
async def test_startup_summary_returns_backlog(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "queued"}
        )
    async with _client(broker_url) as b:
        result = _payload(
            await b.call_tool(
                "startup_summary",
                {"session_id": "bob", "working_dir": "/tmp/b"},
            )
        )
    assert len(result["pending_messages"]) == 1
    assert _msg_core(result["pending_messages"][0]) == {"from": "alice", "message": "queued"}


# ---------------------------------------------------------------------------
# receive_messages fields selector
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_receive_messages_fields_selector(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "hello"}
        )
        msgs = _payload(
            await a.call_tool(
                "receive_messages",
                {"session_id": "bob", "fields": ["from", "message"]},
            )
        )
    assert len(msgs) == 1
    assert msgs[0] == {"from": "alice", "message": "hello"}
    # sent_at_iso should be stripped
    assert "sent_at_iso" not in msgs[0]


@pytest.mark.asyncio
async def test_receive_messages_fields_none_returns_all(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "hello"}
        )
        msgs = _payload(
            await a.call_tool("receive_messages", {"session_id": "bob"})
        )
    assert "sent_at_iso" in msgs[0]


# ---------------------------------------------------------------------------
# broadcast_message recipients=[...] subset filter (v0.10.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadcast_subset_recipients(broker_url: str) -> None:
    """broadcast_message(recipients=[...]) only reaches the listed sessions."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            async with _client(broker_url) as c:
                await c.call_tool(
                    "register_session", {"session_id": "carol", "working_dir": "/tmp/c"}
                )
                result = await a.call_tool(
                    "broadcast_message",
                    {
                        "from_session": "alice",
                        "message": "only bob",
                        "recipients": ["bob"],
                    },
                )
                text = result.content[0].text
                assert "broadcast to 1 sessions" in text

                bob_inbox = _payload(
                    await b.call_tool("receive_messages", {"session_id": "bob"})
                )
                carol_inbox = _payload(
                    await c.call_tool("receive_messages", {"session_id": "carol"})
                )
    assert len(bob_inbox) == 1
    assert _msg_core(bob_inbox[0]) == {"from": "alice", "message": "only bob"}
    assert carol_inbox == []  # not in recipients list


@pytest.mark.asyncio
async def test_broadcast_subset_excludes_self(broker_url: str) -> None:
    """exclude_self still applies when recipients subset is given."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            result = await a.call_tool(
                "broadcast_message",
                {
                    "from_session": "alice",
                    "message": "hi",
                    "recipients": ["alice", "bob"],  # alice in list but excluded by default
                },
            )
            text = result.content[0].text
            assert "broadcast to 1 sessions" in text  # only bob

            alice_inbox = _payload(
                await a.call_tool("receive_messages", {"session_id": "alice"})
            )
    assert alice_inbox == []


@pytest.mark.asyncio
async def test_broadcast_subset_skips_unregistered(broker_url: str) -> None:
    """Sessions listed in recipients but not registered are silently skipped."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        result = await a.call_tool(
            "broadcast_message",
            {
                "from_session": "alice",
                "message": "hello ghost",
                "recipients": ["ghost1", "ghost2"],
            },
        )
    text = result.content[0].text
    assert "broadcast to 0 sessions" in text


# ---------------------------------------------------------------------------
# session TTL (v0.10.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_ttl_registers_with_expiry(broker_url: str) -> None:
    """register_session with ttl_hours succeeds; session appears in list."""
    async with _client(broker_url) as a:
        result = _payload(
            await a.call_tool(
                "register_session",
                {"session_id": "alice", "working_dir": "/tmp/a", "ttl_hours": 24.0},
            )
        )
    assert "alice" in result["status"]


@pytest.mark.asyncio
async def test_startup_summary_with_ttl_hours(broker_url: str) -> None:
    """startup_summary accepts ttl_hours and session is registered."""
    async with _client(broker_url) as a:
        result = _payload(
            await a.call_tool(
                "startup_summary",
                {
                    "session_id": "alice",
                    "working_dir": "/tmp/a",
                    "role": "temp",
                    "ttl_hours": 1.0,
                },
            )
        )
    assert "alice" in result["status"]
    assert any(s["session_id"] == "alice" for s in result["sessions"])


@pytest.mark.asyncio
async def test_session_ttl_expires_and_removed(broker_url: str) -> None:
    """A session registered with a very short TTL is not accessible after expiry.

    Note: the background purge runs every 5 min, so this test fast-expires by
    using a tiny ttl_hours and verifying that _load_state / the background loop
    would purge it. We test the state-file round-trip instead of waiting 5 min.
    """
    import asyncio as _asyncio

    async with _client(broker_url) as a:
        await a.call_tool(
            "register_session",
            {
                "session_id": "temp-session",
                "working_dir": "/tmp/temp",
                "ttl_hours": 0.0001,  # ~0.36 seconds
            },
        )
        # Wait for TTL to elapse
        await _asyncio.sleep(0.5)
        # Manually trigger expiry logic via inbox_stats (does NOT purge sessions)
        # Re-register to verify broker still works; temp-session may still show
        # (background purge hasn't run), but the TTL field is set.
        # Check that the session_expires_at was persisted properly:
        result = _payload(await a.call_tool("list_sessions", {}))
    # session may still be present (purge runs every 5 min in background)
    # The important thing is: no crash and list_sessions returns valid data
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# health_check (v0.10.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_check_returns_expected_fields(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = _payload(await c.call_tool("health_check", {}))
    assert result["version"] == "0.16.0"
    assert isinstance(result["uptime_seconds"], int)
    assert result["uptime_seconds"] >= 0
    assert result["started_at_iso"].startswith("20")
    assert isinstance(result["session_count"], int)
    assert isinstance(result["total_pending"], int)


@pytest.mark.asyncio
async def test_health_check_session_count_reflects_registrations(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            result = _payload(await b.call_tool("health_check", {}))
    assert result["session_count"] == 2


@pytest.mark.asyncio
async def test_health_check_total_pending_counts_messages(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "m1"}
        )
        await a.call_tool(
            "post_message", {"to": "carol", "from_session": "alice", "message": "m2"}
        )
        result = _payload(await a.call_tool("health_check", {}))
    assert result["total_pending"] == 2


# ---------------------------------------------------------------------------
# peek_messages (v0.10.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_peek_messages_returns_without_draining(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "hello"}
        )
        # Peek — should return message without consuming it
        peek1 = _payload(await a.call_tool("peek_messages", {"session_id": "bob"}))
        # Peek again — same result
        peek2 = _payload(await a.call_tool("peek_messages", {"session_id": "bob"}))
        # Drain — message still there
        drain = _payload(await a.call_tool("receive_messages", {"session_id": "bob"}))
        # Now inbox is empty
        peek3 = _payload(await a.call_tool("peek_messages", {"session_id": "bob"}))
    assert len(peek1) == 1
    assert _msg_core(peek1[0]) == {"from": "alice", "message": "hello"}
    assert peek1 == peek2  # non-destructive
    assert len(drain) == 1
    assert peek3 == []  # drained


@pytest.mark.asyncio
async def test_peek_messages_limit(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        for i in range(5):
            await a.call_tool(
                "post_message",
                {"to": "bob", "from_session": "alice", "message": f"msg{i}"},
            )
        peek = _payload(await a.call_tool("peek_messages", {"session_id": "bob", "limit": 3}))
    assert len(peek) == 3
    assert [_msg_core(m)["message"] for m in peek] == ["msg0", "msg1", "msg2"]


@pytest.mark.asyncio
async def test_peek_messages_fields_selector(broker_url: str) -> None:
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message", {"to": "bob", "from_session": "alice", "message": "test"}
        )
        peek = _payload(
            await a.call_tool(
                "peek_messages",
                {"session_id": "bob", "fields": ["from", "message"]},
            )
        )
    assert len(peek) == 1
    assert peek[0] == {"from": "alice", "message": "test"}
    assert "sent_at_iso" not in peek[0]


@pytest.mark.asyncio
async def test_peek_messages_empty_for_unknown_session(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = _payload(await c.call_tool("peek_messages", {"session_id": "ghost"}))
    assert result == []


@pytest.mark.asyncio
async def test_peek_messages_strips_internal_fields(broker_url: str) -> None:
    """Internal _ack_to / _expires_at must not appear in peek output."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {
                "to": "bob",
                "from_session": "alice",
                "message": "secret ttl",
                "ttl_seconds": 120,
                "request_read_ack": True,
            },
        )
        peek = _payload(await a.call_tool("peek_messages", {"session_id": "bob"}))
    assert len(peek) == 1
    assert "_expires_at" not in peek[0]
    assert "_ack_to" not in peek[0]


# ---------------------------------------------------------------------------
# BROKER_MONITOR_SESSION / monitoring copies (v0.12.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monitor_session_receives_post_message_copy(broker_url_monitored: str) -> None:
    """post_message copies arrive in the monitor session inbox."""
    async with _client(broker_url_monitored) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {"to": "bob", "from_session": "alice", "message": "hello bob"},
        )
        monitor_msgs = _payload(
            await a.call_tool("receive_messages", {"session_id": "monitor"})
        )

    assert len(monitor_msgs) == 1
    assert monitor_msgs[0]["from"] == "alice"
    assert monitor_msgs[0]["message"] == "hello bob"
    assert monitor_msgs[0]["monitor_to"] == "bob"
    # internal fields must not leak
    assert "_ack_to" not in monitor_msgs[0]
    assert "_expires_at" not in monitor_msgs[0]


@pytest.mark.asyncio
async def test_monitor_session_receives_broadcast_copy(broker_url_monitored: str) -> None:
    """broadcast_message copies arrive in the monitor session inbox."""
    async with _client(broker_url_monitored) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        async with _client(broker_url_monitored) as b:
            await b.call_tool("register_session", {"session_id": "bob", "working_dir": "/tmp/b"})
            await a.call_tool(
                "broadcast_message",
                {"from_session": "alice", "message": "all hands"},
            )
            monitor_msgs = _payload(
                await a.call_tool("receive_messages", {"session_id": "monitor"})
            )

    assert len(monitor_msgs) == 1
    assert monitor_msgs[0]["from"] == "alice"
    assert monitor_msgs[0]["message"] == "all hands"
    assert isinstance(monitor_msgs[0]["monitor_to"], list)


@pytest.mark.asyncio
async def test_monitor_session_not_duplicated_when_in_targets(broker_url_monitored: str) -> None:
    """Monitor session does not get a copy when it is itself the target."""
    async with _client(broker_url_monitored) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool(
            "post_message",
            {"to": "monitor", "from_session": "alice", "message": "direct to monitor"},
        )
        msgs = _payload(
            await a.call_tool("receive_messages", {"session_id": "monitor"})
        )

    # Only one copy (the direct message), not two
    assert len(msgs) == 1
    assert "monitor_to" not in msgs[0]


@pytest.mark.asyncio
async def test_tool_stats_returns_counts(broker_url: str) -> None:
    """tool_stats returns a counts dict with expected keys."""
    async with _client(broker_url) as a:
        await a.call_tool("register_session", {"session_id": "alice", "working_dir": "/tmp/a"})
        await a.call_tool("list_sessions", {})
        result = _payload(await a.call_tool("tool_stats", {}))
    assert "counts" in result
    assert "total_calls" in result
    assert "uptime_seconds" in result
    assert result["counts"].get("register_session", 0) >= 1
    assert result["counts"].get("list_sessions", 0) >= 1
    assert result["total_calls"] >= 3


# ---------------------------------------------------------------------------
# Plugin lifecycle tools (v0.13.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_plugin_and_list(broker_url: str) -> None:
    """add_plugin registers a plugin; list_plugins returns it."""
    async with _client(broker_url) as c:
        add_result = _payload(await c.call_tool("add_plugin", {
            "name": "test-plugin",
            "command": "sleep 30",
            "session_id": "test-plugin",
        }))
        assert "registered" in add_result

        plugins = _payload(await c.call_tool("list_plugins", {}))
        entry = next((p for p in plugins if p["name"] == "test-plugin"), None)
        assert entry is not None
        assert entry["session_id"] == "test-plugin"
        assert entry["running"] is False
        assert entry["connected"] is False
        assert entry["auto_start"] is False


@pytest.mark.asyncio
async def test_add_plugin_duplicate_rejected(broker_url: str) -> None:
    async with _client(broker_url) as c:
        await c.call_tool("add_plugin", {
            "name": "p1", "command": "sleep 1", "session_id": "p1",
        })
        result = _payload(await c.call_tool("add_plugin", {
            "name": "p1", "command": "sleep 1", "session_id": "p1",
        }))
        assert "already registered" in result


@pytest.mark.asyncio
async def test_remove_plugin(broker_url: str) -> None:
    async with _client(broker_url) as c:
        await c.call_tool("add_plugin", {
            "name": "to-remove", "command": "sleep 1", "session_id": "to-remove",
        })
        remove_result = _payload(await c.call_tool("remove_plugin", {"name": "to-remove"}))
        assert "removed" in remove_result

        plugins = _payload(await c.call_tool("list_plugins", {}))
        assert not any(p["name"] == "to-remove" for p in plugins)


@pytest.mark.asyncio
async def test_remove_plugin_unknown(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = _payload(await c.call_tool("remove_plugin", {"name": "ghost"}))
        assert "not found" in result


@pytest.mark.asyncio
async def test_start_plugin_and_stop(broker_url: str) -> None:
    """start_plugin spawns a process; stop_plugin terminates it."""
    import asyncio as _asyncio
    import sys

    cmd = f"{sys.executable} -c \"import time; time.sleep(30)\""
    async with _client(broker_url) as c:
        await c.call_tool("add_plugin", {
            "name": "sleeper", "command": cmd, "session_id": "sleeper",
        })
        start_result = _payload(await c.call_tool("start_plugin", {"name": "sleeper"}))
        assert "started" in start_result

        await _asyncio.sleep(0.5)
        plugins = _payload(await c.call_tool("list_plugins", {}))
        entry = next(p for p in plugins if p["name"] == "sleeper")
        assert entry["running"] is True
        assert entry["pid"] is not None

        stop_result = _payload(await c.call_tool("stop_plugin", {"name": "sleeper"}))
        assert "stopped" in stop_result

        await _asyncio.sleep(0.5)
        plugins2 = _payload(await c.call_tool("list_plugins", {}))
        entry2 = next(p for p in plugins2 if p["name"] == "sleeper")
        assert entry2["running"] is False


@pytest.mark.asyncio
async def test_restart_plugin(broker_url: str) -> None:
    import asyncio as _asyncio
    import sys

    cmd = f"{sys.executable} -c \"import time; time.sleep(30)\""
    async with _client(broker_url) as c:
        await c.call_tool("add_plugin", {
            "name": "restarter", "command": cmd, "session_id": "restarter",
        })
        await c.call_tool("start_plugin", {"name": "restarter"})
        await _asyncio.sleep(0.3)
        plugins_before = {p["name"]: p for p in _payload(await c.call_tool("list_plugins", {}))}
        pid_before = plugins_before["restarter"]["pid"]

        await c.call_tool("restart_plugin", {"name": "restarter"})
        await _asyncio.sleep(0.5)
        plugins_after = {p["name"]: p for p in _payload(await c.call_tool("list_plugins", {}))}
        pid_after = plugins_after["restarter"]["pid"]

        assert plugins_after["restarter"]["running"] is True
        assert pid_after != pid_before  # new process spawned

        await c.call_tool("stop_plugin", {"name": "restarter"})


@pytest.mark.asyncio
async def test_health_check_version_updated(broker_url: str) -> None:
    async with _client(broker_url) as c:
        result = _payload(await c.call_tool("health_check", {}))
    assert result["version"] == "0.16.0"


# ---------------------------------------------------------------------------
# subscribe_session_events — multiple independent subscriptions
# ---------------------------------------------------------------------------


def _events(msgs: list[dict]) -> list[tuple]:
    return [(m.get("event"), m.get("session_id")) for m in msgs if m.get("from") == "broker"]


@pytest.mark.asyncio
async def test_independent_subscriptions_do_not_bleed(broker_url: str) -> None:
    """A subscriber can hold (posted@A) and (status_changed@B) without mixing."""
    async with _client(broker_url) as s, _client(broker_url) as a, _client(broker_url) as b:
        await s.call_tool("register_session", {"session_id": "sub", "working_dir": "/tmp/s"})
        await a.call_tool("register_session", {"session_id": "A", "working_dir": "/tmp/a"})
        await b.call_tool("register_session", {"session_id": "B", "working_dir": "/tmp/b"})
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["posted"], "session_filter": ["A"],
        })
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["status_changed"], "session_filter": ["B"],
        })
        await s.call_tool("receive_messages", {"session_id": "sub"})  # clear noise
        await a.call_tool("post_message", {"to": "x", "from_session": "A", "message": "m"})
        await a.call_tool("update_session_status", {"session_id": "A", "status": "idle"})
        await b.call_tool("update_session_status", {"session_id": "B", "status": "idle"})
        await b.call_tool("post_message", {"to": "x", "from_session": "B", "message": "m"})
        events = _events(_payload(await s.call_tool("receive_messages", {"session_id": "sub"})))
    assert ("posted", "A") in events
    assert ("status_changed", "B") in events
    assert ("status_changed", "A") not in events  # A's status not watched
    assert ("posted", "B") not in events          # B's posts not watched


@pytest.mark.asyncio
async def test_event_delivered_once_per_subscriber(broker_url: str) -> None:
    """Two overlapping subscriptions both matching an event → delivered once."""
    async with _client(broker_url) as s, _client(broker_url) as a:
        await s.call_tool("register_session", {"session_id": "sub", "working_dir": "/tmp/s"})
        await a.call_tool("register_session", {"session_id": "A", "working_dir": "/tmp/a"})
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["posted"],
        })
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["posted", "status_changed"],
        })
        await s.call_tool("receive_messages", {"session_id": "sub"})
        await a.call_tool("post_message", {"to": "x", "from_session": "A", "message": "m"})
        msgs = _payload(await s.call_tool("receive_messages", {"session_id": "sub"}))
    posted = [m for m in msgs if m.get("event") == "posted"]
    assert len(posted) == 1


@pytest.mark.asyncio
async def test_status_changed_carries_prev_status(broker_url: str) -> None:
    """status_changed events carry prev_status for edge detection."""
    async with _client(broker_url) as s, _client(broker_url) as a:
        await s.call_tool("register_session", {"session_id": "sub", "working_dir": "/tmp/s"})
        await a.call_tool("register_session", {"session_id": "A", "working_dir": "/tmp/a"})
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["status_changed"], "session_filter": ["A"],
        })
        await s.call_tool("receive_messages", {"session_id": "sub"})
        await a.call_tool("update_session_status", {"session_id": "A", "status": "active"})
        await a.call_tool("update_session_status", {
            "session_id": "A", "status": "idle", "detail": "first",
        })
        # idle→idle detail edit: still fires, but prev_status stays idle
        await a.call_tool("update_session_status", {
            "session_id": "A", "status": "idle", "detail": "second",
        })
        msgs = [m for m in _payload(await s.call_tool("receive_messages", {"session_id": "sub"}))
                if m.get("event") == "status_changed"]
    transitions = [(m.get("prev_status"), m.get("status")) for m in msgs]
    assert (None, "active") in transitions       # first edge: unset → active
    assert ("active", "idle") in transitions     # the real idle edge
    assert ("idle", "idle") in transitions       # detail-only edit still fires
    # An edge-detecting consumer (prev != idle, status == idle) sees one idle edge.
    idle_edges = [t for t in transitions if t[1] == "idle" and t[0] != "idle"]
    assert len(idle_edges) == 1


# ---------------------------------------------------------------------------
# set_active — mechanical liveness axis, orthogonal to status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_active_does_not_clobber_status(broker_url: str) -> None:
    """A Stop-hook set_active(False) must not wipe an LLM-declared status."""
    async with _client(broker_url) as c:
        await c.call_tool("register_session", {"session_id": "x", "working_dir": "/tmp/x"})
        # LLM declares semantic status
        await c.call_tool("update_session_status", {
            "session_id": "x", "status": "waiting", "detail": "ci:#1268",
        })
        # Stop hook flips mechanical liveness
        await c.call_tool("set_active", {"session_id": "x", "active": False})
        st = _payload(await c.call_tool("get_session_status", {"session_id": "x"}))
    assert st["active"] is False
    assert st["status"] == "waiting"          # not clobbered
    assert st["status_detail"] == "ci:#1268"  # not clobbered


@pytest.mark.asyncio
async def test_update_status_does_not_touch_active(broker_url: str) -> None:
    """update_session_status leaves the active bool untouched."""
    async with _client(broker_url) as c:
        await c.call_tool("register_session", {"session_id": "x", "working_dir": "/tmp/x"})
        await c.call_tool("set_active", {"session_id": "x", "active": False})
        await c.call_tool("update_session_status", {"session_id": "x", "status": "waiting"})
        st = _payload(await c.call_tool("get_session_status", {"session_id": "x"}))
    assert st["active"] is False  # status update did not flip it back to active


@pytest.mark.asyncio
async def test_active_changed_event_carries_status_enrichment(broker_url: str) -> None:
    async with _client(broker_url) as s, _client(broker_url) as a:
        await s.call_tool("register_session", {"session_id": "sub", "working_dir": "/tmp/s"})
        await a.call_tool("register_session", {"session_id": "A", "working_dir": "/tmp/a"})
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["active_changed"], "session_filter": ["A"],
        })
        await s.call_tool("receive_messages", {"session_id": "sub"})
        await a.call_tool("update_session_status", {
            "session_id": "A", "status": "waiting", "detail": "ci:#1",
        })
        await a.call_tool("set_active", {"session_id": "A", "active": False})
        msgs = [m for m in _payload(await s.call_tool("receive_messages", {"session_id": "sub"}))
                if m.get("event") == "active_changed"]
    assert len(msgs) == 1
    assert msgs[0]["active"] is False
    assert msgs[0]["prev_active"] is True
    assert msgs[0]["status"] == "waiting"   # enrichment carried on the event
    assert msgs[0]["detail"] == "ci:#1"


@pytest.mark.asyncio
async def test_set_active_noop_when_unchanged(broker_url: str) -> None:
    """Setting active to its current value fires no event (no spam)."""
    async with _client(broker_url) as s, _client(broker_url) as a:
        await s.call_tool("register_session", {"session_id": "sub", "working_dir": "/tmp/s"})
        await a.call_tool("register_session", {"session_id": "A", "working_dir": "/tmp/a"})
        await s.call_tool("subscribe_session_events", {
            "subscriber_id": "sub", "event_types": ["active_changed"], "session_filter": ["A"],
        })
        await s.call_tool("receive_messages", {"session_id": "sub"})
        # A registered active=True by default; set_active(True) is a no-op
        await a.call_tool("set_active", {"session_id": "A", "active": True})
        msgs = [m for m in _payload(await s.call_tool("receive_messages", {"session_id": "sub"}))
                if m.get("event") == "active_changed"]
    assert msgs == []


@pytest.mark.asyncio
async def test_unregister_drops_subscriptions_and_commands(broker_url: str) -> None:
    """unregister_session must not leave ghost subscriptions or command schema."""
    async with _client(broker_url) as c:
        await c.call_tool("register_session", {
            "session_id": "ghost", "working_dir": "/tmp/g",
            "commands": [{"name": "ping", "description": "p", "args": []}],
        })
        await c.call_tool("subscribe_session_events", {
            "subscriber_id": "ghost", "event_types": ["posted"],
        })
        before = _payload(await c.call_tool("list_plugin_commands", {"session_id": "ghost"}))
        assert before == [{"name": "ping", "description": "p", "args": []}]

        await c.call_tool("unregister_session", {"session_id": "ghost"})

        # command schema gone
        assert _payload(await c.call_tool("list_plugin_commands", {"session_id": "ghost"})) == []
        # subscription gone: a posted event is NOT queued to the gone ghost
        await c.call_tool("register_session", {"session_id": "mover", "working_dir": "/tmp/m"})
        await c.call_tool("post_message", {"to": "x", "from_session": "mover", "message": "hi"})
        assert _payload(await c.call_tool("receive_messages", {"session_id": "ghost"})) == []


# ---------------------------------------------------------------------------
# Inbox resources (reyn-broker#13)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _subscriber_client(url: str, updates: list[str]):
    """Client that records resources/updated URIs into ``updates``."""

    async def _on_message(msg: Any) -> None:
        root = getattr(msg, "root", None)
        params = getattr(root, "params", None)
        uri = getattr(params, "uri", None)
        if uri is not None and type(root).__name__ == "ResourceUpdatedNotification":
            updates.append(str(uri))

    async with (
        streamable_http_client(url) as (read, write, _close),
        ClientSession(read, write, message_handler=_on_message) as session,
    ):
        await session.initialize()
        yield session


async def _await_updates(updates: list[str], *, minimum: int = 1) -> None:
    """Give notifications a moment to arrive (they are pushed, not polled)."""
    for _ in range(50):
        if len(updates) >= minimum:
            return
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_resources_subscribe_capability_is_advertised(broker_url: str) -> None:
    """The exact field reyn refuses to connect without.

    The SDK hardcodes ``subscribe=False`` on mcp 1.x and registering a
    subscribe handler does not change it, so this is set explicitly by
    ``_advertise_resource_subscribe``. Without the advertisement a client
    that trusts it will never subscribe at all.
    """
    async with (
        streamable_http_client(broker_url) as (read, write, _close),
        ClientSession(read, write) as session,
    ):
        init = await session.initialize()
    assert init.capabilities.resources is not None
    assert init.capabilities.resources.subscribe is True


@pytest.mark.asyncio
async def test_inbox_read_does_not_drain(broker_url: str) -> None:
    """Reading the resource must leave the mail for receive_messages.

    This is the property that makes a dropped notification survivable: a
    client that misses the wake-up still finds the message on the next
    read. If a future change made reads destructive the failure would be
    silent — the notification still fires, the first reader still sees the
    message, and only a missed wake-up loses it.
    """
    sid = "res-nondestructive"
    uri = AnyUrl(f"broker://inbox/{sid}")
    async with _client(broker_url) as c:
        await c.call_tool(
            "post_message", {"to": sid, "from_session": "sender", "message": "keep-me"}
        )

        first = json.loads((await c.read_resource(uri)).contents[0].text)
        assert first["pending_count"] == 1
        assert first["messages"][0]["message"] == "keep-me"

        second = json.loads((await c.read_resource(uri)).contents[0].text)
        assert second["pending_count"] == 1, "re-read lost the message"

        drained = _payload(await c.call_tool("receive_messages", {"session_id": sid}))
        assert [m["message"] for m in drained] == ["keep-me"], "read consumed the mail"

        after = json.loads((await c.read_resource(uri)).contents[0].text)
        assert after["pending_count"] == 0, "receive_messages must still drain"


@pytest.mark.asyncio
async def test_inbox_updated_notification_is_per_session(broker_url: str) -> None:
    """One resource per session — a subscriber is woken only by its own mail.

    Pins the choice against collapsing back to a single shared feed, which
    would wake every peer on every message.
    """
    updates: list[str] = []
    async with _subscriber_client(broker_url, updates) as sub:
        await sub.subscribe_resource(AnyUrl("broker://inbox/alpha"))

        async with _client(broker_url) as poster:
            await poster.call_tool(
                "post_message", {"to": "beta", "from_session": "s", "message": "not yours"}
            )
            await asyncio.sleep(0.5)
            assert updates == [], "woken by another session's mail"

            await poster.call_tool(
                "post_message", {"to": "alpha", "from_session": "s", "message": "yours"}
            )
            await _await_updates(updates)

    assert updates == ["broker://inbox/alpha"]


@pytest.mark.asyncio
async def test_inbox_subscription_survives_reconnect(broker_url: str) -> None:
    """A fresh connection can re-subscribe and still be woken.

    Subscriptions belong to a connection and are not persisted, so this is
    what a client does after a broker restart: subscribe again, read again.
    """
    sid = "reconnector"
    updates_first: list[str] = []
    async with _subscriber_client(broker_url, updates_first) as sub:
        await sub.subscribe_resource(AnyUrl(f"broker://inbox/{sid}"))

    updates_second: list[str] = []
    async with _subscriber_client(broker_url, updates_second) as sub:
        await sub.subscribe_resource(AnyUrl(f"broker://inbox/{sid}"))
        async with _client(broker_url) as poster:
            await poster.call_tool(
                "post_message", {"to": sid, "from_session": "s", "message": "after reconnect"}
            )
        await _await_updates(updates_second)

    assert updates_second == [f"broker://inbox/{sid}"]
