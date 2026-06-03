#!/usr/bin/env python3
"""Inbox watcher for broker-mediated Claude Code sessions.

Polls ``receive_messages`` every ``POLL_S`` seconds and emits one stdout
line per arrived message (JSON-encoded). Designed to be wrapped by the
Claude Code Monitor tool so each new broker message arrives as a
``<task-notification>`` event — i.e. the LLM context gets woken up even
when the Claude Code session is otherwise idle.

Usage (typically as a Monitor command):

    /path/to/broker/.venv/bin/python /path/to/broker/session_watcher.py \
        --session=<id> [--fields from,message]

IMPORTANT: run with the broker's own virtualenv Python so that the ``mcp``
package is available. Using the system ``python3`` will fail with
``ModuleNotFoundError: No module named 'mcp'``.

Output (per arrived message, one JSON line):

    {"from": "<sender>", "message": "<body>"}

For messages whose JSON encoding exceeds ``MAX_INLINE_BODY`` characters,
the watcher writes the full body to a per-session journal file and emits
a *summary* line instead, with ``_truncated: true``, ``_full_path``
pointing at the journal file, and ``_preview`` containing the first N
characters of the body inline. Recipients can use ``_preview`` for quick
routing decisions and fall back to ``_full_path`` for the full body.
This prevents Claude Code's Monitor event-body cap from silently losing
the tail of a long message.

Operational notes:
  - The watcher does NOT call register_session — that's the Claude Code
    session's job. The watcher only drains via receive_messages.
  - At most one watcher per session_id is allowed. This is enforced with an
    ``flock`` on a per-session lock file at startup: a second watcher for the
    same session_id fails to acquire the lock and exits immediately (code 2).
    Without this, concurrent watchers race over the inbox and a message
    drained by the wrong one is silently lost to the intended consumer.
  - Recovery: the loser's stderr names the holder's pid. If that watcher is
    genuinely stale, ``kill <pid>`` and relaunch — the lock auto-releases on
    process exit (kernel-managed), so no stale lock is left behind and the
    relaunch succeeds. If the holder is healthy, the loser was the mistake
    (you already have a working watcher); do nothing.
  - Transient broker outages back off ``ERROR_BACKOFF_S`` and stay quiet
    until 5 consecutive failures, then emit a single ``_watcher_error``
    line so the Claude Code session can see something is wrong.
  - **Batching**: when one poll returns N messages, this script emits N
    stdout lines back-to-back. Claude Code's Monitor tool batches stdout
    lines arriving within ~200ms into a single ``<task-notification>``
    event, so the receiving LLM may see multiple JSON lines concatenated
    in one event body. Recipients should parse the event body
    line-by-line (each line is one complete message JSON).
  - **Journal files** accumulate in ``$BROKER_INBOX_JOURNAL_DIR`` (default
    ``/tmp/reyn-broker-inbox/<session_id>/``). They are not automatically
    cleaned; ``/tmp`` is typically wiped at reboot on most systems. Delete
    manually if disk pressure is a concern.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import re
import sys
import time
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

BROKER_URL = "http://127.0.0.1:8765/mcp"
POLL_S = 30
ERROR_BACKOFF_S = 10
ERROR_QUIET_THRESHOLD = 5  # stay quiet for first N consecutive errors

JOURNAL_BASE = Path(os.environ.get("BROKER_INBOX_JOURNAL_DIR", "/tmp/reyn-broker-inbox"))
MAX_INLINE_BODY = int(os.environ.get("BROKER_WATCHER_MAX_INLINE", "400"))

_SAFE_SENDER_RE = re.compile(r"[^a-zA-Z0-9_-]")


def _acquire_single_instance_lock(session_id: str):
    """Take an exclusive per-session lock so only one watcher runs per session.

    Two watchers polling the same ``session_id`` race over the inbox: whichever
    calls ``receive_messages`` first drains the message and the other never sees
    it. Because each watcher's stdout feeds a *different* Monitor task (or
    /dev/null for a stray ``nohup`` process), a message drained by the wrong
    watcher is silently lost from the intended consumer's perspective.

    We make duplicate starts impossible: the second instance fails to acquire
    the lock and exits immediately. The lock is an ``flock`` on a per-session
    file; it releases automatically when the process exits (or the fd closes),
    so a crashed watcher does not leave a stale lock behind.

    Returns the held lock file object. The caller MUST keep a reference for the
    process lifetime — letting it be garbage-collected would close the fd and
    release the lock.
    """
    safe = _SAFE_SENDER_RE.sub("_", session_id)[:64] or "unknown"
    JOURNAL_BASE.mkdir(parents=True, exist_ok=True)
    lock_path = JOURNAL_BASE / f".watcher-{safe}.lock"
    # Open with "a+" (not "w") so we do NOT truncate the holder's pid before we
    # know whether we won the lock — a loser must still be able to read it.
    lock_fh = open(lock_path, "a+")  # noqa: SIM115 — held for process lifetime, not a with-block
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_fh.seek(0)
        holder_pid = lock_fh.read().strip() or "unknown"
        lock_fh.close()
        sys.stderr.write(
            f"[session_watcher] a watcher for session '{session_id}' is already "
            f"running (pid {holder_pid}, lock: {lock_path}). Refusing to start a "
            f"second instance — concurrent watchers race over the inbox and "
            f"silently drop messages. If that pid is stale, kill it and retry.\n"
        )
        sys.exit(2)
    # We hold the lock: now it is safe to record our pid for any future loser.
    lock_fh.seek(0)
    lock_fh.truncate()
    lock_fh.write(str(os.getpid()))
    lock_fh.flush()
    return lock_fh


def _emit(payload: dict) -> None:
    """One JSON line to stdout, flushed. Used for raw events (e.g. errors)."""
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def _journal_path(session_id: str, sender: str, ts_ms: int) -> Path:
    sender_safe = _SAFE_SENDER_RE.sub("_", sender)[:64] or "unknown"
    return JOURNAL_BASE / session_id / f"msg-{ts_ms}-{sender_safe}.json"


def _write_journal(path: Path, body: str) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        return True
    except OSError:
        return False


def _emit_message(session_id: str, msg: dict) -> None:
    """Emit one inbox message.

    The full message JSON is always written to a journal file first so
    recipients have a guaranteed recovery path. If the JSON exceeds
    ``MAX_INLINE_BODY`` characters, a short summary referencing the
    journal path is emitted instead of the full payload.

    The summary always includes a ``_preview`` field with the first N
    characters of the body that fit within the ``MAX_INLINE_BODY``
    budget, so recipients can make routing decisions without a separate
    ``Read`` round-trip in the common case. Use ``_full_path`` for the
    complete body.
    """
    full_json = json.dumps(msg, ensure_ascii=False)
    sender = str(msg.get("from", "unknown"))
    body = str(msg.get("message", ""))
    ts_ms = int(time.time() * 1000)

    path = _journal_path(session_id, sender, ts_ms)
    journal_ok = _write_journal(path, full_json)

    if len(full_json) <= MAX_INLINE_BODY:
        print(full_json, flush=True)
        return

    if journal_ok:
        marker = (
            f"[long message from {sender}, {len(body)} chars — "
            f"full text at {path}]"
        )
        summary: dict = {
            "from": sender,
            "message": marker,
            "_truncated": True,
            "_full_path": str(path),
            "_body_chars": len(body),
        }
        # Fit an inline preview within the remaining MAX_INLINE_BODY budget.
        # Compute how many raw body chars fit after accounting for the base
        # JSON and the key/quotes/comma overhead of adding "_preview":"...".
        base_len = len(json.dumps(summary, ensure_ascii=False))
        # overhead: ,"_preview":"" → 13 chars, plus closing } already counted
        preview_budget = MAX_INLINE_BODY - base_len - 13
        if body and preview_budget > 20:
            summary["_preview"] = body[:preview_budget]
    else:
        # Journal failed; emit a marker pointing the recipient at receive_messages
        marker = (
            f"[long message from {sender}, {len(body)} chars — "
            f"journal write failed, re-fetch via receive_messages]"
        )
        summary = {
            "from": sender,
            "message": marker,
            "_truncated": True,
            "_body_chars": len(body),
        }
    print(json.dumps(summary, ensure_ascii=False), flush=True)


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


async def _poll_loop(session_id: str, fields: list[str] | None) -> None:
    consecutive_errors = 0
    call_args: dict = {"session_id": session_id}
    if fields is not None:
        call_args["fields"] = fields
    while True:
        try:
            async with (
                streamable_http_client(BROKER_URL) as (read, write, _close),
                ClientSession(read, write) as cs,
            ):
                await cs.initialize()
                consecutive_errors = 0
                while True:
                    result = await cs.call_tool("receive_messages", call_args)
                    for msg in _parse_messages(result):
                        _emit_message(session_id, msg)
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
    parser.add_argument(
        "--fields",
        default=None,
        help=(
            "Comma-separated list of message fields to request from broker "
            "(e.g. 'from,message'). Omitting metadata fields reduces token overhead. "
            "Default: all fields."
        ),
    )
    args = parser.parse_args()
    fields = [f.strip() for f in args.fields.split(",")] if args.fields else None
    # Held for the process lifetime; prevents a second watcher for this session.
    lock_fh = _acquire_single_instance_lock(args.session)
    try:
        asyncio.run(_poll_loop(args.session, fields))
    except KeyboardInterrupt:
        return 130
    finally:
        lock_fh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
