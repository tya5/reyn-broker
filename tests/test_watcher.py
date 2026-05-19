"""Unit tests for session_watcher.py emit logic.

These tests exercise the truncation / journal behaviour of
``_emit_message`` without standing up a broker subprocess. The watcher's
poll loop is integration-tested via ``test_broker.py`` fixtures.
"""

from __future__ import annotations

import importlib
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

import pytest


@pytest.fixture
def watcher_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Import session_watcher with the journal base pointing at tmp."""
    monkeypatch.setenv("BROKER_INBOX_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.setenv("BROKER_WATCHER_MAX_INLINE", "200")
    import session_watcher

    return importlib.reload(session_watcher)


def _emit_capture(watcher_module, session_id: str, msg: dict) -> dict:
    buf = io.StringIO()
    with redirect_stdout(buf):
        watcher_module._emit_message(session_id, msg)
    lines = [ln for ln in buf.getvalue().splitlines() if ln]
    assert len(lines) == 1, f"expected exactly 1 emitted line, got {lines}"
    return json.loads(lines[0])


def test_short_message_emitted_as_is(watcher_module, tmp_path: Path) -> None:
    msg = {"from": "alice", "message": "ping"}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert emitted == msg
    assert "_truncated" not in emitted
    assert "_full_path" not in emitted


def test_short_message_still_journaled(watcher_module, tmp_path: Path) -> None:
    msg = {"from": "alice", "message": "ping"}
    _emit_capture(watcher_module, "bob", msg)
    journal_dir = tmp_path / "journal" / "bob"
    files = list(journal_dir.iterdir())
    assert len(files) == 1
    assert json.loads(files[0].read_text()) == msg


def test_long_message_emits_summary_with_full_path(watcher_module, tmp_path: Path) -> None:
    long_body = "x" * 1000
    msg = {"from": "alice", "message": long_body}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert emitted["from"] == "alice"
    assert emitted["_truncated"] is True
    assert emitted["_body_chars"] == 1000
    assert "_full_path" in emitted
    journal_file = Path(emitted["_full_path"])
    assert journal_file.exists()
    assert json.loads(journal_file.read_text()) == msg
    # Summary must be much smaller than the original full body, regardless
    # of how long the journal path is on this filesystem.
    assert len(json.dumps(emitted)) < len(json.dumps(msg))


def test_summary_marker_mentions_sender_and_size(watcher_module) -> None:
    long_body = "y" * 500
    msg = {"from": "carol", "message": long_body}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert "carol" in emitted["message"]
    assert "500 chars" in emitted["message"]


def test_threshold_boundary_short_path(watcher_module) -> None:
    """A message whose JSON is exactly at the threshold should still emit full."""
    cap = watcher_module.MAX_INLINE_BODY
    base = json.dumps({"from": "alice", "message": ""})
    body_chars = cap - len(base) - 2  # leave room for the closing chars
    msg = {"from": "alice", "message": "z" * body_chars}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert emitted == msg


def test_threshold_boundary_long_path(watcher_module) -> None:
    """One char over the threshold should switch to summary mode."""
    cap = watcher_module.MAX_INLINE_BODY
    base = json.dumps({"from": "alice", "message": ""})
    body_chars = cap - len(base) + 2  # push just over
    msg = {"from": "alice", "message": "z" * body_chars}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert emitted.get("_truncated") is True


def test_unsafe_sender_chars_sanitized_in_filename(watcher_module, tmp_path: Path) -> None:
    msg = {"from": "../etc/passwd", "message": "hello"}
    _emit_capture(watcher_module, "bob", msg)
    journal_dir = tmp_path / "journal" / "bob"
    files = list(journal_dir.iterdir())
    assert len(files) == 1
    # Should not contain path-separator characters
    assert "/" not in files[0].name
    assert "etc" in files[0].name or "_" in files[0].name  # sanitised form


def test_journal_failure_falls_back_to_pointerless_summary(
    watcher_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If journal write fails, summary still emitted (without _full_path)."""
    monkeypatch.setattr(watcher_module, "_write_journal", lambda *_a, **_k: False)
    long_body = "x" * 1000
    msg = {"from": "alice", "message": long_body}
    emitted = _emit_capture(watcher_module, "bob", msg)
    assert emitted["_truncated"] is True
    assert "_full_path" not in emitted
    assert "receive_messages" in emitted["message"]
