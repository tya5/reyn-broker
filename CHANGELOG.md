# Changelog

All notable changes to reyn-broker are documented in this file.

## [0.8.0] - 2026-05-28

### Added
- **Inline preview on truncated notifications** (`session_watcher.py`). When a
  message exceeds `MAX_INLINE_BODY`, the summary line now includes a `_preview`
  field with the first ~100 chars of the body (as many as fit within the
  `MAX_INLINE_BODY` budget). Recipients can use `_preview` for routing decisions
  without a separate `Read` round-trip; `_full_path` is still available for the
  complete body.
- **Multi-recipient `post_message`** — new optional `recipients: list[str]`
  parameter. When provided, the message is queued in every listed session's
  inbox in one call (supersedes `to`). Returns a summary of online/offline
  counts. Backward compatible: omitting `recipients` keeps the original `to`
  single-target behaviour.
- **Message TTL** — new optional `ttl_seconds: int` parameter on `post_message`.
  Sets an expiry timestamp (`_expires_at`) on the queued message. Expired
  messages are silently dropped on the next `receive_messages` drain or
  `inbox_stats` call. The `_expires_at` field is stripped from delivered
  messages and never reaches recipients.

### Changed
- `SESSION_GUIDE.md` — Monitor command updated to use the broker venv Python
  (`/path/to/broker/.venv/bin/python`) with an explicit warning that the system
  `python3` will fail with `ModuleNotFoundError: No module named 'mcp'`.
- `SESSION_GUIDE.md` — truncated notification handling updated to document the
  new `_preview` field.
- `README.md` / `SESSION_GUIDE.md` — documented MCP tool schema cache behaviour
  after broker restart: `post_message` signature changed (added `recipients`,
  `ttl_seconds`), so sessions started against an older broker will hold a stale
  schema. Fix: `ToolSearch(query="select:mcp__broker__post_message")` to refresh,
  or restart the Claude Code session.

### ⚠️ Schema change notice
`post_message` gained two new optional parameters (`recipients`, `ttl_seconds`).
Claude Code sessions that were running before this restart may see tool-call
failures on `post_message`. Run `ToolSearch` to refresh the schema or restart
the session.

## [0.7.0] - 2026-05-28

### Added
- **Message metadata** (`sent_at_iso`, `is_broadcast`, `recipient_count`) added to every
  queued message payload (closes #9).
  - All messages now carry `sent_at_iso` (ISO-8601 UTC) so recipients know when a
    message was sent without relying on wall-clock proximity.
  - Broadcast payloads additionally carry `is_broadcast: true` and
    `recipient_count: N` so recipients can distinguish 1-to-many announcements
    from 1-to-1 work signals without reading the message body.
- **Activity timestamps + inbox count on `list_sessions`** (closes #10).
  - Each session entry now includes `last_post_at` (most recent outbound
    `post_message` / `broadcast_message`, ISO-8601 UTC or `null`),
    `last_receive_at` (most recent inbox drain via `receive_messages` or
    `register_session` backlog, ISO-8601 UTC or `null`), and
    `inbox_unread_count` (current pending message count, non-destructive).
  - Both timestamps are persisted to the state file and survive broker restarts.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-05-19

### Added
- `inbox_stats(session_id)` — non-destructive peek tool returning
  `{pending_count, senders}`. Useful for "have I been heard?" diagnostics
  and detecting races where a watcher has already drained the caller's
  intended payload. Closes #3.
- Optional `request_read_ack=True` parameter on `post_message`. When set,
  the broker automatically queues a `read-ack` notification (from
  `"broker"`) back to the sender's inbox the moment the recipient drains
  via `receive_messages` or `register_session` backlog. Use sparingly for
  confirm-required coordination signals; the ack confirms drain, not
  acted-on. Closes #4.

### Changed
- Drain logic factored into `_drain_inbox_locked` helper so
  `receive_messages` and `register_session` both honour the read-ack
  contract.

## [0.5.1] - 2026-05-19

### Changed
- `BROKER_WATCHER_MAX_INLINE` default lowered from 1500 to 400 chars
  after observing the empirical Monitor cut-off lands around 500 chars
  in practice. Users running into batch-driven truncation (multiple
  short messages within 200 ms) can lower the env var further.

## [0.5.0] - 2026-05-19

### Added
- `session_watcher.py` writes every received message to a per-session
  journal file (default `/tmp/reyn-broker-inbox/<session>/msg-<unix-ms>-<sender>.json`,
  configurable via `BROKER_INBOX_JOURNAL_DIR`).
- Messages whose JSON exceeds `BROKER_WATCHER_MAX_INLINE` chars are
  emitted as a short summary line with `_truncated: true` and
  `_full_path` pointing at the journal file so recipients can recover
  the full body. Closes #1.

## [0.4.0] - 2026-05-19

### Added
- `broadcast_message(from_session, message, exclude_self=True)` tool —
  queues the same message in every registered session's inbox in one
  call. Addressed-inbox semantics preserved (each recipient drains via
  `receive_messages`).

## [0.3.0] - 2026-05-19

### Added
- State persistence: sessions metadata + pending queues atomically
  persisted to `$BROKER_STATE_FILE` (default `~/.local/state/reyn-broker/state.json`).
  Loaded at startup; restart now transparently preserves session
  registration and in-flight messages.
- Optional `role` argument on `register_session`; surfaced in
  `list_sessions` output so peers can find each other by self-declared
  role rather than naming conventions.

### Changed
- `register_session` return value now includes `pending_messages`
  (drained backlog from the persisted store).

## [0.1.0] - 2026-05-19

### Added
- Initial public release.
- Streamable HTTP MCP server with `register_session` / `unregister_session`
  / `list_sessions` / `post_message` / `receive_messages` tools.
- `session_watcher.py` — polling helper that emits each received broker
  message as a single JSON line on stdout, designed for the Claude Code
  Monitor tool.
- `--host` / `--port` / `--log-level` CLI flags on the `reyn-broker`
  entry point, plus matching `BROKER_*` environment variables.
- MIT `LICENSE`; installable `pyproject.toml` (Python ≥ 3.10).
- pytest integration tests, ruff lint, GitHub Actions CI on 3.10 / 3.11
  / 3.12 matrix.

[0.8.0]: https://github.com/tya5/reyn-broker/releases/tag/0.8.0
[0.7.0]: https://github.com/tya5/reyn-broker/releases/tag/0.7.0
[0.6.0]: https://github.com/tya5/reyn-broker/releases/tag/0.6.0
[0.5.1]: https://github.com/tya5/reyn-broker/releases/tag/0.5.1
[0.5.0]: https://github.com/tya5/reyn-broker/releases/tag/0.5.0
[0.4.0]: https://github.com/tya5/reyn-broker/releases/tag/0.4.0
[0.3.0]: https://github.com/tya5/reyn-broker/releases/tag/0.3.0
[0.1.0]: https://github.com/tya5/reyn-broker/releases/tag/0.1.0
