# Changelog

All notable changes to reyn-broker are documented in this file.

## [0.13.1] - 2026-06-03

### Fixed
- **Plugin subprocess robustness** — three safety gaps addressed:
  - `_terminate_plugin`: added SIGKILL fallback after SIGTERM + 5 s timeout.
    A hung plugin no longer lingers indefinitely.
  - `_plugin_supervisor`: new asyncio task (started in `_lifespan`) checks every
    10 s whether any `auto_start` plugin has crashed and restarts it automatically.
    Prevents silent death of monitoring plugins (stall watcher, CI watcher, etc.).
  - `_launch_plugin`: stderr is now redirected to
    `~/.local/state/reyn-broker/plugins/<name>.log` (append mode) instead of
    `/dev/null`. Crash diagnostics are preserved across restarts.

## [0.10.0] - 2026-05-30

### Added
- **`health_check()` tool** — returns `version`, `started_at_iso`, `uptime_seconds`,
  `session_count`, and `total_pending`. Useful for smoke-testing after a broker
  restart and for confirming which version is running before refreshing tool schemas.
  Closes #8.
- **`peek_messages(session_id, limit=10, fields=None)` tool** — non-destructive content
  preview. Returns up to `limit` messages from the inbox without draining them, so
  callers can make triage decisions without triggering read-acks or clearing the queue.
  Supports the same `fields` selector as `receive_messages`.
- **`broadcast_message(recipients=[...])` subset filter** — new optional `recipients`
  parameter. When provided, the broadcast is limited to that subset of registered
  sessions. Unregistered ids in the list are silently skipped. `exclude_self` still
  applies. Omitting `recipients` keeps the existing all-sessions behaviour.
- **Session TTL** — new optional `ttl_hours: float` parameter on `register_session`
  and `startup_summary`. When set, the broker records a `session_expires_at` epoch
  on the entry and the background purge task will automatically remove it after that
  many hours. Useful for short-lived task sessions that may not call
  `unregister_session` before exiting. TTL survives broker restarts (persisted to
  state file).
- **Background purge task** — a background asyncio task (via FastMCP lifespan) runs
  every 5 minutes and purges both expired messages (existing behaviour, now also
  proactive) and expired sessions (new). Previously, expired messages were only
  removed lazily on next drain or `inbox_stats` call.

### Changed
- `_register_locked` now accepts a `ttl_hours` argument; callers that omit it get
  the same `session_expires_at=None` (no TTL) behaviour as before.
- `_save_state` / `_load_state` persist the new `session_expires_at` field.
  State files from v0.9.0 load cleanly — the field defaults to `None` when absent.

### ⚠️ Schema change notice
Five tools changed or were added: `register_session` and `startup_summary` gained
`ttl_hours`; `broadcast_message` gained `recipients`; `health_check` and
`peek_messages` are new. Sessions started against v0.9.0 or earlier should run
`ToolSearch` to refresh schemas or restart their Claude Code session.

## [0.9.0] - 2026-05-30

### Added
- **`startup_summary` tool** — registers a session and returns the peer list in
  one round-trip, replacing the common `register_session` + `list_sessions`
  startup pattern. Defaults to `compact=True` for the session list.
- **`list_sessions(compact=True)`** — compact mode returns only `session_id` and
  `role`, ~60 % fewer tokens than the full shape. Full shape (activity timestamps,
  `inbox_unread_count`) still available with `compact=False` (default kept for
  backward compatibility).
- **`receive_messages(fields=[...])`** — optional field selector. Pass e.g.
  `fields=["from","message"]` to strip `sent_at_iso`, `is_broadcast`, and
  `recipient_count` from returned messages, reducing token overhead for callers
  that do not need metadata.
- **SESSION_GUIDE §5.9 token-saving guidelines** — documents `startup_summary`,
  `compact=True`, field selector, and call-frequency best practices.

### Changed
- `register_session` and `list_sessions` internals factored into
  `_register_locked` / `_session_list_locked` helpers (shared with
  `startup_summary`).
- `list_sessions` `inbox_unread_count` now applies `_purge_expired` before
  counting, so expired messages are not reflected in the count.

### ⚠️ Schema change notice
`list_sessions` gained a `compact` parameter; `receive_messages` gained a
`fields` parameter; `startup_summary` is new. Sessions started against v0.8.0
or earlier should run `ToolSearch` to refresh schemas or restart their Claude
Code session.

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

[0.10.0]: https://github.com/tya5/reyn-broker/releases/tag/0.10.0
[0.9.0]: https://github.com/tya5/reyn-broker/releases/tag/0.9.0
[0.8.0]: https://github.com/tya5/reyn-broker/releases/tag/0.8.0
[0.7.0]: https://github.com/tya5/reyn-broker/releases/tag/0.7.0
[0.6.0]: https://github.com/tya5/reyn-broker/releases/tag/0.6.0
[0.5.1]: https://github.com/tya5/reyn-broker/releases/tag/0.5.1
[0.5.0]: https://github.com/tya5/reyn-broker/releases/tag/0.5.0
[0.4.0]: https://github.com/tya5/reyn-broker/releases/tag/0.4.0
[0.3.0]: https://github.com/tya5/reyn-broker/releases/tag/0.3.0
[0.1.0]: https://github.com/tya5/reyn-broker/releases/tag/0.1.0
