# Changelog

All notable changes to reyn-broker are documented in this file.

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

[0.7.0]: https://github.com/tya5/reyn-broker/releases/tag/0.7.0
[0.6.0]: https://github.com/tya5/reyn-broker/releases/tag/0.6.0
[0.5.1]: https://github.com/tya5/reyn-broker/releases/tag/0.5.1
[0.5.0]: https://github.com/tya5/reyn-broker/releases/tag/0.5.0
[0.4.0]: https://github.com/tya5/reyn-broker/releases/tag/0.4.0
[0.3.0]: https://github.com/tya5/reyn-broker/releases/tag/0.3.0
[0.1.0]: https://github.com/tya5/reyn-broker/releases/tag/0.1.0
