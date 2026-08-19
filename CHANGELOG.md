# Changelog

All notable changes to reyn-broker are documented in this file.

## [Unreleased]

### Changed
- **Runs on both mcp 1.x and 2.0** (#16). The SDK's 2.0 release renames or
  moves most of what the server touches, and the pieces fail at different
  times: the import fails immediately, the private lowlevel-server handle
  fails at startup, and `settings.host`/`settings.port` fail *silently* —
  2.0 dropped those fields, so assigning to them creates an attribute
  nobody reads and the server comes up on the default port looking
  healthy. Each is now version-aware:

  | | mcp 1.x | mcp 2.0 |
  |---|---|---|
  | module | `mcp.server.fastmcp` | `mcp.server.mcpserver` |
  | class | `FastMCP` | `MCPServer` |
  | lowlevel handle | `_mcp_server` | `_lowlevel_server` |
  | subscribe handler | `@server.subscribe_resource()` | `Server(...)` kwargs / `_request_handlers` |
  | bind address | `settings.host` / `.port` | `run()` kwargs |

  `resources/subscribe` is kept rather than replaced. All 7 live watchers
  are pre-2026-07-28 clients — not because the peers are on old SDKs
  (several are already on 2.0) but because every watcher runs
  `session_watcher.py` under *this* repo's venv, so they speak whatever
  version the broker pins. mcp 2.0 still routes the method for exactly
  this case. Newer clients use `subscriptions/listen`, which the 2.0 SDK
  serves on its own — nothing to implement here.

  Verified on both SDKs: 80 tests pass under each, and a live 1.x client
  against the 2.0 server negotiates 2025-11-25, subscribes, is woken,
  reads non-destructively, drains, and sees all 24 tools.

### Fixed
- **Test harness reads the same result twice.** `structuredContent` (1.x)
  and `structured_content` (2.0) are the same field; checking only one
  returned an empty result on the other version instead of an error.
  Same for the notification union (`.root` on 1.x, bare on 2.0) — reading
  only one shape recorded no wake-ups at all on the other, which looks
  like a delivery bug rather than a harness bug.

## [0.16.0] - 2026-08-13

### Added
- **Inboxes are now MCP resources you can subscribe to** (#13). Each session's
  inbox is exposed at `broker://inbox/<session_id>` and the server advertises
  `resources.subscribe`, so a client can be woken by
  `notifications/resources/updated` instead of polling `receive_messages`.
  One resource per session rather than a single shared feed — a subscriber is
  woken only by its own mail.

  Reading the resource is **non-destructive**: it never removes messages, so
  a client that misses a notification still finds the message waiting and
  wake-ups are at-least-once rather than exactly-once. Draining is done by
  `receive_messages`; separately, the TTL sweep drops messages posted with
  `ttl_seconds` once they expire.

  Note for future SDK bumps: on mcp 1.x the `resources.subscribe` capability
  had to be set explicitly — `Server.get_capabilities` hardcodes
  `subscribe=False` and registering a subscribe handler does not change that.
  mcp 2.0 derives the flag, but which input it derives from depends on the
  negotiated protocol version: below 2026-07-28 it reads the registered
  `resources/subscribe` handler (so the wrapper is a no-op), and at 2026-07-28+
  it reads `subscriptions/listen` and ignores the old handler entirely —
  the SDK's own wording is that the modern wire "cannot dispatch" it.

  So dropping `_advertise_resource_subscribe()` is not the whole retirement:
  once peers negotiate 2026-07-28, `resources/subscribe` stops being reachable,
  not merely unadvertised. #20 tracks that, and its condition is a measurement
  of what live watchers negotiate — not of what their SDK version pins allow.

### Fixed
- **`post_message` to an unregistered recipient now says so** (#14).
  Previously an unknown session id and a registered-but-idle peer both came
  back as `online=False`, so a typo'd or wrong-namespace id looked exactly
  like a peer that happened to be away, and the message sat in a queue nobody
  would ever drain. Unregistered targets are now reported separately (single
  and multi-recipient forms). Delivery behaviour is unchanged — posting to a
  not-yet-registered session is still allowed, since sessions legitimately
  register after mail is sent for them.

## [0.15.4] - 2026-06-13

### Fixed
- **ci-watcher never relayed any CI results (`last_post_at=null`).** The plugin
  was a per-PR opt-in model (`watch:#N`) but no session was issuing watch
  commands, so it polled nothing and posted nothing. Additionally, the `watch`
  and `unwatch` handlers stored `broker._session_id` (the plugin's own session
  id, always `"ci-watcher"`) as the requester instead of the caller's id, meaning
  even if someone did subscribe, notifications would post back to the plugin
  itself rather than the caller.

### Added
- **`watch-repo:owner/repo` / `unwatch-repo:owner/repo` commands** for
  repo-level CI subscription. Subscribing to a repo causes ci-watcher to poll
  all open PRs in that repo every `CI_POLL_S` seconds (default 60) and relay
  `ci_result: #N success|failure owner/repo` events to all subscribers when a
  PR's CI reaches a terminal state. This fills the gap where a PR sits in
  BLOCKED state, CI fails, but no `mergeStateStatus` transition occurs so
  github-pr-watcher emits nothing. After subscribing, backlog-watcher no longer
  needs to self-poll GitHub CI — the plugin boundary violation is resolved.
- **Fixed `sender` in `watch`/`unwatch`** — per-PR commands now correctly track
  the requesting session id via the `sender` parameter rather than the plugin's
  own id.

## [0.15.3] - 2026-06-05

### Fixed
- **github-pr-watcher missed `pr_clean` when a PR went BLOCKED→CLEAN→merged
  inside one poll interval.** `pr_clean` is an edge derived by sampling each
  open PR's `mergeStateStatus` every 300 s. When CI completed and the PR was
  merged (manually or by auto/force-merge) faster than the poll interval, no
  poll ever observed the PR in the open+CLEAN state — it jumped straight from
  BLOCKED to gone, producing `pr_merged` and skipping `pr_clean`. This hit the
  force-merge wave hardest (#1318, #1302), where the CLEAN-open window collapses
  to near-zero. Fix: the watcher now **adapts its poll cadence** — it polls at a
  fast interval (`PR_WATCH_FAST_INTERVAL`, default 25 s) while any watched PR is
  mid-flight (`mergeStateStatus` in `PR_WATCH_FAST_STATES`, default
  `BLOCKED,UNKNOWN`) and drops back to the idle interval (`PR_WATCH_INTERVAL`,
  default 300 s) once all PRs settle. Single poll loop, single emitter — the
  existing diff logic is unchanged, so there are no concurrency races. This
  narrows the miss window from 300 s to ~25 s; it does not eliminate it (a true
  zero-miss fix needs GitHub webhooks). Recipients should still treat
  `pr_merged` as the terminal signal that supersedes a possibly-missed
  `pr_clean`.

## [0.15.2] - 2026-06-04

### Fixed
- **Ghost subscriptions / command schemas after a session is gone.**
  `unregister_session` (and TTL expiry) removed the `SessionEntry` but left the
  session's `event_subscriptions` and `plugin_commands` entries behind. The
  stale subscription kept matching events, and `list_plugin_commands` still
  returned commands for a session that no longer existed. Both are now dropped
  via `_forget_session_locked` when the session is removed.

## [0.15.1] - 2026-06-04

### Fixed
- **Plugins were silently killed every few minutes, breaking stall/idle
  detection.** The FastMCP / Streamable-HTTP lifespan context can enter and
  exit more than once during a single broker process. The lifespan's `finally`
  block terminated every plugin on each exit, so auto_start plugins (telegram,
  ci-watcher, github-pr-watcher, peer-idle-notifier) were SIGTERM'd roughly
  every 6 minutes and relaunched. Session events delivered while a plugin was
  down were never processed — e.g. peer-idle-notifier missed `active_changed`
  edges and never emitted PEER_IDLE. Fix:
  - background tasks + plugin auto-launch now run exactly once (guarded by
    `_bg_started`), even if the lifespan is entered repeatedly;
  - the lifespan no longer terminates plugins on context exit;
  - plugin cleanup moved to an `atexit` hook that fires on real process exit.
- **Event-driven plugins never processed session events.** `BrokerPlugin`'s
  inbox loop fetched `receive_messages` with `fields=["from","message"]`, which
  stripped event payload fields (`event` / `active` / `session_id` / …).
  `on_broker_message` saw no `event` key and returned immediately, so
  `peer_idle_notifier` never emitted PEER_IDLE despite events being delivered.
  The inbox loop now fetches all fields.

## [0.15.0] - 2026-06-03

### Added
- **`set_active(session_id, active: bool)` tool** — a mechanical liveness axis
  separate from `update_session_status`. Hook-driven (work-start → `True`,
  Stop → `False`) and deterministic; this is the authoritative signal monitors
  use for stall/idle detection. Fires an `active_changed` event only on an
  actual flip (repeated same-value calls are no-ops, so a PreToolUse hook
  firing `True` every tool call does not spam subscribers).
- **`active_changed` event type** — payload carries `active`, `prev_active`,
  and the current `status` / `detail` for enrichment.
- **`reyn-broker-active SESSION_ID true|false` CLI** — zero-LLM-cost hook entry
  point for the active axis (mirrors `reyn-broker-status` for the semantic axis).
- **Multiple independent event subscriptions** — `subscribe_session_events` no
  longer merges; a subscriber may hold several `(event_types, session_filter)`
  pairs that are evaluated independently. An event is delivered at most once
  per subscriber. Identical pairs are idempotent.
- **`status_changed` carries `prev_status`** — lets consumers edge-detect
  rather than re-fire on detail-only updates.

### Changed
- **Two orthogonal status axes (fixes a clobber bug).** Previously the single
  `status` + `detail` field carried both the mechanical (hook) and semantic
  (LLM) meaning. A Stop hook's `update_session_status(id, "idle")` would wipe
  an LLM-declared `update_session_status(id, "waiting", "ci:#1268")` because
  `detail` was overwritten unconditionally and the Stop hook always runs last.
  Now `set_active` (mechanical bool) and `update_session_status` (semantic
  string) are independent fields with independent setters — neither clobbers
  the other.
- `list_sessions` (compact + full) and `get_session_status` now include
  `active`.
- `peer_idle_notifier` subscribes to `active_changed` (False edge) instead of
  `status_changed`; semantic `status`/`detail` ride along as enrichment.

### ⚠️ Schema change notice
New tool `set_active`; new event `active_changed`; `list_sessions` /
`get_session_status` gained `active`. Run `ToolSearch` to refresh schemas.

## [0.14.0] - 2026-06-03

### Added
- **`update_session_status(session_id, status, detail)` tool** — sessions
  explicitly report their activity state (`"active"`, `"idle"`, `"waiting"`,
  or any custom string). The broker stores the status on `SessionEntry` and
  fires a `status_changed` event to subscribers. Replaces heuristic inference
  from `last_post_at` for stall detection.
- **`status_changed` event type** — new value for `subscribe_session_events`.
  Payload includes `status` and `detail` fields in addition to the standard
  `session_id` / `at` fields.
- **`reyn-broker-status` CLI** (`session_status.py`) — one-shot command for
  use in Claude Code hooks (Stop, PreToolUse, etc.) where no LLM turn is
  available. Usage: `reyn-broker-status SESSION_ID STATUS [DETAIL]`.
- **`BrokerClient.subscribe_events()`** — first-class API on `BrokerClient`
  replacing the `broker._cs.call_tool("subscribe_session_events", ...)` pattern.
- **`@command` `sender` parameter** — plugin command methods can declare
  `sender: str = ""` to receive the caller's session id; injected by the
  framework, not a user-supplied argument.
- **Bundled plugins** — `peer_idle_notifier.py` and
  `plugins/github_pr_watcher.py` added to the package with entry points
  `reyn-broker-peer-idle` and `reyn-broker-github-pr`.

### Changed
- `list_sessions` compact shape now includes `"status"` field.
- `list_sessions` full shape now includes `"status"` and `"status_detail"`.
- `peer_idle_notifier` (formerly `peer_stall_watcher`): an immediate idle
  notifier. Subscribes to `status_changed` and fires instantly when a session
  transitions to an idle state — no timer, no threshold, no local state cache.
  Duplicate notifications are prevented by the server-side `before != after`
  guarantee on `update_session_status`. Env vars use the `PEER_IDLE_*` prefix.
- `plugins/github_pr_watcher`: manual `on_broker_message` dispatch replaced
  with `@command` + `sender` parameter.

### ⚠️ Schema change notice
New tool `update_session_status`. `list_sessions` compact/full shapes gained
`status` (and `status_detail` in full). Run `ToolSearch` to refresh schemas.

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
