# reyn-broker

[![CI](https://github.com/tya5/reyn-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/tya5/reyn-broker/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

A small MCP broker that lets multiple Claude Code sessions exchange messages
by session id.

## What it is

- A single long-running HTTP server that routes messages between independent
  MCP clients. Built on top of FastMCP with Streamable HTTP transport.
- Each client connects, calls `register_session` with an id, and can `post_message`
  to any other registered id. Recipients pull via `register_session` (initial
  backlog) or `receive_messages` (polling).
- Sessions are independent: they start and stop on their own, and the broker
  has no role in spawning or supervising them. The user controls each session
  directly. This is different from orchestrator/worker designs where a lead
  spawns and dispatches to subordinates.

## Limitations

Read these first.

- **No acknowledgement.** `post_message` returns once queued; it cannot tell
  you whether the recipient acted on the message.
- **Push is best-effort.** The broker also fires `notifications/message`
  hints, but clients (including Claude Code) often do not surface log
  notifications to the agent. Treat the inbox queue as the source of truth
  and pull via `receive_messages`.
- **Single host, no auth by default.** Binds to `127.0.0.1` only unless you
  pass `--host`. There is no built-in token check.
- **Tested only with Claude Code.** Other MCP clients should connect if they
  speak Streamable HTTP, but they are unverified.

State (registered sessions, role, pending messages) is persisted to a JSON
file (default `~/.local/state/reyn-broker/state.json`, override with
`BROKER_STATE_FILE`) so the broker can be restarted without registered
sessions needing to re-register or losing queued messages.

## Install

```bash
pip install -e ".[dev]"   # editable + test/lint deps
```

Requires Python 3.10+. The only runtime dependency is `mcp[cli]>=1.27`.

## Run

```bash
reyn-broker                          # binds 127.0.0.1:8765
reyn-broker --host 0.0.0.0 --port 9000
reyn-broker --log-level DEBUG
```

Environment variables (`BROKER_HOST`, `BROKER_PORT`, `BROKER_LOG_LEVEL`,
`BROKER_STATE_FILE`) are honoured as defaults; CLI flags win where they exist.

To stop: signal the broker process directly, e.g. `pkill -f reyn-broker` or
`kill <pid>`. Avoid `lsof -ti:<port> | xargs kill` — that matches both the
listening broker *and* any clients connected to that port (including the
`session_watcher.py` polling processes), so it kills your watchers too.
Use `lsof -ti:<port> -sTCP:LISTEN` if you need a port-based stop.

### After a broker restart

- **State** (registered sessions, roles, pending messages) survives restart automatically via
  the state file. Sessions do not need to re-call `register_session`.
- **`session_watcher.py`** reconnects automatically on its next poll cycle (within
  `ERROR_BACKOFF_S` seconds, default 10 s). No watcher restart needed.
- **MCP tool schema cache** — Claude Code fetches each MCP server's tool list once at
  session startup and caches it. If the broker is restarted with a new version that
  changes a tool's signature (added/removed parameters), connected sessions will still
  hold the old schema. Symptoms: `receive_messages` works but `post_message` (or another
  changed tool) fails.
  **Fix:** run `ToolSearch(query="select:mcp__broker__post_message")` in the affected
  session to force a schema refresh. If that does not help, restart the Claude Code
  session to re-initialize the MCP connection from scratch.
  **Prevention:** broker releases that change tool signatures are noted in CHANGELOG as
  requiring a session-side schema refresh.
- **Upgrading to a version with renamed OR newly-added tools** (e.g. 0.13.x →
  0.14.0 renamed `plugin_add` → `add_plugin`; 0.15.0 added `set_active`) — a
  connected session caches the tool list from when it connected, so it cannot
  see added tools and still sees removed names. `ToolSearch` does **not** help:
  it searches the cached list, so a newly-added tool (e.g. `set_active`) never
  resolves. **Restart all Claude Code sessions** after such an upgrade to get a
  clean schema. (CLI entry points like `reyn-broker-active` open their own
  connection and are unaffected — hooks keep working across upgrades.)

## MCP tools

A misspelled or unrecognized argument to any tool call is rejected as an
error rather than silently dropped — e.g. `broadcast_message(...,
targets=[...])` (the real kwarg is `recipients`) fails loudly instead of
falling back to `recipients`'s default and broadcasting to everyone.

| Tool                | Args                                            | Effect                                                                          |
|---------------------|-------------------------------------------------|---------------------------------------------------------------------------------|
| `register_session`  | `session_id`, `working_dir`, `role?`, `ttl_hours?` | Register this client. Returns `status` and `pending_messages` (drained backlog). `ttl_hours` sets an auto-expiry for the session. |
| `unregister_session`| `session_id`                                    | Remove from the registry.                                                       |
| `startup_summary`   | `session_id`, `working_dir`, `role?`, `compact?`, `ttl_hours?` | Register + list in one round-trip. Returns `status`, `pending_messages`, and `sessions` (compact by default). |
| `list_sessions`     | `compact?`                                      | Return registered sessions. `compact=True` returns only `session_id`+`role` (~60% fewer tokens). Full shape includes activity timestamps. |
| `post_message`      | `to`, `from_session`, `message`, `request_read_ack?`, `recipients?`, `ttl_seconds?` | Queue a message. `recipients=[...]` for multi-target; `ttl_seconds` for auto-expiry. |
| `broadcast_message` | `from_session`, `message`, `exclude_self?`, `recipients?` | Queue the same message in every registered session's inbox (sender skipped by default). `recipients=[...]` limits to a subset; an unregistered id in it is still queued (delivered if that id registers later) but named as NOT REGISTERED in the return value. |
| `receive_messages`  | `session_id`, `fields?`                         | Drain and return the caller's inbox. `fields=["from","message"]` strips metadata to reduce token overhead. |
| `peek_messages`     | `session_id`, `limit?`, `fields?`              | Non-destructive content preview: returns up to `limit` (default 10) messages without draining. Supports `fields` selector. |
| `inbox_stats`       | `session_id`                                    | Non-destructive peek: `{pending_count, senders}`.                              |
| `set_active`        | `session_id`, `active`                          | Set the mechanical liveness bool (hook-driven, deterministic). Fires `active_changed` only on a real flip. The authoritative signal for stall/idle detection. Orthogonal to `update_session_status` — never clobbers `status`. |
| `update_session_status` | `session_id`, `status`, `detail?`           | Report the semantic status (LLM-driven, e.g. `"waiting"` + reason). Fires `status_changed`. Enrichment only; never touches `active`. |
| `get_session_status` | `session_id`                                   | Return `{registered, active, status, status_detail}` for one session. |
| `subscribe_session_events` | `subscriber_id`, `event_types`, `session_filter?` | Push session lifecycle events (`registered`, `unregistered`, `posted`, `status_changed`) into the subscriber's inbox. |
| `unsubscribe_session_events` | `subscriber_id`                          | Cancel an event subscription.                                                   |
| `tool_stats`        | *(none)*                                        | Return per-tool call counts since broker start. Useful for token-cost analysis. |
| `set_monitor_session` | `session_id?`                                 | Enable/disable a monitor session that receives a copy of every `post_message`/`broadcast_message` (with a `monitor_to` field). Omit `session_id` to disable. In-memory only; persist via `BROKER_MONITOR_SESSION` env. |
| `add_plugin`        | `name`, `command`, `session_id`, `env?`, `auto_start?` | Register and optionally auto-start a plugin subprocess.                   |
| `start_plugin` / `stop_plugin` / `restart_plugin` / `remove_plugin` | `name` | Manage plugin lifecycle without broker restart. |
| `list_plugins`       | *(none)*                                        | List registered plugins with running state and PID.                             |
| `list_plugin_commands` | `session_id`                                  | Retrieve a plugin's declared command schema.                                    |
| `subscribe_plugin_notifications` | `plugin`, `session_id`             | Opt in to a plugin's notifications (e.g. `peer-idle-notifier`) — self-service, no maintainer/restart needed. Idempotent, persisted. |
| `unsubscribe_plugin_notifications` | `plugin`, `session_id`           | Opt out. No-op if not subscribed.                                               |
| `list_plugin_notification_subscribers` | `plugin`                     | Current subscriber list for a plugin.                                          |
| `health_check`      | *(none)*                                        | Returns `version`, `started_at_iso`, `uptime_seconds`, `session_count`, `total_pending`. |

### Resources — subscribing to an inbox

| Resource | Description |
|---|---|
| `broker://inbox/{session_id}` | Non-destructive view of one session's inbox: `{session_id, registered, pending_count, messages}`. |

The server advertises `resources.subscribe`, so instead of polling
`receive_messages` a client can subscribe to its own inbox URI and be woken
by `notifications/resources/updated` when mail arrives:

```
resources/subscribe  broker://inbox/<your-session-id>
  → woken on new mail
  → read the resource to see what arrived (does not consume it)
  → receive_messages to drain
```

Two properties worth knowing:

- **One resource per session, not one shared feed.** A subscriber is woken
  only by its own mail; a single global URI would wake every peer on every
  message.
- **Reading is non-destructive.** A missed notification is therefore not a
  lost message — the mail is still there on the next read, which makes
  wake-ups at-least-once rather than exactly-once. Reading removes
  nothing; `receive_messages` drains, and the TTL sweep also drops
  messages that were posted with `ttl_seconds` once they expire.

Subscriptions live in memory and belong to a connection: after a broker
restart, clients re-subscribe and re-read to catch up.

`role` is a short free-text label (e.g. `"PR review"`, `"e2e tests"`) so
peers can find you via `list_sessions` without relying on naming conventions.
It is optional; sessions that omit it appear with `role: null`.

`post_message` distinguishes a recipient that has never registered from one
that is registered but idle: the former is reported as `NOT REGISTERED`
rather than `online=False`. The message is queued either way (a session may
legitimately register after mail is sent for it), but a typo'd or
wrong-namespace id no longer looks like a peer who happens to be away.

`request_read_ack=True` on `post_message` makes the broker queue a
`read-ack` notification (from `"broker"`) back to the sender's inbox the
moment the recipient drains via `receive_messages`. This confirms the
message was *drained*, not that the recipient acted on it — use sparingly
for confirm-required coordination signals such as "block raised", "I'm
picking up #N", or "pause merge".

`inbox_stats(session_id)` returns `{pending_count, senders}` without
draining; useful for sanity-checking that a watcher has not raced ahead
of the caller, or for "have I been heard?" diagnostics.

`peek_messages(session_id, limit=10, fields=None)` returns the oldest
`limit` messages from the inbox without consuming them. Use it to
preview message content and make triage decisions (interrupt or finish
current task first?) before committing to a `receive_messages` drain.

`health_check()` returns `version`, `started_at_iso`, `uptime_seconds`,
`session_count`, and `total_pending`. Run it after a broker restart to
confirm which version is live before refreshing tool schemas in connected
sessions.

## Client configuration

Each MCP client adds an entry to its config (Claude Code reads `.mcp.json`):

```json
{
  "mcpServers": {
    "broker": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

## Receiving without polling: `session_watcher.py`

`session_watcher.py` is a tiny script that polls `receive_messages` on behalf
of a session and emits one JSON line to stdout per arrived message:

```bash
/path/to/broker/.venv/bin/python /path/to/broker/session_watcher.py --session=<your-session-id>
```

> **Note:** use the broker's own virtualenv Python. The system `python3` will fail with
> `ModuleNotFoundError: No module named 'mcp'`.

In Claude Code, run it under the Monitor tool with `persistent=true`; each
stdout line becomes a `<task-notification>` event in the LLM context, so
messages arrive in-channel without the agent having to poll.

### Long-message handling

Claude Code's Monitor caps the body of a `<task-notification>` event at a
few KB. To stop the tail of long messages getting silently dropped, the
watcher:

1. Writes every received message to a per-session journal file at
   `$BROKER_INBOX_JOURNAL_DIR/<session_id>/msg-<unix-ms>-<sender>.json`
   (default `/tmp/reyn-broker-inbox/...`).
2. If the JSON-encoded message exceeds `BROKER_WATCHER_MAX_INLINE` chars
   (default 400), the emitted stdout line is a short summary with
   `_truncated: true`, `_full_path` pointing at the journal file, and
   `_preview` with the first ~100 chars of the body inline — so recipients
   can make routing decisions without a `Read` round-trip in most cases.
   The default was tuned empirically against Claude Code's Monitor cut-off,
   which sits around 500 characters in practice.

The journal files are not auto-cleaned. `/tmp` is typically wiped at
reboot on most systems; delete manually if disk pressure is a concern.

`SESSION_GUIDE.md` documents the protocol clients should follow (session id
conventions, when to call `receive_messages`, watcher batching semantics,
how to recover from a truncated notification).

## Tests

```bash
pytest                # integration tests, ~6s
ruff check .          # lint
```

The test suite starts a fresh broker subprocess on an ephemeral port per
test, so it does not interfere with a locally running broker on 8765.

## License

MIT. See `LICENSE`.
