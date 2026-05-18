# reyn-broker

A small MCP broker that lets multiple Claude Code sessions exchange messages
by session id.

## What it is

- A single long-running HTTP server that routes messages between independent
  MCP clients. About 150 lines of Python on top of FastMCP.
- Each client connects, calls `register_session` with an id, and can `post_message`
  to any other registered id. Recipients pull via `register_session` (initial
  backlog) or `receive_messages` (polling).
- Sessions are independent: they start and stop on their own, and the broker
  has no role in spawning or supervising them. The user controls each session
  directly. This is different from orchestrator/worker designs where a lead
  spawns and dispatches to subordinates.

## Limitations

Read these first.

- **In-memory state.** Restarting the broker drops the registry and any queued
  messages. Persistence is not implemented.
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

Environment variables (`BROKER_HOST`, `BROKER_PORT`, `BROKER_LOG_LEVEL`) are
honoured as defaults; CLI flags win.

## MCP tools

| Tool                | Args                                  | Effect                                                                          |
|---------------------|---------------------------------------|---------------------------------------------------------------------------------|
| `register_session`  | `session_id`, `working_dir`           | Register this client. Returns `status` and `pending_messages` (drained backlog). |
| `unregister_session`| `session_id`                          | Remove from the registry.                                                       |
| `list_sessions`     | —                                     | Return all currently registered sessions.                                       |
| `post_message`      | `to`, `from_session`, `message`       | Queue a message in the recipient's inbox.                                       |
| `receive_messages`  | `session_id`                          | Drain and return the caller's inbox.                                            |

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
python /path/to/broker/session_watcher.py --session=<your-session-id>
```

In Claude Code, run it under the Monitor tool with `persistent=true`; each
stdout line becomes a `<task-notification>` event in the LLM context, so
messages arrive in-channel without the agent having to poll.

`SESSION_GUIDE.md` documents the protocol clients should follow (session id
conventions, when to call `receive_messages`, watcher batching semantics).

## Tests

```bash
pytest                # integration tests, ~6s
ruff check .          # lint
```

The test suite starts a fresh broker subprocess on an ephemeral port per
test, so it does not interfere with a locally running broker on 8765.

## License

MIT. See `LICENSE`.
