# hooks/ — canon for the Claude Code hooks that codify reyn_dev working discipline

This directory is the **canon** for the Claude Code hooks that encode
`~/Workspace/reyn_dev`'s working discipline (session_watcher, broker, coder
sessions). It replaces the prior state, where these 10 scripts lived only in
`~/.claude/hooks/` and each sourced a separately-copied scope guard.
Background: [issue #34](https://github.com/tya5/reyn-broker/issues/34).

## Why this repo, and not `reyn` or the checkouts themselves

- **Not the `reyn` repo**: these hooks encode *our* working discipline, not a
  property of the `reyn` project. Committing them there would force every
  `reyn` contributor to carry them (`git ls-tree origin/main -- .claude/` on
  `reyn` is empty — no precedent either).
- **Not each of the 8 `reyn` checkouts**: hand-placed copies drift.
- **Here**: `~/Workspace/reyn_dev/` is not a `reyn` checkout — it is
  broker / reyn_knowledge / reyn-self / user + 14 session directories, and the
  canon for *that* workspace's working discipline already lives in this
  repo's `SESSION_BOOTSTRAP.md` / `SESSION_GUIDE.md`.

## Layout

- `dispatcher.sh` — the ONLY command registered in `~/.claude/settings.json`
  (registered once per hook event: `PreToolUse`, `PostToolUse`, `Stop`).
  Owns the reyn_dev scope guard (fail-closed) and the foreign-registration
  warn. Reads `dispatch_table.json` to decide which of the scripts below to
  run for a given event + tool.
- `dispatch_table.json` — typed envelope: `{event: [{tool_pattern, scripts}]}`.
  `tests/test_hooks_dispatch_reachable.py` fails CI if any `hooks/*.sh` other
  than `dispatcher.sh` is missing from this table's `scripts` arrays — that
  is what makes "wrote an 11th hook, forgot to wire it" loud instead of silent.
- The 10 `*.sh` scripts — one working-discipline hook each. Unchanged from
  their `~/.claude/hooks/` originals except: the `source
  .../_reyn_dev_scope_guard.sh` line each one carried is REMOVED (the guard
  now lives once, in `dispatcher.sh` — see below).

## Why the scope guard collapsed to ONE copy (dispatcher.sh)

The guard logic itself was never wrong. What failed was the *registration
shape*: the guard was `source`d separately into all 10 scripts, so it fires
only where someone remembered to add that line — a hook added without it
fires everywhere on the machine, unguarded, with nothing catching the
omission. Moving the guard into `dispatcher.sh` — the one thing
`~/.claude/settings.json` calls — makes it structurally impossible to add a
hook that skips the guard: every hook now reaches the machine only by being
listed in `dispatch_table.json`, which `dispatcher.sh` always guards before
consulting.

## Two known-unconfirmed hooks — do not read their presence as "verified alive"

`swe_bench_v_advance_gate.sh`, `single_benchmark_dispatch_guard.sh`, and
`verify_state_giveup_reminder.sh` are **carried forward with unconfirmed
liveness** — owner has been asked whether the swe_bench workflow they gate is
still in use. They are wired into `dispatch_table.json` exactly as before
(carrying means keeping them reachable, not orphaning them) so removing them
later is a real decision, not a silent gap. **If the owner confirms they are
dead, the correct fix is to DELETE them** (script + dispatch_table.json
entry), not to add another guard around them.

## What the foreign-registration warn does and does NOT catch

`dispatcher.sh` reads `~/.claude/settings.json` once per session (keyed by
the hook payload's `session_id`) and warns to stderr — never blocks — if any
registered `command` resolves to something other than itself. This is the
**only** place that can see this at all: CI in this repo can verify every
`hooks/*.sh` is reachable through `dispatcher.sh`'s table, but CI cannot see
whether `~/.claude/settings.json` on the machine registers anything besides
`dispatcher.sh` — that file lives outside this repo, is applied by hand, and
this repo's CI has no view into it.

- **Catches**: a hook added straight to `~/.claude/settings.json` without
  going through this repo (the exact shape of the incident in #34).
- **Does NOT catch**: a script that copies or impersonates `dispatcher.sh`'s
  own path/behavior. There is no way to distinguish that from inside
  `dispatcher.sh` itself. This is a real, disclosed limit — not closed by
  this design, and not closeable by any mechanism that also lives only at
  hook-run time.
- **Never blocks**: blocking a hook registered outside this repo would risk
  stopping a legitimate non-reyn_dev hook of the owner's — the same
  direction of mistake as the original incident, just inverted.

## Scope guard

`dispatcher.sh` fires only when `${CLAUDE_PROJECT_DIR:-$PWD}` is
`$REYN_DEV_ROOT` (default `$HOME/Workspace/reyn_dev`) or a path under it,
matched as the two case arms `$root` / `$root/*` — never a bare-prefix glob,
which would also match an unrelated sibling like `reyn_dev_other`. Outside
that scope it exits 0 immediately (fail-closed: uncertain → do not fire).

`tests/test_dispatcher_scope_guard.py` verifies this with a differential
probe (same payload, in-scope vs out-of-scope vs prefix-collision sibling vs
`CLAUDE_PROJECT_DIR`-over-`cwd`) — never by reading an empty-stdin `rc=0` as
proof, since that rc is identical on both sides of the guard.

## Adding a new hook

1. Add the script to `hooks/`.
2. Add it to `dispatch_table.json` under the right event + `tool_pattern`.
3. `pytest tests/test_hooks_dispatch_reachable.py` — CI fails if you skip step 2.

There is no step where you touch `~/.claude/settings.json` — that file
registers `dispatcher.sh` once per event and never changes when hooks are
added or removed here.
