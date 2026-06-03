# Stall Detection — Falsifiable-Wait Design

**Status:** Design proposal (doc-first). Authored by lead-coder for broker-maintainer
review. Implementation extends `peer_stall_watcher.py`; this document defines the
*why* and the *token budget* the implementation must hold to.

---

## 1. Existing mechanism (what we build on)

- **`session_watcher.py`** — per-session inbox Monitor. Drains `receive_messages`
  and emits one stdout line per arrived broker message, so a Claude Code session is
  *woken* (via the Monitor → `<task-notification>`) even when otherwise idle. This is
  the *delivery* layer. **Zero LLM** until a real message arrives.
- **`peer_stall_watcher.py`** — backlog-watcher Monitor. Polls `list_sessions` every
  5 min and emits `PEER_STALL: <sid> silent=<min>min` when a peer's `last_post_at` has
  been silent for ≥ 15 min. **Zero LLM** until it emits; only then does the
  backlog-watcher LLM wake. This is the *detection* layer.
- **`SESSION_GUIDE.md`** convention — backlog-watcher aggregates peer-status and
  surfaces **only actionable stall / blocker** to the leader.

The architecture is already token-efficient in shape (Python pollers, LLM only on
emit). The gap is purely in **what counts as a stall**.

## 2. The problem: `last_post` silence conflates three different states

A session that has not posted to the broker recently can be in any of:

1. **Real stall** — turn-cut idle (the session ended its turn with work remaining and
   nothing will re-invoke it) or an orphaned `session_watcher` (drains but does not
   surface → the session is deaf). *Needs intervention.*
2. **Legitimate wait** — the session stopped because it is blocked on a **coming
   external event** (a peer's PR, a CI run, a dispatched sub-agent). *Must be left
   alone.*
3. **Silent-but-working** — the session is actively producing work that does not flow
   through the broker (e.g. pushing WIP commits to a branch without a status post). Its
   `last_post_at` is stale, but it is not idle at all.

`peer_stall_watcher.py` today keys on `last_post_at` alone, so it cannot tell these
apart. The consequences are exactly the two failure modes we want to avoid:

- **Over-fires** on (2) and (3) → wakes the leader needlessly → **wasted tokens**
  (observed: a session pushing commits without posting trips a false `PEER_STALL`).
- If the threshold is raised to suppress that noise, it **under-fires** on (1) → real
  stalls go undetected (observed this session: a real turn-cut idle was caught by the
  human, not the watcher).

## 3. Principle — detection stays deterministic; the LLM fires only on a *confirmed,
   actionable* stall

The hard rule the implementation must hold:

> All stall *reasoning* happens in the watcher's Python loop using deterministic,
> machine-checkable queries (broker `list_sessions`, `gh`, `git`). An LLM is woken
> **only** when a stall is *confirmed* — i.e. there is a concrete next action (nudge a
> dormant session). LLM cost is then bounded by the number of *real* stalls, not by
> poll frequency, heartbeat volume, or the number of idle sessions.

This is the same discipline the existing pollers already use; the refinement must not
break it (no LLM in the detection path).

## 4. Design

### 4.1 Multi-signal liveness (fixes failure mode 3)

Reset the silence timer on **any** observable activity, not just `last_post_at`:

- a new broker post (current behaviour), **OR**
- a new commit on the session's active branch (`git ls-remote` — the session declares
  its active branch in its heartbeat, see §4.3), **OR**
- a sub-task state change (if the session owns a dispatched task).

All three are checkable in the watcher's Python loop with no LLM. This alone removes
the silent-but-working false-positive (the common, cheap win).

### 4.2 Falsifiable wait (fixes failure mode 2 vs 1)

The key insight: **the thing a session waits for is almost always observable**, so the
watcher can *falsify* "I'm legitimately waiting" by checking whether the awaited event
has already happened.

A session self-declares a **machine-checkable** `waiting_for` when it stops. The
watcher evaluates it deterministically:

| `waiting_for` token   | watcher check (Python, no LLM)            |
|-----------------------|-------------------------------------------|
| `ci:#1268`            | `gh pr checks 1268` — all non-pending?    |
| `merge:#1268`         | `gh pr view 1268 --json mergedAt`         |
| `pr-from:e2e:#1092`   | `gh pr list --author e2e` for the ref     |
| `commit:feat/x`       | `git ls-remote` HEAD changed?             |
| `broker-reply:e2e`    | inbox has a reply from `e2e`?             |
| `subtask:<id>`        | task state == done?                       |
| `external:<desc>`     | *unobservable* → timeout-only (see §4.4)  |

Decision in the watcher loop:

- `waiting_for` **unmet** → legitimate wait → **stay quiet**.
- `waiting_for` **met** but the session is still stopped → the wake-up **failed**
  (broker notification lost, orphaned watcher, or turn-cut after the event) → **this is
  a real stall** → emit `PEER_STALL` → leader nudges.
- declared `state: working` but no liveness signal (§4.1) for N min → stall.

`waiting_for` **must** be constrained to this small vocabulary. Free-text would force
an LLM to interpret it — which defeats the token budget.

### 4.3 Stop-hook heartbeat (makes "stopped" explicit + carries `waiting_for`)

A Claude Code **Stop hook** (a shell command, **zero LLM**) posts a small status record
to a broker *status registry* on every turn-end:

```json
{ "session": "<id>", "state": "working|waiting|done|blocked",
  "waiting_for": "ci:#1268", "active_branch": "feat/x",
  "head_sha": "<sha>", "ts": "<iso>" }
```

This turns "stopped" into an explicit, queryable fact (rather than inferring it from
`last_post_at` staleness) and supplies the `waiting_for` the falsifiable check needs.
The session declares `waiting_for`/`active_branch` as the last step of its work (e.g.
writes a status file the hook reads, or the hook derives `active_branch`/`head_sha`
from git).

### 4.4 Residual: unobservable waits

`external:<desc>` (e.g. waiting on a human reply) cannot be falsified by a query. These
fall back to **timeout escalation**: a bash timer in the watcher; on expiry, one LLM
wake to ping the awaited party or the leader. Still bounded and cheap — most waits are
observable and handled by §4.2; only the genuinely-external residue uses a timer.

## 5. Token budget (the whole point)

| component                     | cost                                       |
|-------------------------------|--------------------------------------------|
| stop-hook heartbeat           | shell write to registry — **0 LLM**        |
| watcher poll + falsifiable check | Python (broker/`gh`/`git`) — **0 LLM**  |
| heartbeat fan-out             | registry write only; watcher reads on its own cadence (heartbeats do **not** wake the leader) |
| nudge on confirmed stall      | **1 LLM wake**, only on a real stall       |

Idle / polling cost is **zero**. LLM consumption scales with *actual stalls*, not with
poll frequency or the number of idle sessions.

## 6. Rollout (tiered — each tier is independently shippable)

- **Tier 0 (cheap, immediate):** add **multi-signal liveness** (§4.1) to
  `peer_stall_watcher.py` — reset the timer on branch-commit activity, not just
  `last_post`. Removes the silent-but-working false-positive with **no new convention**.
- **Tier 1:** stop-hook heartbeat (§4.3) + `waiting_for` vocabulary + falsifiable check
  (§4.2). The robust version.
- **Tier 2:** `external:*` timeout escalation (§4.4).

## 7. Open questions for the maintainer

- Status registry placement — a broker server endpoint (`set_status` / `get_statuses`)
  vs a flat file under `/tmp/reyn-broker-inbox/`. An endpoint keeps it queryable via the
  same MCP the watcher already uses.
- `waiting_for` declaration ergonomics — session writes a status file the Stop hook
  reads, vs the hook derives what it can (branch/sha) and the session only annotates
  `waiting_for` when it differs from "working".
- Whether `peer_stall_watcher.py`'s `EXCLUDE_SESSIONS` and thresholds become per-role
  (e.g. a HOLD dogfood session has a longer legitimate-silence window).
