#!/bin/bash
# reyn-broker hooks dispatcher -- the ONLY command registered in
# ~/.claude/settings.json. See hooks/README.md for the design (issue #34).
#
# Why this exists (architect ruling, #34): the unit is the *workspace*
# (~/Workspace/reyn_dev/*), the canon for it lives in this repo, and the
# reyn_dev scope guard must live in exactly ONE place -- here -- because a
# guard sourced separately into N hook scripts silently omits an (N+1)th one
# a future hook forgets to source. Consolidating the guard into the single
# thing that is registered removes the "forgot to wire it" failure mode by
# construction; hooks/test_hooks_dispatch_reachable.py's CI gate covers the
# analogous "forgot to add it to the dispatch table" failure mode on the
# repo side.
#
# Invocation (from settings.json): `dispatcher.sh <HookEventName>`, e.g.
# `dispatcher.sh PreToolUse`. The event name is a CLI arg -- not read from
# the hook JSON payload -- because it must be known before dispatch_table.json
# is even consulted, and passing it explicitly avoids depending on an
# undocumented/version-dependent JSON field for something settings.json
# already knows statically (which top-level hooks.<Event> array this
# registration lives under).
#
# Contract with the hooks it dispatches to: every hooks/*.sh script reads its
# OWN full stdin via `input=$(cat)` (verified: every existing hook does this).
# The dispatcher reads stdin exactly once and re-feeds an identical copy to
# each dispatched script -- see "stdin fan-out" below. This is the one place
# the brief asked to be explicit rather than implicit; getting it wrong means
# every dispatched hook silently sees empty stdin.
#
# No `set -u`: macOS ships bash 3.2 as /bin/bash (this script's own shebang
# target), and 3.2 raises "unbound variable" on `"${arr[@]}"` for a
# zero-element array even after `arr=()` -- a real bug, not a style choice.
# None of the existing hooks/*.sh use `set -u` either; matching that.

hooks_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
self_path="$hooks_dir/$(basename "${BASH_SOURCE[0]}")"
table_file="$hooks_dir/dispatch_table.json"

event="${1:-}"

# ---------------------------------------------------------------------------
# stdin fan-out: read once, re-feed the SAME bytes to every dispatched
# script. `input=$(cat)` strips a trailing newline the way every dispatched
# script's own `input=$(cat)` already did when Claude Code invoked it
# directly -- so this is not a new normalization, just moving where it
# happens.
# ---------------------------------------------------------------------------
input="$(cat)"

# ---------------------------------------------------------------------------
# reyn_dev scope guard -- THE ONE COPY (owner report 2026-09-26: 10 hooks
# fired in every project on the machine because the guard was duplicated
# into 10 files and 0 of them had it originally).
#
# fail-CLOSED: when the project can't be determined as inside reyn_dev, do
# NOT dispatch. Matching must reject a shared-prefix sibling
# (reyn_dev_other) -- hence the explicit `dir|dir/*` case arms, never a glob
# on the bare prefix.
# ---------------------------------------------------------------------------
reyn_dev_root="${REYN_DEV_ROOT:-$HOME/Workspace/reyn_dev}"
scope_dir="${CLAUDE_PROJECT_DIR:-$PWD}"
case "$scope_dir" in
  "$reyn_dev_root"|"$reyn_dev_root"/*) ;;
  *) exit 0 ;;
esac

# ---------------------------------------------------------------------------
# Foreign-registration warn (architect, #34): the dispatcher is the only
# place that can see ~/.claude/settings.json at hook-run time, so it is the
# only place that can notice a hook someone registered WITHOUT going through
# this repo. WARN only, never block (blocking would repeat the owner's
# incident in the opposite direction: stopping a legitimate non-reyn_dev
# hook). At most once per session (session_id from the hook JSON) to avoid
# per-hook-call noise.
#
# LIMIT (see hooks/README.md): this catches "registered a command that is
# not this dispatcher". It cannot catch a script that copies this
# dispatcher's own path/behavior to impersonate it.
# ---------------------------------------------------------------------------
settings_file="$HOME/.claude/settings.json"
session_id="$(printf '%s' "$input" | jq -r '.session_id // "unknown"' 2>/dev/null || echo unknown)"
warn_marker="${TMPDIR:-/tmp}/reyn-broker-dispatcher-warned-${session_id}"
if [ -r "$settings_file" ] && [ ! -e "$warn_marker" ]; then
  foreign="$(jq -r '[.hooks[][]?.hooks[]?.command] | .[]' "$settings_file" 2>/dev/null \
    | awk '{print $1}' | sort -u | grep -vF -- "$self_path" || true)"
  : > "$warn_marker" 2>/dev/null || true
  if [ -n "$foreign" ]; then
    {
      echo "[dispatcher] WARN: ~/.claude/settings.json registers command(s) other than this dispatcher:"
      printf '  %s\n' "$foreign"
      echo "  Hooks must be registered by routing through tya5/reyn-broker's hooks/dispatcher.sh (issue #34)."
      echo "  This warns only -- it does not block -- and it cannot detect a script that impersonates the dispatcher (see hooks/README.md)."
    } >&2
  fi
fi

# ---------------------------------------------------------------------------
# Dispatch table lookup + fan-out.
# ---------------------------------------------------------------------------
if [ -z "$event" ] || [ ! -r "$table_file" ]; then
  exit 0
fi

tool_name="$(printf '%s' "$input" | jq -r '.tool_name // ""' 2>/dev/null)"

matched_scripts=()
while IFS= read -r entry_json; do
  [ -n "$entry_json" ] || continue
  pattern="$(printf '%s' "$entry_json" | jq -r '.tool_pattern')"
  if [ "$pattern" = "null" ] || [[ "$tool_name" =~ ^($pattern)$ ]]; then
    while IFS= read -r script_name; do
      [ -n "$script_name" ] && matched_scripts+=("$script_name")
    done < <(printf '%s' "$entry_json" | jq -r '.scripts[]')
  fi
done < <(jq -c --arg ev "$event" '.[$ev] // [] | .[]' "$table_file" 2>/dev/null)

blocked=0
block_messages=()
context_parts=()

for script_name in "${matched_scripts[@]}"; do
  script_path="$hooks_dir/$script_name"
  [ -x "$script_path" ] || continue

  err_file="$(mktemp)"
  out="$(printf '%s' "$input" | "$script_path" 2>"$err_file")"
  rc=$?
  err="$(cat "$err_file" 2>/dev/null)"
  rm -f "$err_file"

  if [ "$rc" -eq 2 ]; then
    blocked=1
    [ -n "$err" ] && block_messages+=("[$script_name] $err")
  fi

  if [ -n "$out" ]; then
    ctx="$(printf '%s' "$out" | jq -r '.hookSpecificOutput.additionalContext // empty' 2>/dev/null)"
    [ -n "$ctx" ] && context_parts+=("$ctx")
  fi
done

if [ "$blocked" -eq 1 ]; then
  printf '%s\n\n' "${block_messages[@]}" >&2
  exit 2
fi

if [ "${#context_parts[@]}" -gt 0 ]; then
  combined="$(printf '%s\n\n' "${context_parts[@]}")"
  jq -n --arg ctx "$combined" --arg ev "$event" \
    '{hookSpecificOutput: {hookEventName: $ev, additionalContext: $ctx}}'
fi

exit 0
