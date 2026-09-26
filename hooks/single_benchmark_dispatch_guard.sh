#!/bin/bash
# PreToolUse Bash hook: block firing a `reyn eval benchmark` dispatch (or a
# dispatch_*.sh wrapper that runs one) while a PRIOR benchmark dispatch is still
# running. Concurrent dispatches (a) overwrite each other's trace/output files
# mid-analysis → corrupted reads, and (b) violate TPM safety (parallel gemini
# calls beyond concurrency=1).
#
# Trip (2026-05-30): re-fired the 5-Class-A + N=3 dispatch scripts multiple times
# without checking prior ones had finished → 8+ concurrent reyn-eval processes
# rewriting the same llm_trace_*.jsonl files. This produced hours of
# "inconsistent reads" that I mis-attributed to env flakiness / grep patterns.
# Root cause was concurrent-dispatch file corruption + TPM violation.
#
# Cost-0 early-exits:
#   1. not a Bash tool call → exit 0
#   2. command doesn't launch a reyn benchmark dispatch → exit 0
# Block (exit 2) only when the command WOULD launch a dispatch AND a prior
# `reyn eval benchmark` process is already running.

input=$(cat)

tool=$(echo "$input" | jq -r '.tool_name // ""' 2>/dev/null)
[ "$tool" = "Bash" ] || exit 0

cmd=$(echo "$input" | jq -r '.tool_input.command // ""' 2>/dev/null)
[ -z "$cmd" ] && exit 0

# Does this command launch a benchmark dispatch?
#  - direct: contains "reyn eval benchmark"
#  - wrapper: invokes a dispatch_*.sh script that runs one
launches=0
echo "$cmd" | grep -qE 'reyn[[:space:]]+eval[[:space:]]+benchmark' && launches=1
echo "$cmd" | grep -qE 'dispatch_[A-Za-z0-9_]*\.sh' && launches=1
[ "$launches" = "1" ] || exit 0

# Allow pure inspection commands that merely mention the dispatch (grep/cat/ls/ps/pgrep/kill)
# without actually launching it. If the command's first meaningful token is an
# inspection verb, don't block.
first=$(echo "$cmd" | sed -E 's/^[[:space:]]*//; s/^cd[[:space:]]+[^;&|]+[;&|]+[[:space:]]*//' | awk '{print $1}')
case "$first" in
  grep|cat|ls|ps|pgrep|pkill|kill|head|tail|wc|echo|less|tail|find|rg|ugrep) exit 0 ;;
esac

# Is a prior benchmark dispatch already running?
running=$(pgrep -f "reyn eval benchmark" 2>/dev/null | wc -l | tr -d ' ')
if [ "${running:-0}" -gt 0 ]; then
  cat <<EOF >&2
[single_benchmark_dispatch_guard] BLOCK: a prior 'reyn eval benchmark' dispatch
is still running (${running} process(es)).

Firing another dispatch now would:
  - overwrite the in-flight run's trace/output files mid-analysis (= the
    "inconsistent read" corruption from 2026-05-30), AND
  - run concurrent LLM calls beyond concurrency=1 (= TPM safety violation,
    gemini-2.5-flash-lite TPM 4M).

Do ONE of:
  1. Wait for the running dispatch to finish (check: pgrep -fl "reyn eval benchmark";
     the background task will notify on completion).
  2. If the running dispatch is stale/runaway, stop it first:
     pkill -f "reyn eval benchmark"; pkill -f "dispatch_.*\.sh"
     then re-fire ONCE.

Never re-fire a dispatch script without confirming the prior instance exited.
EOF
  exit 2
fi

exit 0
