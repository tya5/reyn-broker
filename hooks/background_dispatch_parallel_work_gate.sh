#!/bin/bash
# Stop hook: enforce parallel productive work obligation when a background
# swe_bench dispatch was queued in the current agent turn.
#
# Trip pattern (= 2026-05-30 user pushback「自走モードだったのにとまってる理由を教えて」):
#   dispatch (run_in_background:true) → 1-line ack → STOP
#   = treats "do not poll" as "do nothing" instead of "do OTHER productive work"
#
# Cost-0 paths (= early-exit when hook is not relevant):
#   1. stop_hook_active=true → exit 0 (= already fired this stop, avoid infinite loop)
#   2. transcript_path missing → exit 0
#   3. tail -n 200 → no run_in_background:true in recent transcript → exit 0
#   4. matched line not a swe_bench dispatch → exit 0
#   5. >= 2 tool_use entries after dispatch in current turn → exit 0
#
# Block path: exit 2 with stderr reminder citing productive-work options.
#
# Cross-refs:
#   - [[feedback_dogfood_driver_role]] (= parent: drive autonomously, not idle)
#   - [[feedback_user_intervention_via_issue_label]] (= no proxy questions sibling)
#   - [[feedback_dogfood_batch_roi_gate]] (= pre-batch verify parallel)

input=$(cat)

# Early-exit 1: avoid infinite re-block loop
stop_hook_active=$(echo "$input" | jq -r '.stop_hook_active // false' 2>/dev/null)
if [ "$stop_hook_active" = "true" ]; then
  exit 0
fi

# Early-exit 2: no transcript = nothing to inspect
transcript=$(echo "$input" | jq -r '.transcript_path // ""' 2>/dev/null)
if [ -z "$transcript" ] || [ ! -f "$transcript" ]; then
  exit 0
fi

# Early-exit 3: scan only recent 200 lines (= bounded cost)
# Find the latest line containing "run_in_background": true
recent=$(tail -n 200 "$transcript")
bg_line_offset=$(echo "$recent" | grep -n -E '"run_in_background"[[:space:]]*:[[:space:]]*true' | tail -1 | cut -d: -f1)
if [ -z "$bg_line_offset" ]; then
  exit 0
fi

# Early-exit 4: verify it's a swe_bench dispatch (= same line should have swe_bench keyword)
bg_line_content=$(echo "$recent" | sed -n "${bg_line_offset}p")
if ! echo "$bg_line_content" | grep -q "swe_bench"; then
  exit 0
fi

# Early-exit 5: count tool_use entries after the dispatch line in the recent window
post_dispatch_recent=$(echo "$recent" | tail -n "+$((bg_line_offset + 1))")
post_dispatch_tool_uses=$(echo "$post_dispatch_recent" | grep -c '"type"[[:space:]]*:[[:space:]]*"tool_use"' || true)

if [ "$post_dispatch_tool_uses" -ge 2 ]; then
  # Sufficient productive follow-up work in current turn — allow stop
  exit 0
fi

# Block: insufficient parallel productive work
cat <<EOF >&2
[background_dispatch_parallel_work_gate] BLOCK: background swe_bench dispatch queued
  post-dispatch tool_use count in current turn: ${post_dispatch_tool_uses} (< 2 threshold)

per [[feedback_dogfood_driver_role]] + [[feedback_user_intervention_via_issue_label]]:
parallel productive work obligation activates AT dispatch start, not at completion.

"do NOT poll" ≠ "do nothing" — drive OTHER productive work in parallel:
  (a) verify-state populate / past-comparison draft / next-instance pre-stage
  (b) memory pin lesson articulate (= cycle observations / trip retrospect)
  (c) lead-coder broker progress post (= multi-instance progress summary)
  (d) tier-2 test scaffold pre-stage for next wave
  (e) related cleanup: subset.jsonl generation, output dir hygiene, log analysis

If genuinely no parallel work remaining (= rare), articulate why explicitly in text
response (= "all populate done, awaiting only 13977 completion to compose final
retrospect"). Then re-stop will pass through this hook's stop_hook_active=true gate.
EOF
exit 2
