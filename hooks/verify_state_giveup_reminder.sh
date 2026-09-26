#!/bin/bash
# PostToolUse hook: fire ONCE when verify-state.json has unresolved entries
# (= status=error + structural_cause_verified=false + fix_applied=null), to
# remind that "giveup" classification is an option per user direction 2026-05-28
# 「dogfood trace tool を使っても構造問題をどうしても見つけられない場合は、それを
#   giveup scenario として記録して、v から除外して。」
# Wiring: ~/.claude/settings.json PostToolUse Write|Edit matcher.
#
# COST OPTIMIZATION (= user direction 2026-05-28「hook タイミングはちゃんと
# コスト考慮して最適なタイミングのみになるようにしてね」):
#
#  1. **Early-exit on path mismatch**: if file_path does NOT end with
#     `verify-state.json`, exit 0 immediately (= no jq spawn, no parsing).
#  2. **Throttle via mtime marker**: only fire reminder if state file's
#     mtime has changed since last fire (= one reminder per write-burst,
#     not per every keystroke-equivalent edit).
#  3. **Silent when nothing to remind about**: if all entries are resolved
#     (= cause-found OR giveup), exit 0 silently.

input=$(cat)

file_path=$(echo "$input" | jq -r '.tool_input.file_path // ""' 2>/dev/null)

# Path filter at top (= early-exit, no jq overhead beyond initial parse)
case "$file_path" in
  */verify-state.json) ;;
  *) exit 0 ;;
esac

# File must exist (= PostToolUse fires after write, so it should)
if [ ! -f "$file_path" ]; then
  exit 0
fi

# Throttle marker — only emit reminder once per unique mtime
state_mtime=$(stat -f %m "$file_path" 2>/dev/null || stat -c %Y "$file_path" 2>/dev/null)
marker_file="/tmp/.verify_state_giveup_reminder_last_mtime"
last_mtime=$(cat "$marker_file" 2>/dev/null || echo "")

if [ "$state_mtime" = "$last_mtime" ]; then
  exit 0
fi

# Scan for unresolved entries
unresolved=$(jq -r '
  to_entries | map(select(.value | type == "object" and has("instances"))) |
  map(.value.instances // {} | to_entries | map(
    . as $kv |
    if ($kv.value.status == "error" and
        ($kv.value.structural_cause_verified == false or $kv.value.structural_cause_verified == null) and
        (($kv.value.fix_applied // "") == "")) then
      $kv.key
    else
      empty
    end
  )) | flatten | .[]
' "$file_path" 2>/dev/null)

if [ -z "$unresolved" ]; then
  echo "$state_mtime" > "$marker_file"
  exit 0
fi

# Count + sample
n_unresolved=$(echo "$unresolved" | wc -l | tr -d ' ')
sample=$(echo "$unresolved" | head -3 | sed 's/^/    - /')

reminder="**Giveup-classification reminder** (= cost-optimal hook fire trigger): \
verify-state.json has ${n_unresolved} entries with status=error + \
structural_cause_verified=false + fix_applied=null (= investigation \
unresolved). Sample:
${sample}

If you have attempted **dogfood_trace.py inspection** + structural-candidate \
rule-out for these instances and the cause is genuinely undiscoverable, \
per user direction 2026-05-28: classify as giveup instead of leaving \
unresolved.

**Giveup entry shape** (= ~/.claude/hooks/swe_bench_v_advance_gate.sh accepts):
\`\`\`json
{
  \"status\": \"giveup\",
  \"exclude_from_subsequent\": true,
  \"primary_evidence\": \"<trace observation explaining why cause is undiscoverable>\",
  \"dogfood_trace_inspection_cite\": \"<cmd / output cite proving inspection was attempted>\"
}
\`\`\`

Giveup'd instances are then excluded from next v's task subset (= the \
v-advance hook also enforces this: a giveup'd instance remaining in \
--tasks file is BLOCKed).

Pins: [[feedback_swe_bench_v_advance_hook_gate]] for the gate mechanism,
[[feedback_no_advance_until_structural_ruled_out]] for the discipline."

# Emit Claude Code hook JSON: additionalContext appended to next prompt.
jq -n --arg ctx "$reminder" '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $ctx}}'

# Update marker so subsequent writes within same mtime don't re-fire
echo "$state_mtime" > "$marker_file"
exit 0
