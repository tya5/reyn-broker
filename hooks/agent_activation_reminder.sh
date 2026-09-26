#!/bin/bash
# PostToolUse hook: agent activation trigger reminder.
# Wired in ~/.claude/settings.json (matcher = "Write").
# Triggered when Claude writes specific files that should activate peer agents.
#
# Root cause for this hook:
#   - 2026-05-28 user direction「あなたが彼らをうまく使いこなせるか心配 / hook などの
#     仕組みで自動化できるところは自動化」
#   - [[feedback_lead_coder_agent_activation_obligation]] N=1 + N=2 trip log で
#     「lead-coder agency under-recognition」 + 「activation obligation 履行漏れ」
#     instance accumulate、 memory pin 単独 insufficient confirmed。
#   - mechanical safety net で lead-coder agent activation discipline 強化。
#
# Activation triggers:
#   - feedback_*.md write → memory-curator broadcast 依頼 + docs-maintainer
#     cross-reference audit 依頼 reminder
#   - docs/*.md write (= reference / deep-dives / concepts) → docs-maintainer
#     drift audit 依頼 reminder
#   - README.md write → docs-maintainer audit 依頼 reminder

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // ""' 2>/dev/null)

reminder=""

# Trigger 1: feedback_*.md (= memory pin write)
if echo "$file_path" | grep -qE "/memory/feedback_.*\.md$"; then
  pin_name=$(basename "$file_path" .md)
  reminder="memory pin write detected: ${pin_name}. [[feedback_lead_coder_agent_activation_obligation]] activation obligation: (1) memory-curator へ broadcast 依頼 broker post obligation (= 全 5 peer 各 role 向け具体例付き cross-share)、 (2) docs-maintainer へ cross-reference audit 依頼 broker post (= dangling reference detect + matrix integrity check)。 ALWAYS post_message(to=\"memory-curator\" + to=\"docs-maintainer\") 同 turn 内実投函必須、 「次に投函予定」 frame 禁止 ([[feedback_broker_post_articulate_action_gap]] N=2 trap class)。"
fi

# Trigger 2: docs/*.md (= reference / deep-dives / concepts) or README.md
if echo "$file_path" | grep -qE "/(docs/.*\.md|README\.md)$"; then
  doc_path=$(echo "$file_path" | sed 's|.*/reyn_dev/[^/]*/||')
  if [ -z "$reminder" ]; then
    reminder="docs write detected: ${doc_path}. [[feedback_lead_coder_agent_activation_obligation]] activation obligation: docs-maintainer へ drift audit 依頼 broker post obligation (= touched file vs code reality sync gap check + CLAUDE.md hard rule sync verify)。 ALWAYS post_message(to=\"docs-maintainer\") 同 turn 内実投函必須。"
  else
    reminder="${reminder} 加えて docs write detected: ${doc_path}、 docs-maintainer audit 依頼も同 broker post に含める。"
  fi
fi

if [ -n "$reminder" ]; then
  # Emit Claude Code hook JSON: additionalContext appended to next prompt.
  jq -n --arg ctx "$reminder" '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $ctx}}'
fi

exit 0
