#!/bin/bash
# PostToolUse hook: detect BOOTSTRAP.md write and emit deploy checklist reminder.
# Wired in ~/.claude/settings.json (matcher = "Write").
# Triggered when Claude writes any BOOTSTRAP.md file (= 新 agent deploy step).
#
# Root cause for this hook (N=1 + N=2 lessons):
#   - 2026-05-28 docs-maintainer + backlog-watcher deploy 時、 BOOTSTRAP.md 作成のみで
#     deploy 完了と誤認識、 step 1-3 (folder mkdir + git clone + venv + dependencies
#     install) 履行漏れ instance。
#   - N=2 trip = lead-coder agency under-recognition + 「setup owner 暗黙的 user 期待」
#     trap、 user 直接 trigger 「セットアップ本当に完了してるか」 で初検出。
#   - 真因対策 = lead-coder direct setup execute default、 deploy_agent.sh script で 1-cmd
#     setup path 確立。

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // ""' 2>/dev/null)

if echo "$file_path" | grep -qE "BOOTSTRAP\.md$"; then
  agent_dir=$(dirname "$file_path" | xargs basename)
  reminder="BOOTSTRAP.md write detected for new agent '${agent_dir}'. [[feedback_new_agent_deploy_checklist]] 9-step deploy obligation **lead-coder default** (= setup owner = lead-coder、 「user 直接 setup」 inference reject):

(1) folder mkdir → lead-coder
(2) git clone (= reyn repo 展開、 .git/ + src/ + tests/ + .github/) → lead-coder
(3) venv 作成 + pip install -e '.[dev,mcp,web]' → lead-coder
(4) BOOTSTRAP.md 配置 ← 本 step → lead-coder
(5) tmux session 起動 (= claude --model X cmd) → **user 介入 required** (= 唯一の user 領域)
(6) broker register → peer agent 自走 (= BOOTSTRAP 内 articulate)
(7) Monitor watch 起動 → peer agent 自走
(8) routing config 確認 → 既 session_watcher.py 自動
(9) test message ping ack → lead-coder

**ALWAYS** lead-coder direct setup execute path:
\\\$ ~/.claude/scripts/deploy_agent.sh ${agent_dir}

= 1-cmd で step 1-3 一括 execute (= git clone + venv + pip install + verify)、 lead-coder agency default。

**NEVER** 「BOOTSTRAP.md 作成 = deploy 完了」 誤認識 anti-pattern。

**ALWAYS** verify post-setup: cwd 内 .git/ + .venv/ + .gitignore + .github/ + src/reyn 配置 + reyn import primary evidence backed。

Trip log: N=1 (= step 1-3 履行漏れ) + N=2 (= lead-coder agency under-recognition) accumulate、 memory pin + hook + deploy script の 3-axis 重ね防衛で trap mitigation。"

  # Emit Claude Code hook JSON: additionalContext appended to next prompt.
  jq -n --arg ctx "$reminder" '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $ctx}}'
fi

exit 0
