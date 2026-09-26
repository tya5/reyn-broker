#!/bin/bash
# PreToolUse hook: gate Write/Edit operations on memory pin files where the
# YAML frontmatter `description:` field exceeds 200 chars (= MEMORY.md index
# governance rule).
#
# Wired in ~/.claude/settings.json (matcher = "Write|Edit", event = "PreToolUse").
#
# Root cause for this hook (= user direction 2026-05-28 sweep wave reflection):
#   sweep 結果 = 92 pins / 59 violations、 うち description_too_long が >50%、
#   私 sandbox_2 直近 6 新 pin 中 4 件 が 200 超過 land = author-side check
#   欠落 real risk。 memory-curator 評価で hook candidate articulate、 implement。
#
# COST OPTIMIZATION:
#   1. **Path-filter early-exit**: file_path が */memory/*.md でない → exit 0 即時
#   2. **MEMORY.md skip**: index file は frontmatter なし、 skip
#   3. **Description-extracted-from-content only**: Edit で新 string に description
#      field が含まれない場合 (= 部分 edit) は skip — 次の Write で catch
#   4. **Single regex parse**: minimal cost per matching write

input=$(cat)

tool_name=$(echo "$input" | jq -r '.tool_name // ""' 2>/dev/null)

# Only check Write and Edit
case "$tool_name" in
  "Write"|"Edit") ;;
  *) exit 0 ;;
esac

file_path=$(echo "$input" | jq -r '.tool_input.file_path // ""' 2>/dev/null)

# Path filter at top (= early-exit, no parse overhead)
case "$file_path" in
  */memory/*.md) ;;
  *) exit 0 ;;
esac

# Skip MEMORY.md (= index file, has its own format constraints)
case "$(basename "$file_path")" in
  MEMORY.md) exit 0 ;;
esac

# Extract candidate description from tool_input
case "$tool_name" in
  "Write")
    content=$(echo "$input" | jq -r '.tool_input.content // ""' 2>/dev/null)
    ;;
  "Edit")
    content=$(echo "$input" | jq -r '.tool_input.new_string // ""' 2>/dev/null)
    ;;
esac

if [ -z "$content" ]; then
  exit 0
fi

# Find description field — only check if the content actually contains one
# (= for Edit, partial edits may not touch description, skip those)
if ! echo "$content" | grep -qE '^description:'; then
  exit 0
fi

# Extract description value
# Pattern: description: "..." (= quoted) or description: ... (= unquoted, single line)
desc=$(echo "$content" | awk '
  /^description:[[:space:]]+/ {
    # Strip leading "description:" + whitespace
    sub(/^description:[[:space:]]+/, "")
    # If starts with quote, capture until matching close quote on same line
    if (substr($0, 1, 1) == "\"") {
      # Strip leading + trailing quote
      sub(/^"/, "")
      sub(/"[[:space:]]*$/, "")
    }
    print
    exit
  }
')

# Count characters
n_chars=${#desc}

if [ "$n_chars" -gt 200 ]; then
  excess=$((n_chars - 200))
  preview=$(echo "$desc" | head -c 100)
  cat >&2 <<EOF
[memory_description_200char_check] BLOCK: ${tool_name} on ${file_path} has description field of ${n_chars} chars (= ${excess} over 200-char rule).

Preview (first 100 chars):
  ${preview}...

Per MEMORY.md index governance rule (= user direction 2026-05-28 sweep wave):
  description field must be ≤200 chars, suitable as a one-line summary in
  the MEMORY.md index table.

Rewrite paths:
  (a) Compress redundant phrasing — keep the "what" + "why pin matters",
      drop full examples (= those belong in the body).
  (b) Move longer narrative into the body's first paragraph; description
      remains the index-line summary.
  (c) Split into 2 pins if the description genuinely needs >200 chars
      to describe distinct concerns.

Bypass: not provided by design. ≤200 char is the governance contract.
EOF
  exit 2
fi

exit 0
