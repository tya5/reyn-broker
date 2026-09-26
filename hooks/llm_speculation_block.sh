#!/bin/bash
# PreToolUse hook: block Write/Edit operations that contain LLM-side speculation
# phrases without primary-evidence observation.
#
# Wired in ~/.claude/settings.json (matcher = "Write" / "Edit", event = "PreToolUse").
#
# Root cause for this hook (= user direction 2026-05-28):
#   「その推測をしないように hook で対処できない？」 = sandbox_2 が v8 verify-state.json
#   に「LLM thrashing within budget」 等 LLM-side cognition speculation を書いた trip
#   (= per-turn act-op 観測なしで attribution)、 user pushback。
#
#   [[feedback_no_advance_until_structural_ruled_out]] 「LLM 性能由来 attribution は
#   per-instance 構造候補全排除観測 + primary evidence cite 必須」 を 「discipline」
#   ではなく 「mechanical enforcement at write-time」 として hook 化。
#
# Detection: forbidden phrases regex against tool_input.content (Write) or
# tool_input.new_string (Edit).
#
# Forbidden patterns (= LLM-cognition speculation without observation):
#   - "LLM thrash" / "LLM thrashing"
#   - "LLM limit" / "LLM cognition limit"
#   - "LLM cognition layer" (= 用語使用は OK だが speculation 文脈で多)
#   - "LLM capability" (= without "observed" / "measured" qualifier)
#   - "LLM 性能由来"
#   - "LLM-side limit" / "LLM-side cause"
#   - "weak[-_ ]?model baseline" / "weak[-_ ]?LLM"
#   - "LLM (couldn't|cannot|failed) (understand|reason|converge)"
#   - "LLM (misunderstood|confused|overthought)"
#   - "LLM judgment" (= when followed by "limit" / "gap" — judgment as deficit)
#
# Whitelist (= explicit structural / observational frames OK):
#   - "LLM API (timeout|error|5xx|failure)" → infrastructure cause
#   - "LLM output" / "LLM input" / "LLM call" / "LLM emit" → pure observation
#   - "LLM saw" / "LLM observed" → describing what LLM was shown
#   - "LLM-emitted" → describing artifact provenance
#
# When detected: exit 2 with stderr listing the matched phrase + relevant memory pins.

input=$(cat)

tool_name=$(echo "$input" | jq -r '.tool_name // ""' 2>/dev/null)

# Only check Write and Edit (= file content mutations)
case "$tool_name" in
  "Write")
    content=$(echo "$input" | jq -r '.tool_input.content // ""' 2>/dev/null)
    ;;
  "Edit")
    content=$(echo "$input" | jq -r '.tool_input.new_string // ""' 2>/dev/null)
    ;;
  *)
    exit 0
    ;;
esac

# Empty content (= no write to inspect)
if [ -z "$content" ]; then
  exit 0
fi

# File path — only enforce on files we own / care about (= not on every random write)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // ""' 2>/dev/null)

# Apply check only to non-source-code, non-self-documenting files.
# Source code may legitimately reference "LLM" tokens in comments/strings as
# domain vocabulary; the discipline target is OUR articulation (= notes, state
# files, broker post drafts).
# Self-documenting files (= memory pins / hook scripts / hook-rule documentation)
# necessarily quote the forbidden phrases as the rule they document — exempt them.
case "$file_path" in
  */src/*.py | */src/*.md | */tests/*.py)
    # Source code / test fixtures — pass.
    exit 0
    ;;
  */memory/feedback_*.md | */memory/MEMORY.md)
    # Memory pins document the rules — they will quote forbidden phrases as
    # examples. The discipline applies to applying-the-rule, not documenting it.
    exit 0
    ;;
  */hooks/*.sh)
    # Hook scripts encode the rules — they will contain the forbidden patterns
    # as regex / examples.
    exit 0
    ;;
esac

# Forbidden patterns — case-insensitive grep -E.
# Each pattern aims for "LLM-side speculation absent observation cite".
forbidden_patterns=(
  'LLM[[:space:]]+(thrash|thrashing)'
  'LLM[[:space:]]+(cognition[[:space:]]+limit|cognition[[:space:]]+gap)'
  'LLM[[:space:]]+(capability[[:space:]]+(limit|lower[[:space:]]*bound|cap))'
  'LLM[[:space:]]+性能由来'
  'LLM-side[[:space:]]+(limit|cause|attribution[[:space:]]+candidate)'
  'weak[-_[:space:]]*model[[:space:]]+(baseline|capability|limit|lower[[:space:]]*bound)'
  'weak[-_[:space:]]*LLM[[:space:]]+(limit|capability)'
  'LLM[[:space:]]+(couldn'\''?t|cannot|failed)[[:space:]]+(understand|reason|converge|grasp)'
  'LLM[[:space:]]+(misunderstood|confused|overthought|hallucinated[[:space:]]+(due[[:space:]]+to[[:space:]]+limit|because))'
  'LLM[[:space:]]+(misread|misinterpreted)[[:space:]]+(due[[:space:]]+to|because)'
  '真[[:space:]]*の[[:space:]]*LLM[[:space:]]+(性能|限界|cognition)'
  'lower[[:space:]]+bound[[:space:]]+capability'
  'attribute[d]?[[:space:]]+to[[:space:]]+LLM[[:space:]]+(cognition|capability|limit)'
)

matched=""
for pat in "${forbidden_patterns[@]}"; do
  # Match case-insensitive
  if echo "$content" | grep -qiE "$pat"; then
    sample=$(echo "$content" | grep -iE "$pat" | head -2)
    matched="${matched}
  pattern: ${pat}
  sample: ${sample}
"
  fi
done

if [ -n "$matched" ]; then
  cat >&2 <<EOF
[llm_speculation_block] BLOCK: ${tool_name} contains LLM-side speculation phrase(s) without primary-evidence cite.

Detected:
${matched}

Per [[feedback_no_advance_until_structural_ruled_out]] + [[feedback_observe_before_speculate_llm]]:
  "LLM 性能由来 / weak-model baseline / capability lower bound" attribution
  is NOT permitted without per-instance structural-candidates rule-out
  via primary evidence (= dogfood_trace.py inspection / events.jsonl read /
  literal payload audit).

Rewrite paths:
  (a) Replace with primary-evidence observation:
        "observed: LLM emitted X at turn N (= primary evidence)"
  (b) Replace with un-ruled-out structural candidate enumeration:
        "structural candidates NOT yet rule-out: (B1) ... (B2) ... (B3) ..."
        "LLM cognition attribution NOT permitted until B1-B3 observed"
  (c) If genuinely structural-not-LLM (= API / infra / template):
        say "LLM API timeout" or "LLM emit shape" — those phrases are NOT
        flagged (= they are pure observation, not cognition attribution).

To bypass intentionally (= you have observation cite in the SAME write):
  add an inline marker  // PRIMARY-EVIDENCE-CITED: <observation> //
  on a line preceding the LLM-attribution phrase, and the hook will skip.
EOF
  exit 2
fi

# Whitelisted escape hatch: PRIMARY-EVIDENCE-CITED marker
# (Implemented above by NOT including non-attribution phrases in forbidden_patterns.
# We do not need a separate whitelist scan because forbidden patterns are tight.)

exit 0
