#!/bin/bash
# PostToolUse hook: detect a `gh pr merge` INVOCATION, then RESOLVE the PR's
# actual state before saying anything about it.
#
# #5269 (two rounds). Round 1 fixed the WORDING; architect's point closed the
# rest: the wording changed but the FIRING CONDITION did not, so a failed
# merge -- or a bare `--auto` arming, or the phrase merely appearing inside a
# heredoc -- still fired the broker-post obligation, leaving "check the result
# yourself" as a DISCIPLINE. The referent is cheaply pullable here, so the
# hook pulls it: `gh pr view <N> --json state,mergedAt`. Three outcomes, and
# the obligation fires on exactly one of them.
#
# SCOPE (architect, #5269): the pull answers "is that PR merged NOW", NOT
# "did YOUR command merge it" -- re-running against an already-merged PR also
# says MERGED. That only ever errs on the safe side of the harm this closes
# (claiming merged when nothing landed), so it is not a defect; it is written
# here so a later reader does not take the checkmark as a witness of
# authorship.
#
# Never assert MERGED without that pull. If the pull fails (offline, no gh,
# wrong repo, the number came from surrounding text and does not exist), say
# the verification failed -- an unverifiable state is not a merged state.
#
# Wired in ~/.claude/settings.json (matcher = "Bash").

input=$(cat)
cmd=$(echo "$input" | jq -r '.tool_input.command // ""' 2>/dev/null)

echo "$cmd" | grep -qE "gh pr merge[[:space:]]+[0-9]+.*--(squash|merge|rebase)" || exit 0

pr=$(echo "$cmd" | grep -oE "merge[[:space:]]+[0-9]+" | grep -oE "[0-9]+" | head -1)
[ -n "$pr" ] || exit 0

obligation="broker post 必須 obligation (= [[feedback_broker_post_articulate_action_gap]] N=2 sub-discipline): (1) author への merge ack (= 該当する場合: calibration / sustain / next-action trigger 通知)、 (2) downstream peer への cascade trigger 通知 (= 例: PR land で peer next-task trigger 条件成立)、 (3) end-of-turn summary に明示 articulate。 「merge MERGED ✓」 のみで turn 終了 = 通知漏れ trap。 該当ないケースは silent OK (= state-echo ack 不要)、 ただし 「該当ない」 判断は明示。"

# Resolve the referent. Bounded so a hung network cannot stall the session.
TO=""
command -v timeout  >/dev/null 2>&1 && TO="timeout 15"
[ -z "$TO" ] && command -v gtimeout >/dev/null 2>&1 && TO="gtimeout 15"

state=""
merged_at=""
if command -v gh >/dev/null 2>&1; then
  out=$($TO gh pr view "$pr" --json state,mergedAt 2>/dev/null)
  if [ -n "$out" ]; then
    state=$(echo "$out" | jq -r '.state // ""' 2>/dev/null)
    merged_at=$(echo "$out" | jq -r '.mergedAt // ""' 2>/dev/null)
  fi
fi

if [ "$state" = "MERGED" ]; then
  reminder="✅ PR #${pr} は MERGED です (mergedAt=${merged_at}、この hook が \`gh pr view ${pr} --json state,mergedAt\` を引いて確認しました)。 ${obligation}"
elif [ -n "$state" ]; then
  reminder="🔴 PR #${pr} は **${state}** です — **merge されていません。** \`gh pr merge\` は打たれましたが、着地していません（失敗した／\`--auto\` で武装しただけ／あるいはこの番号がコマンド文中の別の文字列から拾われた、のいずれか）。**merged と書かないでください。** broker post の obligation は発火させません（#5269）。"
else
  reminder="⚠️ PR #${pr} の状態を確認できませんでした（\`gh pr view ${pr} --json state,mergedAt\` が失敗）。**確認できない状態は merged ではありません。** 自分で引いてから、merge されていた場合にのみ以下を行ってください: ${obligation}"
fi

jq -n --arg ctx "$reminder" '{hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $ctx}}'
exit 0
