#!/bin/bash
# Stop hook: block ending a turn with a permission-seeking / role-reversal
# question when the next step is already defined (= dogfood-driver must continue
# autonomously, not ask the user for permission to proceed).
#
# Trip (2026-05-30): ended a turn with "続けてよろしいですか? それとも先に
# 報告しますか?" and stopped until the user said "続けて". Driver-role reversal
# ([[feedback_dogfood_driver_role]]). background_dispatch_parallel_work_gate only
# fires on active bg dispatch; this hook catches end-turn-permission-questions
# when nothing is dispatched.
#
# Matching is python-only (avoids bash-grep/python-re drift), anchored to the
# final line of the last assistant message, and requires a '?' in that line.
#
# Cost-0 early-exits: stop_hook_active, missing transcript, no trailing
# permission phrase.

input=$(cat)

stop_hook_active=$(echo "$input" | jq -r '.stop_hook_active // false' 2>/dev/null)
[ "$stop_hook_active" = "true" ] && exit 0

transcript=$(echo "$input" | jq -r '.transcript_path // ""' 2>/dev/null)
{ [ -z "$transcript" ] || [ ! -f "$transcript" ]; } && exit 0

verdict=$(tail -n 40 "$transcript" 2>/dev/null | python3 -c '
import sys, json, re

last = ""
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    if d.get("type") != "assistant":
        continue
    msg = d.get("message", {})
    if msg.get("role") != "assistant":
        continue
    content = msg.get("content", [])
    if isinstance(content, list):
        for blk in content:
            if isinstance(blk, dict) and blk.get("type") == "text":
                last = blk.get("text", "")

if not last.strip():
    print("OK"); raise SystemExit

lines = last.strip().splitlines()
tail_seg = (lines[-1] if lines else "")[-160:]
has_q = ("?" in tail_seg) or ("？" in tail_seg)
patterns = [
    r"続けてよろし", r"進めてよろし", r"進めてよい", r"進めても(いい|よい)",
    r"よろしいですか", r"よろしいでしょうか", r"進めますか", r"報告しますか",
    r"先に報告", r"どちらにし", r"どうしますか", r"どうしましょう",
    r"してOK", r"いいですか",
    r"shall I (proceed|continue|report)", r"should I (proceed|continue|report)",
    r"want me to (proceed|continue|report)", r"do you want me to",
]
hit = has_q and any(re.search(p, tail_seg) for p in patterns)
print("BLOCK" if hit else "OK")
' 2>/dev/null)

[ "$verdict" = "BLOCK" ] || exit 0

cat <<'EOF' >&2
[no_permission_seeking_stop_gate] BLOCK: turn ends with a permission-seeking question.

You are the dogfood driver — continue autonomously ([[feedback_dogfood_driver_role]]).
Do NOT end a turn asking the user for permission to continue / proceed / report
when the next step is already defined.

- Next action clear → just DO it (run it, report it). Don't ask.
- Genuinely blocked on a user-only decision (design trade-off, scope re-frame,
  spend approval) → `gh issue create --label wait_owner_iv` with the specific
  decision, then continue OTHER parallel work — do not idle.
- "次どうしますか?" / "続けてよろしいですか?" / "先に報告しますか?" = role reversal.
  The user is the reviewer, not the dispatcher.

Rephrase: drop the permission question, state what you are doing, and do it.
EOF
exit 2
