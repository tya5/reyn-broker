#!/bin/bash
# PreToolUse hook: gate `reyn eval benchmark swe_bench ... --output .../results_retry_vN`
# dispatch until all 10 instances of v(N-1) have structural countermeasures verified.
#
# Wired in ~/.claude/settings.json (matcher = "Bash", event = "PreToolUse").
#
# Root cause for this hook (= user direction 2026-05-28):
#   「10件全て対策しないと次の v に進まないよう hook で対策して」
#   = PASS していない全 scenario について 構造問題 not-ruled-out のうちは
#     次 v retry 不許可 ([[feedback_no_advance_until_structural_ruled_out]] +
#     [[feedback_all_failures_structural_verify_obligation]] hard gate
#     mechanical enforcement)。
#
# State file: /tmp/swe-bench-cal/verify-state.json
#   {
#     "v<N>": {
#       "instances": {
#         "astropy__astropy-XXXXX": {
#           "status": "done|pass|error",
#           "structural_cause_verified": true|false,
#           "fix_applied": "<PR ref or description>",
#           "primary_evidence": "<trace excerpt / observation>"
#         },
#         ... (10 instances total)
#       }
#     }
#   }
#
# Gate logic:
#   1. Parse target_v from `--output .../results_retry_v<N>` in command.
#   2. If no v(N-1) entry: pass (= first run / bootstrap).
#   3. If v(N-1) entry exists:
#      - Each of 10 instances must have entry.
#      - Each instance must have either:
#        (a) status in ("done", "pass") → no fix needed, OR
#        (b) status == "error" AND structural_cause_verified == true
#            AND fix_applied != null AND primary_evidence != null
#      - Missing or unverified → BLOCK with stderr listing what's missing.

input=$(cat)

# Only check Bash tool calls
tool_name=$(echo "$input" | jq -r '.tool_name // ""' 2>/dev/null)
if [ "$tool_name" != "Bash" ]; then
  exit 0
fi

command_text=$(echo "$input" | jq -r '.tool_input.command // ""' 2>/dev/null)

# Match `reyn eval benchmark swe_bench ...` at command boundary (= actual invocation,
# not quoted text). Boundary = start-of-string, or after pipe/semi/and/newline/space-only.
# This excludes false positives like `echo "reyn eval benchmark swe_bench ..."` or
# `grep "reyn eval benchmark swe_bench" ...`.
if ! echo "$command_text" | grep -qE '(^|[;&|]|^[[:space:]]*|&&[[:space:]]+|\|\|[[:space:]]+)reyn[[:space:]]+eval[[:space:]]+benchmark[[:space:]]+swe_bench'; then
  exit 0
fi

# Additional guard: must have `--output` flag pointing to results_retry_vN (= actual retry pattern).
if ! echo "$command_text" | grep -qE -- '--output[[:space:]]+\S*results_retry_v[0-9]+'; then
  exit 0
fi

# Extract target_v from --output path
target_v=$(echo "$command_text" | grep -oE -- '--output[[:space:]]+\S*results_retry_v[0-9]+' | grep -oE 'results_retry_v[0-9]+' | head -1 | sed 's/results_retry_v//')

if [ -z "$target_v" ]; then
  # No v marker — not the retry pattern; pass.
  exit 0
fi

prior_v=$((target_v - 1))

state_file="/tmp/swe-bench-cal/verify-state.json"

# Bootstrap: if state file doesn't exist, allow the first run (= v1) but warn.
if [ ! -f "$state_file" ]; then
  if [ "$target_v" -le 1 ]; then
    exit 0
  fi
  echo "[swe_bench_v_advance_gate] BLOCK: v${target_v} dispatch requires verify-state.json with v${prior_v} entries." >&2
  echo "[swe_bench_v_advance_gate] State file missing: ${state_file}" >&2
  echo "[swe_bench_v_advance_gate] Per [[feedback_no_advance_until_structural_ruled_out]]: write per-instance countermeasure verification for v${prior_v} (10 instances) before dispatching v${target_v}." >&2
  exit 2
fi

# Read prior_v entries
prior_entry=$(jq -r ".\"v${prior_v}\" // empty" "$state_file" 2>/dev/null)
if [ -z "$prior_entry" ]; then
  echo "[swe_bench_v_advance_gate] BLOCK: v${target_v} dispatch requires v${prior_v} verification entries in ${state_file}." >&2
  echo "[swe_bench_v_advance_gate] Per [[feedback_no_advance_until_structural_ruled_out]] + [[feedback_all_failures_structural_verify_obligation]]: every non-passing v${prior_v} scenario must have structural_cause_verified=true + fix_applied + primary_evidence." >&2
  exit 2
fi

# Check each instance has full verification.
# Passable states:
#   (a) status in {done, pass} → no fix required.
#   (b) status == "error" → structural_cause_verified=true + fix_applied + primary_evidence.
#   (c) status == "giveup" → exclude_from_subsequent=true + dogfood_trace_inspection_cite + primary_evidence
#       (= user direction 2026-05-28「dogfood trace tool を使っても構造問題をどうしても見つけられない場合は giveup scenario として記録して v から除外」).
missing=$(echo "$prior_entry" | jq -r '
  .instances // {} | to_entries | map(
    . as $kv |
    if ($kv.value.status == "done" or $kv.value.status == "pass") then
      empty
    elif ($kv.value.status == "giveup" and
          $kv.value.exclude_from_subsequent == true and
          ($kv.value.dogfood_trace_inspection_cite // "") != "" and
          ($kv.value.primary_evidence // "") != "") then
      empty
    elif ($kv.value.status == "error" and
          $kv.value.structural_cause_verified == true and
          ($kv.value.fix_applied // "") != "" and
          ($kv.value.primary_evidence // "") != "") then
      empty
    else
      "\($kv.key): status=\($kv.value.status // "MISSING") verified=\($kv.value.structural_cause_verified // false) fix=\($kv.value.fix_applied // "MISSING") evidence=\(($kv.value.primary_evidence // "") | if . == "" then "MISSING" else "ok" end) giveup_exclusion=\(($kv.value.exclude_from_subsequent // false) | tostring) trace_inspection=\(($kv.value.dogfood_trace_inspection_cite // "") | if . == "" then "MISSING" else "ok" end)"
    end
  ) | .[]
' 2>/dev/null)

# Additional check: any "giveup" instance with exclude_from_subsequent=true must NOT
# appear in the upcoming --tasks file's subset (= mechanically prevent re-running
# a scenario we already gave up on).
tasks_arg=$(echo "$command_text" | grep -oE -- '--tasks[[:space:]]+\S+' | sed 's/--tasks[[:space:]]*//' | head -1)
if [ -n "$tasks_arg" ] && [ -f "$tasks_arg" ]; then
  giveup_ids=$(echo "$prior_entry" | jq -r '
    .instances // {} | to_entries | map(
      select(.value.status == "giveup" and .value.exclude_from_subsequent == true)
      | .key
    ) | .[]
  ' 2>/dev/null)
  if [ -n "$giveup_ids" ]; then
    while IFS= read -r iid; do
      [ -z "$iid" ] && continue
      if grep -q "\"$iid\"" "$tasks_arg"; then
        echo "[swe_bench_v_advance_gate] BLOCK: giveup'd instance ${iid} is still in --tasks subset ${tasks_arg}." >&2
        echo "[swe_bench_v_advance_gate] Per giveup discipline (= user direction 2026-05-28): exclude_from_subsequent=true instances must be removed from next v's task subset." >&2
        echo "[swe_bench_v_advance_gate] Fix: regenerate --tasks file without ${iid}, or set exclude_from_subsequent=false to opt back in." >&2
        exit 2
      fi
    done <<< "$giveup_ids"
  fi
fi

# Also check count == 10 (or whatever the expected subset size)
instance_count=$(echo "$prior_entry" | jq -r '.instances // {} | length' 2>/dev/null)
expected_count=$(echo "$prior_entry" | jq -r '.expected_instance_count // 10' 2>/dev/null)

if [ "$instance_count" -lt "$expected_count" ]; then
  echo "[swe_bench_v_advance_gate] BLOCK: v${prior_v} has only ${instance_count}/${expected_count} instances entered in ${state_file}." >&2
  echo "[swe_bench_v_advance_gate] Per [[feedback_no_advance_until_structural_ruled_out]]: PASS していない全 scenario について 構造問題 not-ruled-out verify until 次許可禁止。" >&2
  exit 2
fi

if [ -n "$missing" ]; then
  echo "[swe_bench_v_advance_gate] BLOCK: v${prior_v} has unverified instances:" >&2
  echo "$missing" | sed 's/^/[swe_bench_v_advance_gate]   /' >&2
  echo "[swe_bench_v_advance_gate]" >&2
  echo "[swe_bench_v_advance_gate] Per [[feedback_no_advance_until_structural_ruled_out]] + [[feedback_root_cause_not_symptom_fix]]:" >&2
  echo "[swe_bench_v_advance_gate]   each error instance needs structural_cause_verified=true + fix_applied + primary_evidence." >&2
  echo "[swe_bench_v_advance_gate] Edit ${state_file} to add the missing fields, then retry." >&2
  exit 2
fi

# All checks passed
exit 0
