#!/bin/bash
# scripts/install_hooks.sh -- copies this repo's hooks/ (dispatcher.sh,
# dispatch_table.json, and every hooks/*.sh) into the location Claude Code
# hook events actually invoke: ~/.claude/hooks/ (override via
# CLAUDE_HOOKS_DIR, used only by tests/test_install_hooks.py).
#
# Why a copy at all, and not registering the repo checkout's own hooks/
# directly (architect ruling, issue #34 follow-up): the moment
# ~/.claude/settings.json's `command` points at a path inside this repo,
# that working tree becomes a runtime dependency of every reyn_dev session.
# `git checkout` on this repo is a routine operation, not a rare accident,
# and a missing/wrong dispatcher.sh reports NOTHING -- Claude Code was
# measured (issue #34) to exit a turn normally, rc=0, with no warning, when
# a registered hook command does not exist. So the fix has to remove the
# working tree from the command's resolution path entirely, not add a
# watchdog for when it breaks:
#
#   - Execution:  the installed copy under CLAUDE_HOOKS_DIR (`git checkout`
#                 here never touches it).
#   - Canon:      this repo.
#   - Update:     re-run this script (one command).
#
# Idempotent: running this twice with no repo changes produces
# byte-identical installed files and an unchanged .hooks_version stamp.
#
# Never deletes anything in the target directory outside the managed files
# below. ~/.claude/hooks/ also holds project-scope hooks
# (block_wide_pytest.sh, invalidate_stop_decision.sh,
# what_are_you_waiting_for.sh) registered by unrelated project
# .claude/settings.json files (lead-coder / architect / e2e-coder /
# tui-coder) -- this script must never remove-then-repopulate the whole
# directory, only add/overwrite the files it manages.
#
# This script never writes ~/.claude/settings.json. Pointing
# PreToolUse/PostToolUse/Stop hook commands at the installed dispatcher.sh
# is a manual, one-time step for a human to make (this script prints what
# to write, at the end) -- an unattended install must never be able to
# change what a hook event invokes.
set -eu

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hooks_src="$repo_root/hooks"
target_dir="${CLAUDE_HOOKS_DIR:-$HOME/.claude/hooks}"

mkdir -p "$target_dir"

# The managed set: every hooks/*.sh plus dispatch_table.json. Same glob
# scripts/hooks_version.sh hashes -- kept identical on purpose (README.md
# and anything else in hooks/ is repo documentation, not installed).
managed=()
while IFS= read -r f; do
  managed+=("$f")
done < <(cd "$hooks_src" && ls *.sh dispatch_table.json 2>/dev/null | sort)

if [ "${#managed[@]}" -eq 0 ]; then
  echo "install_hooks.sh: found no managed files under $hooks_src -- refusing to touch $target_dir" >&2
  exit 1
fi

for f in "${managed[@]}"; do
  cp "$hooks_src/$f" "$target_dir/$f"
  case "$f" in
    *.sh) chmod +x "$target_dir/$f" ;;
  esac
done

version="$("$repo_root/scripts/hooks_version.sh" "$hooks_src")"
printf '%s\n' "$version" > "$target_dir/.hooks_version"

echo "Installed ${#managed[@]} file(s) from $hooks_src to $target_dir (version $version)."
echo
echo "If ~/.claude/settings.json's PreToolUse/PostToolUse/Stop hook commands"
echo "do not yet point here, register (once, by hand):"
echo "  $target_dir/dispatcher.sh <event>"
echo "This script never edits settings.json itself."
