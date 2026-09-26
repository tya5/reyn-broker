#!/bin/bash
# scripts/hooks_version.sh <hooks_dir> -- prints a content-hash stamp over
# the MANAGED files in <hooks_dir>: every *.sh plus dispatch_table.json
# (README.md and anything else is not part of what gets installed or
# compared -- see scripts/install_hooks.sh and hooks/README.md).
#
# Used from TWO call sites that must never hash differently:
#   1. scripts/install_hooks.sh -- writes this repo's current hash into the
#      installed copy's .hooks_version stamp.
#   2. hooks/dispatcher.sh's drift warn -- recomputes this repo's CURRENT
#      hash (from wherever REYN_BROKER_REPO_DIR points) and compares it
#      against that stamp, at hook-run time.
# Keeping the hashing logic in exactly one script (rather than duplicated
# inline in both) is what makes "install wrote X, dispatcher reads Y" not a
# way for the two to silently drift apart from each other.
#
# A hash, not a hand-bumped version number: this must catch an uncommitted
# working-tree edit too (issue #34 -- the repo can be "mid-branch" with no
# commit to bump a number on), so the input has to be the files' actual
# bytes, not something a person remembers to increment.
set -eu

hooks_dir="$1"

sha_cmd() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256
  else
    sha256sum
  fi
}

files=()
while IFS= read -r f; do
  files+=("$f")
done < <(cd "$hooks_dir" && ls *.sh dispatch_table.json 2>/dev/null | sort)

for f in "${files[@]}"; do
  # Filename is part of the hashed stream (not just file contents) so
  # renaming or adding/removing a managed file changes the stamp too, not
  # only editing one's body.
  printf '%s\n' "$f"
  cat "$hooks_dir/$f"
done | sha_cmd | awk '{print $1}'
