#!/bin/bash
# Fixture for tests/test_dispatcher_dispatch.py -- NOT a real hook, lives
# under tests/fixtures/ (not hooks/) so it never collides with
# test_hooks_dispatch_reachable.py's reachability gate.
#
# Records exactly what stdin it received, byte for byte, to $OUT_FILE_A so
# the test can assert dispatcher.sh's stdin fan-out did not truncate,
# re-encode, or (worse) only reach the FIRST dispatched script.
input=$(cat)
printf '%s' "$input" > "$OUT_FILE_A"
exit 0
