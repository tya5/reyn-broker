#!/bin/bash
# See echo_stdin_a.sh -- the SECOND script in a 2-script dispatch table, so
# the test can catch a fan-out bug that only reaches the first dispatched
# script (a single-script fixture cannot distinguish "consumed and
# forwarded" from "consumed once, never forwarded again").
input=$(cat)
printf '%s' "$input" > "$OUT_FILE_B"
exit 0
