#!/bin/bash
# Fixture for tests/test_dispatcher_dispatch.py -- always exits 2 with a
# distinctive stderr marker, regardless of stdin. Used to prove dispatcher.sh
# surfaces a dispatched script's block (exit 2 + its stderr) rather than
# silently downgrading it to advice (e.g. by only inspecting stdout).
cat >/dev/null
echo "PROBE-BLOCK-MARKER: always_block.sh fixture fired" >&2
exit 2
