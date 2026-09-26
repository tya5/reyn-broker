#!/bin/bash
# Fixture for tests/test_dispatcher_dispatch.py -- always exits 0 (never
# blocks), regardless of stdin. Used to prove dispatcher.sh's exit-2
# aggregation is NOT a "always exit 2" implementation: a dispatch table of
# only always-passing scripts must leave dispatcher.sh exiting 0.
cat >/dev/null
exit 0
