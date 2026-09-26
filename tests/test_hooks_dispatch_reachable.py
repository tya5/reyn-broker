"""Tier 1: hooks/dispatch_table.json's scripts are exactly hooks/*.sh minus dispatcher.sh.

The contract this asserts: every hook script the repo carries is reachable
through dispatcher.sh's dispatch table, and every script the table names
actually exists on disk. This is the CI gate for "wrote an 11th hook script,
forgot to add it to the dispatch table" (#34) -- without it, a hook added
straight to hooks/ silently never fires (or, if someone edits
~/.claude/settings.json by hand instead, fires unguarded outside this repo
entirely, which is the incident this issue is about).
"""

from __future__ import annotations

import json
from pathlib import Path

HOOKS_DIR = Path(__file__).resolve().parent.parent / "hooks"

# Files in hooks/ that are infrastructure, not dispatchable hook scripts.
NOT_A_HOOK = {"dispatcher.sh"}


def _dispatch_table() -> dict:
    with open(HOOKS_DIR / "dispatch_table.json", encoding="utf-8") as f:
        return json.load(f)


def _scripts_named_in_table(table: dict) -> set[str]:
    named: set[str] = set()
    for value in table.values():
        if not isinstance(value, list):
            continue
        for entry in value:
            named.update(entry.get("scripts", []))
    return named


def test_every_hook_script_is_named_in_the_dispatch_table() -> None:
    on_disk = {
        p.name for p in HOOKS_DIR.glob("*.sh") if p.name not in NOT_A_HOOK
    }
    assert on_disk, "expected at least one hooks/*.sh script on disk"

    named = _scripts_named_in_table(_dispatch_table())

    missing = on_disk - named
    assert not missing, (
        f"hooks/{sorted(missing)} exist on disk but are not wired into any "
        "event in dispatch_table.json -- dispatcher.sh will never run them"
    )


def test_dispatch_table_never_names_a_script_that_does_not_exist() -> None:
    named = _scripts_named_in_table(_dispatch_table())
    on_disk = {p.name for p in HOOKS_DIR.glob("*.sh")}

    dangling = named - on_disk
    assert not dangling, (
        f"dispatch_table.json names {sorted(dangling)}, which do not exist "
        "under hooks/"
    )


def test_dispatch_table_never_names_dispatcher_itself() -> None:
    named = _scripts_named_in_table(_dispatch_table())
    assert "dispatcher.sh" not in named, (
        "dispatcher.sh dispatching to itself would recurse; it must never "
        "appear as a dispatched script in its own table"
    )


def test_dispatch_table_keys_are_known_hook_events() -> None:
    known_events = {"PreToolUse", "PostToolUse", "Stop"}
    table = _dispatch_table()
    event_keys = {k for k in table if not k.startswith("_")}
    assert event_keys <= known_events, (
        f"dispatch_table.json has event key(s) {event_keys - known_events} "
        f"outside the known set {known_events} -- dispatcher.sh's CLI arg "
        "contract only accepts these"
    )
