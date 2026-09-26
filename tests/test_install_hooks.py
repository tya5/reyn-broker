"""Tier 2: scripts/install_hooks.sh's two OS-level invariants -- idempotent,
and it never deletes a file it does not manage.

Background: issue #34's follow-up (architect ruling). dispatcher.sh's
runtime copy must live outside the repo's working tree (a `git checkout`
must never change what a hook event invokes), and the only way that copy
gets refreshed is a human re-running this script. That makes two properties
load-bearing:

  - running it twice must converge, not drift or duplicate -- someone
    re-running it after an unrelated `git checkout` (the exact scenario
    the whole design exists for) must land on the SAME tree as a clean run.
  - it must never wipe the target directory first -- ~/.claude/hooks/ also
    holds project-scope hooks (block_wide_pytest.sh,
    invalidate_stop_decision.sh, what_are_you_waiting_for.sh) that this
    repo does not own. A "delete everything, then repopulate" install would
    destroy those on every run; this is the exact implementation shape the
    brief for this change named and forbade.

Runs entirely against a tmp_path target via CLAUDE_HOOKS_DIR -- never
touches the real ~/.claude/hooks/.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_hooks.sh"
HOOKS_SRC = REPO_ROOT / "hooks"


def _run_install(target_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(INSTALL_SCRIPT)],
        capture_output=True,
        text=True,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": str(target_dir.parent),
            "CLAUDE_HOOKS_DIR": str(target_dir),
        },
    )


def test_install_is_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "hooks"

    r1 = _run_install(target)
    assert r1.returncode == 0, r1.stderr
    snapshot1 = {p.name: p.read_bytes() for p in target.iterdir()}

    r2 = _run_install(target)
    assert r2.returncode == 0, r2.stderr
    snapshot2 = {p.name: p.read_bytes() for p in target.iterdir()}

    assert snapshot1 == snapshot2, "running install_hooks.sh twice changed the installed tree"


def test_install_never_touches_a_file_it_does_not_manage(tmp_path: Path) -> None:
    """Deliberately mixes file TYPES (a project-scope .sh hook, a .py
    script, a stray .bak-* file, and a README.md) so an implementation that
    happens to skip deletion only for one extension -- e.g. "never delete
    *.sh" instead of "never touch anything outside the managed allowlist"
    -- cannot pass by accident. This is the actual on-disk shape of
    ~/.claude/hooks/ today (project-scope hooks, did_the_requester_get_it.py,
    merge_broker_reminder.sh.bak-5269, and a hand-written README.md), not a
    hypothetical.

    Disclosed gap: this checks existence and CONTENT, not file mode --
    `_mode` is written but never re-asserted. An install that left content
    untouched but re-chmod'd an unmanaged file would still pass here.
    """
    target = tmp_path / "hooks"
    target.mkdir(parents=True)

    unrelated_files = {
        # A project-scope hook this repo does not own (real basename
        # convention: block_wide_pytest.sh / invalidate_stop_decision.sh /
        # what_are_you_waiting_for.sh).
        "block_wide_pytest.sh": ("#!/bin/bash\necho project-scope-hook-untouched\n", 0o755),
        # A non-shell file type.
        "did_the_requester_get_it.py": ("print('untouched')\n", 0o644),
        # A backup/superseded file with a non-standard suffix.
        "merge_broker_reminder.sh.bak-5269": ("#!/bin/bash\necho stale-backup\n", 0o644),
        # A hand-written boundary doc install must not overwrite.
        "README.md": ("# hand-written boundary notes\n", 0o644),
    }
    for name, (body, mode) in unrelated_files.items():
        path = target / name
        path.write_text(body)
        path.chmod(mode)

    result = _run_install(target)
    assert result.returncode == 0, result.stderr

    for name, (body, _mode) in unrelated_files.items():
        path = target / name
        assert path.exists(), (
            f"install_hooks.sh touched (deleted or overwrote) {name}, which it does not "
            "manage -- allowlist-by-managed-file is required, not an extension-based skip "
            "or a 'wipe the target dir, then repopulate' shape"
        )
        assert path.read_text() == body, (
            f"{name} content changed even though install_hooks.sh does not manage it"
        )


def test_install_copies_every_managed_file_and_writes_a_version_stamp(tmp_path: Path) -> None:
    target = tmp_path / "hooks"

    result = _run_install(target)
    assert result.returncode == 0, result.stderr

    expected = {p.name for p in HOOKS_SRC.glob("*.sh")} | {"dispatch_table.json"}
    installed = {p.name for p in target.iterdir() if not p.name.startswith(".")}
    assert expected <= installed, expected - installed

    version_file = target / ".hooks_version"
    assert version_file.exists(), (
        "install_hooks.sh must write a version stamp for dispatcher.sh's drift warn"
    )
    assert version_file.read_text().strip()
