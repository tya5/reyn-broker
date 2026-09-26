"""Tier 2: dispatcher.sh's drift warn -- compares the installed copy's
version stamp against the repo it claims to be installed from, WARNS on a
mismatch, and never changes the exit code either way.

Background: issue #34's follow-up (architect ruling). The installed copy
under DISPATCHER_HOOKS_DIR is what always executes; the repo is canon but
never runs directly. This is the ONLY place that can notice the two have
drifted apart (a human forgot to re-run scripts/install_hooks.sh after
changing the repo, e.g. via `git checkout`) -- so it has to warn, but must
never block: the repo may legitimately be mid-branch, and blocking here
would stop every dispatched hook over a fact about a checkout, not about
the copy that is actually running.

Three assertions, each the counter-test to a wrong-shape implementation:
  - matching versions -> no warn (counters "always warns regardless").
  - mismatched versions -> warn AND rc unchanged (counters both "never
    warns" and "warns by blocking").
  - repo absent -> silent (counters "errors out when there's nothing to
    compare against").

Runs entirely in tmp_path via DISPATCHER_HOOKS_DIR / REYN_BROKER_REPO_DIR --
never touches the real ~/.claude/hooks/ or the real broker checkout.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DISPATCHER = REPO_ROOT / "hooks" / "dispatcher.sh"
HOOKS_VERSION_SCRIPT = REPO_ROOT / "scripts" / "hooks_version.sh"

PROBE_HOOK_BODY = "#!/bin/bash\ncat >/dev/null\nexit 0\n"


def _build_fake_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "fake_repo"
    (repo / "hooks").mkdir(parents=True)
    (repo / "scripts").mkdir(parents=True)
    (repo / "hooks" / "dispatch_table.json").write_text(json.dumps({"PreToolUse": []}))
    probe = repo / "hooks" / "probe_hook.sh"
    probe.write_text(PROBE_HOOK_BODY)
    probe.chmod(0o755)
    shutil.copyfile(HOOKS_VERSION_SCRIPT, repo / "scripts" / "hooks_version.sh")
    (repo / "scripts" / "hooks_version.sh").chmod(0o755)
    return repo


def _installed_copy_matching(tmp_path: Path, repo: Path) -> Path:
    installed = tmp_path / "installed_hooks"
    installed.mkdir()
    shutil.copyfile(repo / "hooks" / "dispatch_table.json", installed / "dispatch_table.json")
    shutil.copyfile(repo / "hooks" / "probe_hook.sh", installed / "probe_hook.sh")
    version = subprocess.run(
        [str(repo / "scripts" / "hooks_version.sh"), str(repo / "hooks")],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    (installed / ".hooks_version").write_text(version)
    return installed


def _run_dispatcher(*, installed_dir: Path, repo_dir: Path, project_dir: Path):
    return subprocess.run(
        [str(DISPATCHER), "PreToolUse"],
        input=json.dumps({"session_id": "drift-probe", "tool_name": "Bash"}),
        capture_output=True,
        text=True,
        cwd=str(project_dir),
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": str(project_dir),
            "CLAUDE_PROJECT_DIR": str(project_dir),
            "REYN_DEV_ROOT": str(project_dir),
            "TMPDIR": str(project_dir),
            "DISPATCHER_HOOKS_DIR": str(installed_dir),
            "REYN_BROKER_REPO_DIR": str(repo_dir),
        },
    )


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    d = tmp_path / "project"
    d.mkdir()
    return d


def test_matching_version_produces_no_drift_warn(tmp_path: Path, project_dir: Path) -> None:
    repo = _build_fake_repo(tmp_path)
    installed = _installed_copy_matching(tmp_path, repo)

    result = _run_dispatcher(installed_dir=installed, repo_dir=repo, project_dir=project_dir)

    assert result.returncode == 0, result.stderr
    assert "out of date" not in result.stderr, result.stderr


def test_mismatched_version_warns_but_never_blocks(tmp_path: Path, project_dir: Path) -> None:
    repo = _build_fake_repo(tmp_path)
    installed = _installed_copy_matching(tmp_path, repo)

    # Change the repo AFTER the installed copy's stamp was taken -- exactly
    # the shape of "install_hooks.sh has not been re-run since the repo
    # changed" (e.g. a `git checkout`).
    (repo / "hooks" / "probe_hook.sh").write_text(PROBE_HOOK_BODY + "# changed\n")

    result = _run_dispatcher(installed_dir=installed, repo_dir=repo, project_dir=project_dir)

    assert result.returncode == 0, (
        f"drift warn must never block -- got rc={result.returncode}: {result.stderr}"
    )
    assert "out of date" in result.stderr, result.stderr


def test_repo_absent_is_silent(tmp_path: Path, project_dir: Path) -> None:
    repo = _build_fake_repo(tmp_path)
    installed = _installed_copy_matching(tmp_path, repo)
    missing_repo = tmp_path / "no_such_repo_here"

    result = _run_dispatcher(
        installed_dir=installed, repo_dir=missing_repo, project_dir=project_dir
    )

    assert result.returncode == 0, result.stderr
    assert "out of date" not in result.stderr, result.stderr
