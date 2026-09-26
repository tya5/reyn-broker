"""Tier 2: dispatcher.sh's two load-bearing mechanics -- stdin fan-out and
exit-2 aggregation -- actually do what dispatcher.sh's own comments claim.

test_dispatch_reachable.py covers the dispatch TABLE's structure and
test_dispatcher_scope_guard.py covers the scope guard, but neither exercises
whether dispatcher.sh correctly RUNS the hooks it dispatches to. A break in
either mechanic is silent by construction:

- stdin fan-out broken -> every dispatched hook sees empty stdin -> every
  hook's own `input=$(cat)` early-exit reads that as "not applicable" and
  exits 0 quietly. All 10 gates go dark with no error anywhere -- the same
  shape as the incident #34 is about (a gate that looks present but is not
  effective).
- exit-2 aggregation broken -> a dispatched script's block (exit 2 + stderr)
  gets swallowed and dispatcher.sh exits 0 -- 6 real block hooks silently
  downgrade to advice.

Both fixtures live under tests/fixtures/dispatcher_probe/, not hooks/, so
they never appear in hooks/dispatch_table.json and never collide with
test_hooks_dispatch_reachable.py's reachability gate. Each test points
dispatcher.sh at an isolated tmp_path hooks_dir (via DISPATCHER_HOOKS_DIR)
holding only the fixture scripts + a table built for that one test, so
production hooks/dispatch_table.json is never touched or depended on here.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DISPATCHER = REPO_ROOT / "hooks" / "dispatcher.sh"
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "dispatcher_probe"


def _make_hooks_dir(tmp_path: Path, table: dict, *fixture_names: str) -> Path:
    """Build an isolated hooks_dir: copies the named fixture scripts in and
    writes a dispatch_table.json built just for this test."""
    hooks_dir = tmp_path / "hooks_dir"
    hooks_dir.mkdir()
    for name in fixture_names:
        dest = hooks_dir / name
        shutil.copyfile(FIXTURES_DIR / name, dest)
        dest.chmod(0o755)
    (hooks_dir / "dispatch_table.json").write_text(json.dumps(table))
    return hooks_dir


def _run_dispatcher(
    hooks_dir: Path, *, project_dir: Path, extra_env: dict
) -> subprocess.CompletedProcess:
    env = {
        "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
        # no ~/.claude/settings.json here -> foreign-registration warn is a no-op
        "HOME": str(project_dir),
        "CLAUDE_PROJECT_DIR": str(project_dir),
        "REYN_DEV_ROOT": str(project_dir),  # project_dir IS in-scope
        "TMPDIR": str(project_dir),
        "DISPATCHER_HOOKS_DIR": str(hooks_dir),
    }
    env.update(extra_env)
    return subprocess.run(
        [str(DISPATCHER), "PreToolUse"],
        input=json.dumps({"session_id": "dispatch-probe", "tool_name": "Bash"}),
        capture_output=True,
        text=True,
        cwd=str(project_dir),
        env=env,
    )


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    d = tmp_path / "in_scope_project"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# stdin fan-out
# ---------------------------------------------------------------------------


def test_stdin_fanout_is_byte_identical_to_every_dispatched_script(
    tmp_path: Path, project_dir: Path
) -> None:
    table = {
        "PreToolUse": [
            {"tool_pattern": None, "scripts": ["echo_stdin_a.sh", "echo_stdin_b.sh"]},
        ]
    }
    hooks_dir = _make_hooks_dir(tmp_path, table, "echo_stdin_a.sh", "echo_stdin_b.sh")
    out_a = tmp_path / "out_a.txt"
    out_b = tmp_path / "out_b.txt"

    payload = json.dumps(
        {"session_id": "dispatch-probe", "tool_name": "Bash", "marker": "FANOUT-BYTES-42"}
    )
    result = subprocess.run(
        [str(DISPATCHER), "PreToolUse"],
        input=payload,
        capture_output=True,
        text=True,
        cwd=str(project_dir),
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": str(project_dir),
            "CLAUDE_PROJECT_DIR": str(project_dir),
            "REYN_DEV_ROOT": str(project_dir),
            "TMPDIR": str(project_dir),
            "DISPATCHER_HOOKS_DIR": str(hooks_dir),
            "OUT_FILE_A": str(out_a),
            "OUT_FILE_B": str(out_b),
        },
    )

    assert result.returncode == 0, result.stderr
    assert out_a.exists(), (
        "first dispatched script never ran -- dispatch table lookup itself is broken"
    )
    assert out_b.exists(), (
        "second dispatched script never received stdin -- fan-out only reached the FIRST "
        "script (the failure a single-script fixture cannot catch)"
    )

    # Bytes received by EACH script must equal what was sent, and therefore
    # each other -- not merely "some non-empty thing that happens to match".
    assert out_a.read_text() == payload
    assert out_b.read_text() == payload


# ---------------------------------------------------------------------------
# exit-2 aggregation
# ---------------------------------------------------------------------------


def test_a_blocking_script_makes_dispatcher_exit_2_with_its_stderr(
    tmp_path: Path, project_dir: Path
) -> None:
    table = {
        "PreToolUse": [
            {"tool_pattern": None, "scripts": ["always_pass.sh", "always_block.sh"]},
        ]
    }
    hooks_dir = _make_hooks_dir(tmp_path, table, "always_pass.sh", "always_block.sh")

    result = _run_dispatcher(hooks_dir, project_dir=project_dir, extra_env={})

    assert result.returncode == 2, (
        f"a dispatched script exited 2 but dispatcher.sh returned {result.returncode} -- "
        "the block was swallowed, downgrading a gate to advice"
    )
    assert "PROBE-BLOCK-MARKER" in result.stderr, result.stderr


def test_all_passing_scripts_leave_dispatcher_exit_0(tmp_path: Path, project_dir: Path) -> None:
    """Counter-test to the one above: an aggregation implementation that
    always returns 2 (regardless of what it dispatched to) would pass the
    blocking test above but must fail this one."""
    table = {
        "PreToolUse": [
            {"tool_pattern": None, "scripts": ["always_pass.sh", "always_pass.sh"]},
        ]
    }
    hooks_dir = _make_hooks_dir(tmp_path, table, "always_pass.sh")

    result = _run_dispatcher(hooks_dir, project_dir=project_dir, extra_env={})

    assert result.returncode == 0, result.stderr
