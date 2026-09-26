"""Tier 2: dispatcher.sh's reyn_dev scope guard actually differentiates in/out of scope.

Chosen witness: llm_speculation_block.sh (one of the real dispatched hooks)
blocks a Write containing a forbidden phrase. The SAME payload, through the
SAME dispatcher, must block when CLAUDE_PROJECT_DIR is inside
$REYN_DEV_ROOT and must NOT block outside it -- a rc=0 by itself is not
sufficient evidence the guard fired (empty/irrelevant stdin also yields
rc=0 on both sides of the guard), so every assertion here is a DIFFERENCE
between an in-scope and an out-of-scope run of the identical input, never a
bare rc=0 read alone.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent.parent / "hooks"
DISPATCHER = HOOKS_DIR / "dispatcher.sh"

BLOCKING_PAYLOAD = {
    "session_id": "test-scope-guard",
    "tool_name": "Write",
    "tool_input": {
        "content": "the LLM thrashing happened here",
        "file_path": "/tmp/notes.txt",
    },
}


def _run_dispatcher(*, project_dir: str, reyn_dev_root: str, cwd: str, session_id: str):
    payload = dict(BLOCKING_PAYLOAD)
    payload["session_id"] = session_id
    return subprocess.run(
        [str(DISPATCHER), "PreToolUse"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=cwd,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": cwd,
            "CLAUDE_PROJECT_DIR": project_dir,
            "REYN_DEV_ROOT": reyn_dev_root,
            "TMPDIR": cwd,
        },
    )


@pytest.fixture
def scope_dirs(tmp_path: Path):
    root = tmp_path / "reyn_dev"
    inside = root / "some_session_dir"
    sibling = tmp_path / "reyn_dev_other"  # shares the bare prefix "reyn_dev"
    outside = tmp_path / "unrelated"
    for d in (inside, sibling, outside):
        d.mkdir(parents=True)
    return {"root": root, "inside": inside, "sibling": sibling, "outside": outside}


def test_fires_inside_reyn_dev_scope(scope_dirs) -> None:
    result = _run_dispatcher(
        project_dir=str(scope_dirs["inside"]),
        reyn_dev_root=str(scope_dirs["root"]),
        cwd=str(scope_dirs["inside"]),
        session_id="in-scope",
    )
    assert result.returncode == 2, result.stderr
    assert "llm_speculation_block" in result.stderr


def test_does_not_fire_outside_reyn_dev_scope(scope_dirs) -> None:
    result = _run_dispatcher(
        project_dir=str(scope_dirs["outside"]),
        reyn_dev_root=str(scope_dirs["root"]),
        cwd=str(scope_dirs["outside"]),
        session_id="outside-scope",
    )
    assert result.returncode == 0, result.stderr
    assert "llm_speculation_block" not in result.stderr


def test_prefix_collision_sibling_is_treated_as_outside(scope_dirs) -> None:
    """reyn_dev_other shares dispatcher's bare string prefix "reyn_dev" but
    is a distinct sibling directory -- it must be rejected, not swept in by
    a naive glob match."""
    result = _run_dispatcher(
        project_dir=str(scope_dirs["sibling"]),
        reyn_dev_root=str(scope_dirs["root"]),
        cwd=str(scope_dirs["sibling"]),
        session_id="sibling-scope",
    )
    assert result.returncode == 0, result.stderr
    assert "llm_speculation_block" not in result.stderr


def test_claude_project_dir_overrides_cwd(scope_dirs) -> None:
    """cwd is outside scope; CLAUDE_PROJECT_DIR (inside scope) must win --
    this is the same precedence _reyn_dev_scope_guard.sh's
    ${CLAUDE_PROJECT_DIR:-$PWD} already encoded, now inside dispatcher.sh."""
    result = _run_dispatcher(
        project_dir=str(scope_dirs["inside"]),
        reyn_dev_root=str(scope_dirs["root"]),
        cwd=str(scope_dirs["outside"]),
        session_id="cwd-override",
    )
    assert result.returncode == 2, result.stderr
    assert "llm_speculation_block" in result.stderr
