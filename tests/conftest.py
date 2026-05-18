"""Pytest fixtures for reyn-broker tests.

Each test gets a fresh broker subprocess bound to an ephemeral port and
a per-test state file so tests are isolated from each other and from
any locally running broker.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

_BROKER_DIR = Path(__file__).resolve().parents[1]
_SERVER_PY = _BROKER_DIR / "server.py"
_BOOT_TIMEOUT_S = 10.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"broker did not start listening on port {port} within {timeout}s")


def _spawn_broker(port: int, state_file: Path) -> subprocess.Popen:
    env = {**os.environ, "BROKER_STATE_FILE": str(state_file)}
    proc = subprocess.Popen(
        [sys.executable, str(_SERVER_PY), "--port", str(port), "--log-level", "WARNING"],
        cwd=str(_BROKER_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_port(port, _BOOT_TIMEOUT_S)
    return proc


def _stop_broker(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture
def broker_url(tmp_path: Path) -> Iterator[str]:
    """Spawn a fresh broker subprocess with an isolated state file."""
    port = _free_port()
    state_file = tmp_path / "state.json"
    proc = _spawn_broker(port, state_file)
    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        _stop_broker(proc)


@pytest.fixture
def broker_restart(tmp_path: Path) -> Iterator[Callable[[], str]]:
    """Yield a callable that starts a broker (kills the previous one) on the
    same port and state file, so tests can simulate server restarts.

    Usage:
        def test_x(broker_restart):
            url = broker_restart()      # boot 1
            ... do stuff ...
            url = broker_restart()      # boot 2 — state.json preserved
    """
    port = _free_port()
    state_file = tmp_path / "state.json"
    current: dict[str, subprocess.Popen] = {}

    def restart() -> str:
        if "proc" in current:
            _stop_broker(current["proc"])
        current["proc"] = _spawn_broker(port, state_file)
        return f"http://127.0.0.1:{port}/mcp"

    try:
        yield restart
    finally:
        if "proc" in current:
            _stop_broker(current["proc"])
