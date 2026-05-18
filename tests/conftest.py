"""Pytest fixtures for reyn-broker tests.

Each test gets a fresh broker subprocess bound to an ephemeral port so
tests are isolated from each other and from any locally running broker.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from collections.abc import Iterator
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


@pytest.fixture
def broker_url() -> Iterator[str]:
    """Spawn a fresh broker subprocess for a single test."""
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, str(_SERVER_PY), "--port", str(port), "--log-level", "WARNING"],
        cwd=str(_BROKER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port, _BOOT_TIMEOUT_S)
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
