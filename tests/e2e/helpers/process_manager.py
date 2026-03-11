"""Subprocess utilities for managing Anvil and ampd processes in E2E tests."""

import logging
import os
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ManagedProcess:
    """Wrapper around subprocess.Popen with cleanup."""

    process: subprocess.Popen
    name: str
    _log_files: list = field(default_factory=list, repr=False)

    def terminate(self, timeout: int = 5) -> None:
        """Terminate the process with a kill fallback."""
        if self.process.poll() is not None:
            self._close_logs()
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning(f'{self.name} did not terminate in {timeout}s, killing')
            self.process.kill()
            self.process.wait(timeout=5)
        self._close_logs()

    def _close_logs(self) -> None:
        for f in self._log_files:
            f.close()
        self._log_files.clear()

    def is_alive(self) -> bool:
        return self.process.poll() is None


def get_free_port() -> int:
    """Bind to port 0 and return the assigned port."""
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def spawn_anvil(log_dir: Path) -> tuple[ManagedProcess, str]:
    """Spawn an Anvil process on a random free port.

    Returns the managed process and the HTTP URL.
    """
    port = get_free_port()
    log_dir.mkdir(parents=True, exist_ok=True)

    stdout_f = open(log_dir / 'anvil_stdout.log', 'w')
    stderr_f = open(log_dir / 'anvil_stderr.log', 'w')

    process = subprocess.Popen(
        ['anvil', '--port', str(port)],
        stdout=stdout_f,
        stderr=stderr_f,
    )

    url = f'http://127.0.0.1:{port}'
    _wait_for_jsonrpc(url, timeout=30)

    return (
        ManagedProcess(process=process, name='anvil', _log_files=[stdout_f, stderr_f]),
        url,
    )


def _wait_for_jsonrpc(url: str, timeout: int = 30) -> None:
    """Poll a JSON-RPC endpoint until it responds."""
    start = time.monotonic()
    with httpx.Client() as client:
        while time.monotonic() - start < timeout:
            try:
                resp = client.post(
                    url,
                    json={'jsonrpc': '2.0', 'method': 'eth_blockNumber', 'params': [], 'id': 1},
                )
                if resp.status_code == 200:
                    return
            except httpx.ConnectError:
                pass
            time.sleep(0.2)
    raise TimeoutError(f'JSON-RPC at {url} not ready after {timeout}s')


def mine_blocks(anvil_url: str, count: int) -> None:
    """Mine blocks on an Anvil instance via JSON-RPC evm_mine."""
    with httpx.Client() as client:
        for _ in range(count):
            resp = client.post(
                anvil_url,
                json={'jsonrpc': '2.0', 'method': 'evm_mine', 'params': [], 'id': 1},
            )
            resp.raise_for_status()


def spawn_ampd(config_path: Path, log_dir: Path) -> ManagedProcess:
    """Spawn ampd dev with the given config file."""
    log_dir.mkdir(parents=True, exist_ok=True)

    stdout_f = open(log_dir / 'ampd_stdout.log', 'w')
    stderr_f = open(log_dir / 'ampd_stderr.log', 'w')

    amp_dir = config_path.parent / '.amp'
    process = subprocess.Popen(
        ['ampd', 'dev'],
        env={**os.environ, 'AMP_CONFIG': str(config_path), 'AMP_DIR': str(amp_dir)},
        stdout=stdout_f,
        stderr=stderr_f,
    )

    return ManagedProcess(process=process, name='ampd', _log_files=[stdout_f, stderr_f])


def wait_for_ampd_ready(admin_port: int, timeout: int = 60) -> None:
    """Poll the Admin API until ampd is ready."""
    url = f'http://127.0.0.1:{admin_port}/datasets'
    start = time.monotonic()
    with httpx.Client() as client:
        while time.monotonic() - start < timeout:
            try:
                resp = client.get(url)
                if resp.status_code == 200:
                    logger.info(f'ampd ready after {time.monotonic() - start:.1f}s')
                    return
            except httpx.ConnectError:
                pass
            time.sleep(0.5)
    raise TimeoutError(f'ampd admin API not ready after {timeout}s')


def wait_for_data_ready(flight_port: int, timeout: int = 60) -> None:
    """Poll Flight SQL until data is queryable."""
    from amp.client import Client

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            client = Client(query_url=f'grpc://127.0.0.1:{flight_port}')
            table = client.sql('SELECT block_num FROM anvil.blocks LIMIT 1').to_arrow()
            if len(table) > 0:
                logger.info(f'Data ready after {time.monotonic() - start:.1f}s')
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f'Flight SQL data not queryable after {timeout}s')
