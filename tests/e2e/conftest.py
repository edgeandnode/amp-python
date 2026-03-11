"""E2E test fixtures: session-scoped and function-scoped ampd + Anvil infrastructure."""

import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from .helpers.config import copy_anvil_manifest, generate_ampd_config, generate_provider_toml
from .helpers.dataset_manager import DatasetManager
from .helpers.process_manager import (
    get_free_port,
    mine_blocks,
    spawn_ampd,
    spawn_anvil,
    wait_for_ampd_ready,
    wait_for_data_ready,
)

logger = logging.getLogger(__name__)


def _skip_if_missing_deps():
    missing = [b for b in ('anvil', 'ampd', 'initdb', 'postgres') if not shutil.which(b)]
    if missing:
        pytest.skip(f'Missing binaries: {", ".join(missing)}')


@dataclass
class AmpTestServer:
    """An ampd + Anvil stack with a connected client."""

    client: object
    anvil_url: str
    admin_url: str
    ports: dict


def _setup_amp_stack(num_blocks: int = 10):
    """Spin up anvil + ampd + register + deploy.

    Returns (AmpTestServer, cleanup_fn).
    """
    from amp.client import Client

    temp_dir = Path(tempfile.mkdtemp(prefix='amp_e2e_'))
    try:
        log_dir = temp_dir / 'logs'
        ports = {
            'admin': get_free_port(),
            'flight': get_free_port(),
            'jsonl': get_free_port(),
        }

        anvil_proc, anvil_url = spawn_anvil(log_dir)
        mine_blocks(anvil_url, num_blocks)

        config_path = generate_ampd_config(
            temp_dir,
            ports['admin'],
            ports['flight'],
            ports['jsonl'],
        )
        generate_provider_toml(temp_dir, anvil_url)
        manifest_path = copy_anvil_manifest(temp_dir)

        ampd_proc = spawn_ampd(config_path, log_dir)
        wait_for_ampd_ready(ports['admin'])

        admin_url = f'http://127.0.0.1:{ports["admin"]}'
        manager = DatasetManager(admin_url)
        try:
            manager.register_provider('anvil', temp_dir / 'provider_sources' / 'anvil.toml')
            manager.register_dataset('_', 'anvil', manifest_path, '0.0.1')
            manager.deploy_dataset('_', 'anvil', '0.0.1')
            wait_for_data_ready(ports['flight'])
        finally:
            manager.close()
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    server = AmpTestServer(
        client=Client(query_url=f'grpc://127.0.0.1:{ports["flight"]}'),
        anvil_url=anvil_url,
        admin_url=admin_url,
        ports=ports,
    )

    def cleanup():
        ampd_proc.terminate()
        anvil_proc.terminate()
        shutil.rmtree(temp_dir, ignore_errors=True)

    return server, cleanup


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope='session')
def e2e_server():
    _skip_if_missing_deps()
    server, cleanup = _setup_amp_stack()
    yield server
    cleanup()


@pytest.fixture(scope='session')
def e2e_client(e2e_server):
    return e2e_server.client


# ---------------------------------------------------------------------------
# Function-scoped fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def amp_test_server():
    """Isolated ampd + Anvil stack for a single test."""
    _skip_if_missing_deps()
    server, cleanup = _setup_amp_stack()
    yield server
    cleanup()
