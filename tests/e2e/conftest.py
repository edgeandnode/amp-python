"""E2E test fixtures: session-scoped ampd + Anvil infrastructure."""

import logging
import shutil
import tempfile
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


@pytest.fixture(scope='session')
def e2e_temp_dir():
    _skip_if_missing_deps()
    d = tempfile.mkdtemp(prefix='amp_e2e_')
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope='session')
def e2e_ports():
    return {
        'admin': get_free_port(),
        'flight': get_free_port(),
        'jsonl': get_free_port(),
    }


@pytest.fixture(scope='session')
def e2e_anvil(e2e_temp_dir):
    log_dir = e2e_temp_dir / 'logs'
    proc, url = spawn_anvil(log_dir)
    mine_blocks(url, 10)
    yield proc, url
    proc.terminate()


@pytest.fixture(scope='session')
def e2e_ampd_config(e2e_temp_dir, e2e_anvil, e2e_ports):
    _, anvil_url = e2e_anvil
    config_path = generate_ampd_config(
        e2e_temp_dir,
        e2e_ports['admin'],
        e2e_ports['flight'],
        e2e_ports['jsonl'],
    )
    provider_path = generate_provider_toml(e2e_temp_dir, anvil_url)
    manifest_path = copy_anvil_manifest(e2e_temp_dir)
    return config_path, provider_path, manifest_path


@pytest.fixture(scope='session')
def e2e_ampd(e2e_temp_dir, e2e_ampd_config, e2e_ports):
    config_path, _, _ = e2e_ampd_config
    log_dir = e2e_temp_dir / 'logs'
    proc = spawn_ampd(config_path, log_dir)
    wait_for_ampd_ready(e2e_ports['admin'])
    yield proc
    proc.terminate()


@pytest.fixture(scope='session')
def e2e_dataset(e2e_ampd, e2e_ampd_config, e2e_ports):
    _, provider_path, manifest_path = e2e_ampd_config
    admin_url = f'http://127.0.0.1:{e2e_ports["admin"]}'

    manager = DatasetManager(admin_url)
    try:
        manager.register_provider('anvil', provider_path)
        manager.register_dataset('_', 'anvil', manifest_path, '0.0.1')
        manager.deploy_dataset('_', 'anvil', '0.0.1')
        wait_for_data_ready(e2e_ports['flight'])
    finally:
        manager.close()


@pytest.fixture(scope='session')
def e2e_client(e2e_dataset, e2e_ports):
    from amp.client import Client

    return Client(query_url=f'grpc://127.0.0.1:{e2e_ports["flight"]}')
