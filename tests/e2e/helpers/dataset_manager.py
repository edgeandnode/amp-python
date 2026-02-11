"""Dataset registration and deployment for E2E tests via Admin API."""

import json
import logging
import tomllib
from pathlib import Path

from amp.admin import AdminClient

logger = logging.getLogger(__name__)


class DatasetManager:
    """Manages provider registration, dataset registration, and deployment."""

    def __init__(self, admin_url: str):
        self._admin = AdminClient(base_url=admin_url)

    def register_provider(self, name: str, provider_source_path: Path) -> None:
        with open(provider_source_path, 'rb') as f:
            config = tomllib.load(f)
        config['name'] = name
        self._admin._request('POST', '/providers', json=config)
        logger.info(f'Registered provider {name}')

    def register_dataset(self, namespace: str, name: str, manifest_path: Path, tag: str) -> None:
        with open(manifest_path) as f:
            manifest = json.load(f)
        self._admin.datasets.register(namespace, name, tag, manifest)
        logger.info(f'Registered dataset {namespace}/{name}@{tag}')

    def deploy_dataset(self, namespace: str, name: str, version: str, end_block: str = 'latest') -> None:
        self._admin.datasets.deploy(namespace, name, version, end_block=end_block)
        logger.info(f'Deployed {namespace}/{name}@{version}')

    def close(self) -> None:
        self._admin.close()
