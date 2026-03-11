"""Config file generation for ampd E2E tests."""

import shutil
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent.parent / 'fixtures'


def generate_ampd_config(
    config_dir: Path,
    admin_port: int,
    flight_port: int,
    jsonl_port: int,
) -> Path:
    """Generate ampd config.toml and required subdirectories.

    Returns the path to config.toml.
    """
    for subdir in ('data', 'manifests', 'providers', 'provider_sources'):
        (config_dir / subdir).mkdir(parents=True, exist_ok=True)

    template = (FIXTURES_DIR / 'config.toml.template').read_text()
    config = template.format(
        manifests_dir=config_dir / 'manifests',
        providers_dir=config_dir / 'providers',
        data_dir=config_dir / 'data',
        admin_port=admin_port,
        flight_port=flight_port,
        jsonl_port=jsonl_port,
    )

    config_path = config_dir / 'config.toml'
    config_path.write_text(config)
    return config_path


def generate_provider_toml(config_dir: Path, anvil_url: str) -> Path:
    """Write provider source TOML for Anvil.

    Returns the path to the provider source file.
    """
    provider_sources_dir = config_dir / 'provider_sources'
    provider_sources_dir.mkdir(parents=True, exist_ok=True)

    template = (FIXTURES_DIR / 'provider.toml.template').read_text()
    provider_toml = template.format(anvil_url=anvil_url)

    path = provider_sources_dir / 'anvil.toml'
    path.write_text(provider_toml)
    return path


def copy_anvil_manifest(config_dir: Path) -> Path:
    """Copy anvil.json fixture into the manifests directory.

    Returns the path to the copied manifest.
    """
    dest = config_dir / 'manifests' / 'anvil.json'
    shutil.copy(FIXTURES_DIR / 'anvil.json', dest)
    return dest
