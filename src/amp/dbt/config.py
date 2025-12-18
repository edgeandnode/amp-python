"""Configuration parsing for Amp DBT."""

import re
from pathlib import Path
from typing import Dict, Optional

import yaml

from amp.dbt.exceptions import ConfigError
from amp.dbt.models import ModelConfig


def parse_config_block(sql: str) -> tuple[str, ModelConfig]:
    """Parse config block from model SQL.

    Args:
        sql: Model SQL with optional config block

    Returns:
        Tuple of (sql_without_config, ModelConfig)

    Raises:
        ConfigError: If config block is invalid
    """
    # Pattern to match {{ config(...) }} block
    # Need to handle nested braces in the config dict
    config_pattern = r'\{\{\s*config\s*\((.*?)\)\s*\}\}'

    match = re.search(config_pattern, sql, re.DOTALL)
    if not match:
        # No config block, return default config
        return sql, ModelConfig()

    config_str = match.group(1)
    sql_without_config = sql[: match.start()] + sql[match.end() :]

    # Parse config as Python dict-like syntax
    # Simple parser for key=value pairs
    config = ModelConfig()

    # Extract dependencies
    deps_match = re.search(r"dependencies\s*=\s*\{([^}]+)\}", config_str, re.DOTALL)
    if deps_match:
        deps_str = deps_match.group(1)
        dependencies = {}
        # Parse key: value pairs
        for dep_match in re.finditer(r"['\"]?(\w+)['\"]?\s*:\s*['\"]?([^'\"]+)['\"]?", deps_str):
            key = dep_match.group(1)
            value = dep_match.group(2)
            dependencies[key] = value
        config.dependencies = dependencies

    # Extract boolean flags
    for flag in ['track_progress', 'register', 'deploy']:
        if re.search(rf'{flag}\s*=\s*True', config_str, re.IGNORECASE):
            setattr(config, flag, True)

    # Extract string values
    track_col_match = re.search(r"track_column\s*=\s*['\"](\w+)['\"]", config_str)
    if track_col_match:
        config.track_column = track_col_match.group(1)

    desc_match = re.search(r"description\s*=\s*['\"]([^'\"]+)['\"]", config_str)
    if desc_match:
        config.description = desc_match.group(1)

    incr_match = re.search(r"incremental_strategy\s*=\s*['\"](\w+)['\"]", config_str)
    if incr_match:
        config.incremental_strategy = incr_match.group(1)

    return sql_without_config, config


def load_project_config(project_root: Path) -> Dict:
    """Load dbt_project.yml configuration.

    Args:
        project_root: Root directory of the DBT project

    Returns:
        Dictionary with project configuration

    Raises:
        ConfigError: If config file is invalid or missing
    """
    config_path = project_root / 'dbt_project.yml'
    if not config_path.exists():
        return {}

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config or {}
    except yaml.YAMLError as e:
        raise ConfigError(f'Invalid YAML in dbt_project.yml: {e}') from e
    except Exception as e:
        raise ConfigError(f'Failed to load dbt_project.yml: {e}') from e


def load_profiles(profiles_path: Optional[Path] = None) -> Dict:
    """Load profiles.yml configuration.

    Args:
        profiles_path: Optional path to profiles.yml (default: ~/.amp-dbt/profiles.yml)

    Returns:
        Dictionary with profiles configuration

    Raises:
        ConfigError: If profiles file is invalid
    """
    if profiles_path is None:
        profiles_path = Path.home() / '.amp-dbt' / 'profiles.yml'

    if not profiles_path.exists():
        return {}

    try:
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
            return profiles or {}
    except yaml.YAMLError as e:
        raise ConfigError(f'Invalid YAML in profiles.yml: {e}') from e
    except Exception as e:
        raise ConfigError(f'Failed to load profiles.yml: {e}') from e

