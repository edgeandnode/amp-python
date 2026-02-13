"""Data models for Amp DBT."""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ModelConfig:
    """Configuration for a single model."""

    dependencies: Dict[str, str] = field(default_factory=dict)
    track_progress: bool = False
    track_column: Optional[str] = None
    register: bool = False
    deploy: bool = False
    description: Optional[str] = None
    incremental_strategy: Optional[str] = None


@dataclass
class CompiledModel:
    """Result of compiling a model."""

    name: str
    sql: str
    config: ModelConfig
    dependencies: Dict[str, str]  # Maps ref name to dataset reference
    raw_sql: str  # Original SQL before compilation

