"""
Shared types for loader operations.

This module contains types that are used across multiple modules to avoid circular imports.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from ..streaming.types import BlockRange


class LoadMode(Enum):
    APPEND = 'append'
    OVERWRITE = 'overwrite'
    UPSERT = 'upsert'
    MERGE = 'merge'


@dataclass
class LoadResult:
    """Result of a data loading operation"""

    rows_loaded: int
    duration: float
    ops_per_second: float
    table_name: str
    loader_type: str
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Streaming/reorg specific fields
    is_reorg: bool = False
    invalidation_ranges: Optional[List[BlockRange]] = None

    def __str__(self) -> str:
        if self.is_reorg:
            return f'🔄 Reorg detected: {len(self.invalidation_ranges or [])} ranges invalidated'
        elif self.success:
            return f'✅ Loaded {self.rows_loaded} rows to {self.table_name} in {self.duration:.2f}s'
        else:
            return f'❌ Failed to load to {self.table_name}: {self.error}'


@dataclass
class LabelJoinConfig:
    """Configuration for label joining operations"""

    label_name: str
    label_key_column: str
    stream_key_column: str


@dataclass
class LoadConfig:
    """Configuration for data loading operations"""

    batch_size: int = 10000
    mode: LoadMode = LoadMode.APPEND
    create_table: bool = True
    schema_evolution: bool = False
    max_retries: int = 3
    retry_delay: float = 1.0
