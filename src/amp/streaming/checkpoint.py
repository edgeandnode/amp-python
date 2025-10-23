"""
Checkpointing system for durable streaming with resume capability.

Provides checkpoint management to enable resuming streaming queries from
the last successfully processed position, with support for reorg detection
and invalidation.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional

from .types import BlockRange, ResumeWatermark

logger = logging.getLogger(__name__)


@dataclass
class CheckpointConfig:
    """Configuration for checkpoint behavior."""

    enabled: bool = False  # Opt-in for backward compatibility
    storage: str = 'db'  # 'db', 'external', or 'both'
    table_prefix: str = 'amp_'


@dataclass
class CheckpointState:
    """
    State of a checkpoint, storing server-provided metadata for resume.

    This directly stores the block ranges and metadata from the server,
    allowing us to create a ResumeWatermark for resuming streams.
    """

    # Server metadata (from ranges field)
    ranges: List[BlockRange]

    # Client metadata
    timestamp: datetime
    worker_id: int = 0
    is_reorg: bool = False  # True if this checkpoint was created due to a reorg

    def to_resume_watermark(self) -> ResumeWatermark:
        """
        Convert checkpoint to ResumeWatermark for resuming streaming queries.

        Uses the block ranges from the server to create a watermark that
        tells the server where to resume from.
        """
        return ResumeWatermark(
            ranges=[r.to_dict() for r in self.ranges],
            timestamp=self.timestamp.isoformat() if self.timestamp else None,
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'ranges': [r.to_dict() for r in self.ranges],
            'timestamp': self.timestamp.isoformat(),
            'worker_id': self.worker_id,
            'is_reorg': self.is_reorg,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'CheckpointState':
        """Create from dictionary"""
        return cls(
            ranges=[BlockRange.from_dict(r) for r in data['ranges']],
            timestamp=datetime.fromisoformat(data['timestamp']),
            worker_id=data.get('worker_id', 0),
            is_reorg=data.get('is_reorg', False),
        )


class CheckpointStore(ABC):
    """
    Abstract interface for checkpoint storage.

    Implementations can store checkpoints in databases, external stores
    like Redis/S3, or both.
    """

    def __init__(self, config: CheckpointConfig):
        self.config = config

    @abstractmethod
    def save(
        self, connection_name: str, table_name: str, checkpoint: CheckpointState
    ) -> None:
        """Save a checkpoint for a specific connection and table"""
        pass

    @abstractmethod
    def load(
        self, connection_name: str, table_name: str, worker_id: int = 0
    ) -> Optional[CheckpointState]:
        """Load the latest checkpoint for a connection and table"""
        pass

    @abstractmethod
    def delete_for_network(self, connection_name: str, table_name: str, network: str) -> None:
        """Delete all checkpoints for a specific network (used after reorgs)"""
        pass

    @abstractmethod
    def delete(self, connection_name: str, table_name: str, worker_id: int = 0) -> None:
        """Delete a checkpoint"""
        pass


class DatabaseCheckpointStore(CheckpointStore):
    """
    Store checkpoints in the destination database.

    This implementation requires the destination database to support a
    checkpoints table (currently PostgreSQL, Snowflake, etc.).
    """

    def __init__(self, config: CheckpointConfig, db_connection):
        """
        Initialize database checkpoint store.

        Args:
            config: Checkpoint configuration
            db_connection: Database connection object with execute() method
        """
        super().__init__(config)
        self.conn = db_connection
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Ensure the checkpoints table exists in the database"""
        table_name = f'{self.config.table_prefix}checkpoints'

        # Try to create table (PostgreSQL syntax - will need adaptation for other DBs)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            connection_name VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            worker_id INT NOT NULL DEFAULT 0,
            checkpoint_data JSONB NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            metadata JSONB,
            PRIMARY KEY (connection_name, table_name, worker_id)
        )
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(create_sql)
            self.conn.commit()
            logger.debug(f'Ensured checkpoint table {table_name} exists')
        except Exception as e:
            # Table might already exist or DB might not support IF NOT EXISTS
            logger.debug(f'Checkpoint table creation skipped: {e}')
            self.conn.rollback()

    def save(
        self, connection_name: str, table_name: str, checkpoint: CheckpointState
    ) -> None:
        """Save checkpoint to database"""
        if not self.config.enabled:
            return

        table = f'{self.config.table_prefix}checkpoints'

        # Serialize entire checkpoint to JSON
        checkpoint_json = json.dumps(checkpoint.to_dict())

        # Upsert checkpoint
        upsert_sql = f"""
        INSERT INTO {table} (connection_name, table_name, worker_id, checkpoint_data, timestamp)
        VALUES (%s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (connection_name, table_name, worker_id)
        DO UPDATE SET
            checkpoint_data = EXCLUDED.checkpoint_data,
            timestamp = EXCLUDED.timestamp
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                upsert_sql,
                (
                    connection_name,
                    table_name,
                    checkpoint.worker_id,
                    checkpoint_json,
                    checkpoint.timestamp,
                ),
            )
            self.conn.commit()
            logger.info(
                f'Saved checkpoint for {connection_name}.{table_name} '
                f'(worker {checkpoint.worker_id}, {len(checkpoint.ranges)} ranges)'
            )
        except Exception as e:
            logger.error(f'Failed to save checkpoint: {e}')
            self.conn.rollback()
            raise

    def load(
        self, connection_name: str, table_name: str, worker_id: int = 0
    ) -> Optional[CheckpointState]:
        """Load checkpoint from database"""
        if not self.config.enabled:
            return None

        table = f'{self.config.table_prefix}checkpoints'

        select_sql = f"""
        SELECT checkpoint_data
        FROM {table}
        WHERE connection_name = %s AND table_name = %s AND worker_id = %s
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(select_sql, (connection_name, table_name, worker_id))
            row = cursor.fetchone()

            if not row:
                logger.debug(f'No checkpoint found for {connection_name}.{table_name}')
                return None

            checkpoint_json = row[0]

            # Parse checkpoint from JSON
            if isinstance(checkpoint_json, str):
                checkpoint_data = json.loads(checkpoint_json)
            else:
                # Already parsed (psycopg2 with RealDictCursor)
                checkpoint_data = checkpoint_json

            checkpoint = CheckpointState.from_dict(checkpoint_data)

            logger.info(
                f'Loaded checkpoint for {connection_name}.{table_name} '
                f'(worker {checkpoint.worker_id}, {len(checkpoint.ranges)} ranges)'
            )
            return checkpoint

        except Exception as e:
            logger.error(f'Failed to load checkpoint: {e}')
            return None

    def delete_for_network(self, connection_name: str, table_name: str, network: str) -> None:
        """
        Delete all checkpoints containing ranges for a specific network.

        This is called after a reorg to force full rescan of affected ranges.
        Checkpoints are JSON so we search within the stored data.
        """
        if not self.config.enabled:
            return

        table = f'{self.config.table_prefix}checkpoints'

        # Delete checkpoints where the network appears in the checkpoint_data JSON
        # This uses JSON containment checking (PostgreSQL jsonb @> operator)
        delete_sql = f"""
        DELETE FROM {table}
        WHERE connection_name = %s AND table_name = %s
        AND checkpoint_data::text LIKE %s
        """

        try:
            cursor = self.conn.cursor()
            # Search for "network": "<network>" in the JSON (note: JSONB adds space after colon)
            network_pattern = f'%"network": "{network}"%'
            cursor.execute(delete_sql, (connection_name, table_name, network_pattern))
            deleted_count = cursor.rowcount
            self.conn.commit()

            if deleted_count > 0:
                logger.warning(
                    f'Deleted {deleted_count} checkpoint(s) for {connection_name}.{table_name} '
                    f'after reorg on network {network}'
                )
        except Exception as e:
            logger.error(f'Failed to delete checkpoints for network {network}: {e}')
            self.conn.rollback()

    def delete(self, connection_name: str, table_name: str, worker_id: int = 0) -> None:
        """Delete a specific checkpoint"""
        if not self.config.enabled:
            return

        table = f'{self.config.table_prefix}checkpoints'

        delete_sql = f"""
        DELETE FROM {table}
        WHERE connection_name = %s AND table_name = %s AND worker_id = %s
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(delete_sql, (connection_name, table_name, worker_id))
            self.conn.commit()
            logger.info(f'Deleted checkpoint for {connection_name}.{table_name} (worker {worker_id})')
        except Exception as e:
            logger.error(f'Failed to delete checkpoint: {e}')
            self.conn.rollback()


class NullCheckpointStore(CheckpointStore):
    """No-op checkpoint store for when checkpointing is disabled"""

    def save(
        self, connection_name: str, table_name: str, checkpoint: CheckpointState
    ) -> None:
        pass

    def load(
        self, connection_name: str, table_name: str, worker_id: int = 0
    ) -> Optional[CheckpointState]:
        return None

    def delete_for_network(self, connection_name: str, table_name: str, network: str) -> None:
        pass

    def delete(self, connection_name: str, table_name: str, worker_id: int = 0) -> None:
        pass
