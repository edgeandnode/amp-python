"""
Idempotency system for exactly-once processing semantics.

Tracks processed block ranges to prevent duplicate processing of the same data,
providing exactly-once guarantees for financial and mission-critical applications.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import List, Optional

from .types import BlockRange

logger = logging.getLogger(__name__)


@dataclass
class IdempotencyConfig:
    """Configuration for idempotency behavior.

    Mode is auto-detected:
    - Transactional: If loader has load_batch_transactional() method
    - Tracking: Otherwise (uses separate check/load/mark flow)
    """

    enabled: bool = False  # Opt-in for backward compatibility
    table_prefix: str = 'amp_'
    verification_hash: bool = False  # Compute and verify batch content hashes
    cleanup_days: int = 30  # Auto-cleanup processed ranges older than this


@dataclass
class ProcessedRange:
    """
    Record of a processed block range.

    Stores information about which block ranges have been successfully processed
    to enable duplicate detection and exactly-once semantics.
    """

    connection_name: str
    table_name: str
    network: str
    start_block: int
    end_block: int
    processed_at: datetime
    batch_hash: Optional[str] = None  # Optional: content verification hash

    def matches_range(self, block_range: BlockRange) -> bool:
        """Check if this processed range matches a block range"""
        return (
            self.network == block_range.network
            and self.start_block == block_range.start
            and self.end_block == block_range.end
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            'connection_name': self.connection_name,
            'table_name': self.table_name,
            'network': self.network,
            'start_block': self.start_block,
            'end_block': self.end_block,
            'processed_at': self.processed_at.isoformat(),
            'batch_hash': self.batch_hash,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ProcessedRange':
        """Create from dictionary"""
        return cls(
            connection_name=data['connection_name'],
            table_name=data['table_name'],
            network=data['network'],
            start_block=data['start_block'],
            end_block=data['end_block'],
            processed_at=datetime.fromisoformat(data['processed_at']),
            batch_hash=data.get('batch_hash'),
        )


class ProcessedRangesStore(ABC):
    """
    Abstract interface for tracking processed block ranges.

    Implementations can use database tables, external stores, or other mechanisms
    to track which ranges have been successfully processed.
    """

    def __init__(self, config: IdempotencyConfig):
        self.config = config

    @abstractmethod
    def is_processed(self, connection_name: str, table_name: str, ranges: List[BlockRange]) -> bool:
        """
        Check if a set of block ranges has already been processed.

        Args:
            connection_name: Name of the connection
            table_name: Name of the destination table
            ranges: List of block ranges to check

        Returns:
            True if all ranges have been processed, False otherwise
        """
        pass

    @abstractmethod
    def mark_processed(
        self,
        connection_name: str,
        table_name: str,
        ranges: List[BlockRange],
        batch_hash: Optional[str] = None,
    ) -> None:
        """
        Mark a set of block ranges as processed.

        Args:
            connection_name: Name of the connection
            table_name: Name of the destination table
            ranges: List of block ranges that were processed
            batch_hash: Optional hash of batch content for verification
        """
        pass

    @abstractmethod
    def cleanup_old_ranges(self, connection_name: str, table_name: str, days: int) -> int:
        """
        Clean up processed ranges older than specified days.

        Args:
            connection_name: Name of the connection
            table_name: Name of the destination table
            days: Delete ranges older than this many days

        Returns:
            Number of ranges deleted
        """
        pass


class DatabaseProcessedRangesStore(ProcessedRangesStore):
    """
    Store processed ranges in the destination database.

    Uses a dedicated table to track which block ranges have been processed,
    enabling exactly-once semantics even for non-transactional operations.
    """

    def __init__(self, config: IdempotencyConfig, db_connection):
        """
        Initialize database processed ranges store.

        Args:
            config: Idempotency configuration
            db_connection: Database connection object with execute() method
        """
        super().__init__(config)
        self.conn = db_connection
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Ensure the processed ranges table exists in the database"""
        table_name = f'{self.config.table_prefix}processed_ranges'

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            connection_name VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            network VARCHAR(50) NOT NULL,
            start_block BIGINT NOT NULL,
            end_block BIGINT NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            batch_hash VARCHAR(64),
            PRIMARY KEY (connection_name, table_name, network, start_block, end_block)
        )
        """

        # Create index for efficient lookups
        index_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_{self.config.table_prefix}processed_ranges_lookup
        ON {table_name}(connection_name, table_name, network, start_block)
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(create_sql)
            cursor.execute(index_sql)
            self.conn.commit()
            logger.debug(f'Ensured processed ranges table {table_name} exists')
        except Exception as e:
            logger.debug(f'Processed ranges table creation skipped: {e}')
            self.conn.rollback()

    def is_processed(self, connection_name: str, table_name: str, ranges: List[BlockRange]) -> bool:
        """Check if all ranges have been processed"""
        if not self.config.enabled or not ranges:
            return False

        table = f'{self.config.table_prefix}processed_ranges'

        # Check if ALL ranges in the list have been processed
        for block_range in ranges:
            check_sql = f"""
            SELECT COUNT(*) FROM {table}
            WHERE connection_name = %s
              AND table_name = %s
              AND network = %s
              AND start_block = %s
              AND end_block = %s
            """

            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    check_sql,
                    (
                        connection_name,
                        table_name,
                        block_range.network,
                        block_range.start,
                        block_range.end,
                    ),
                )
                count = cursor.fetchone()[0]

                if count == 0:
                    # This range not processed yet
                    return False

            except Exception as e:
                logger.error(f'Failed to check processed range: {e}')
                return False

        # All ranges have been processed
        logger.info(f'Duplicate detected: {len(ranges)} ranges already processed for {connection_name}.{table_name}')
        return True

    def mark_processed(
        self,
        connection_name: str,
        table_name: str,
        ranges: List[BlockRange],
        batch_hash: Optional[str] = None,
    ) -> None:
        """Mark ranges as processed"""
        if not self.config.enabled:
            return

        table = f'{self.config.table_prefix}processed_ranges'
        processed_at = datetime.now(UTC)

        insert_sql = f"""
        INSERT INTO {table} (connection_name, table_name, network, start_block, end_block, processed_at, batch_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (connection_name, table_name, network, start_block, end_block)
        DO UPDATE SET
            processed_at = EXCLUDED.processed_at,
            batch_hash = EXCLUDED.batch_hash
        """

        try:
            cursor = self.conn.cursor()

            for block_range in ranges:
                cursor.execute(
                    insert_sql,
                    (
                        connection_name,
                        table_name,
                        block_range.network,
                        block_range.start,
                        block_range.end,
                        processed_at,
                        batch_hash,
                    ),
                )

            self.conn.commit()
            logger.debug(f'Marked {len(ranges)} ranges as processed for {connection_name}.{table_name}')

        except Exception as e:
            logger.error(f'Failed to mark ranges as processed: {e}')
            self.conn.rollback()
            raise

    def cleanup_old_ranges(self, connection_name: str, table_name: str, days: int) -> int:
        """Clean up old processed ranges"""
        if not self.config.enabled:
            return 0

        table = f'{self.config.table_prefix}processed_ranges'

        delete_sql = f"""
        DELETE FROM {table}
        WHERE connection_name = %s
          AND table_name = %s
          AND processed_at < NOW() - INTERVAL '{days} days'
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(delete_sql, (connection_name, table_name))
            deleted_count = cursor.rowcount
            self.conn.commit()

            if deleted_count > 0:
                logger.info(
                    f'Cleaned up {deleted_count} old processed ranges for '
                    f'{connection_name}.{table_name} (older than {days} days)'
                )

            return deleted_count

        except Exception as e:
            logger.error(f'Failed to cleanup old ranges: {e}')
            self.conn.rollback()
            return 0


class NullProcessedRangesStore(ProcessedRangesStore):
    """No-op processed ranges store when idempotency is disabled"""

    def is_processed(self, connection_name: str, table_name: str, ranges: List[BlockRange]) -> bool:
        return False

    def mark_processed(
        self,
        connection_name: str,
        table_name: str,
        ranges: List[BlockRange],
        batch_hash: Optional[str] = None,
    ) -> None:
        pass

    def cleanup_old_ranges(self, connection_name: str, table_name: str, days: int) -> int:
        return 0


def compute_batch_hash(batch_data) -> str:
    """
    Compute a hash of batch data for verification.

    Args:
        batch_data: PyArrow RecordBatch

    Returns:
        SHA256 hash of batch content
    """
    try:
        # Convert batch to bytes for hashing
        import pyarrow as pa

        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch_data.schema)
        writer.write_batch(batch_data)
        writer.close()

        batch_bytes = sink.getvalue().to_pybytes()
        return hashlib.sha256(batch_bytes).hexdigest()

    except Exception as e:
        logger.warning(f'Failed to compute batch hash: {e}')
        return None
