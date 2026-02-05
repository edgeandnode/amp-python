"""
LMDB-based stream state store for durable batch tracking.

This implementation uses LMDB (Lightning Memory-Mapped Database) for fast,
embedded, durable storage of batch processing state. It can be used with any
loader (Kafka, PostgreSQL, etc.) to provide crash recovery and idempotency.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import lmdb

from .state import BatchIdentifier, StreamStateStore
from .types import BlockRange, ResumeWatermark


class LMDBStreamStateStore(StreamStateStore):
    """
    Generic LMDB-based state store for tracking processed batches.

    Uses LMDB for fast, durable key-value storage with ACID transactions.
    Tracks individual batches with unique hash-based IDs to support:
    - Crash recovery and resume
    - Idempotency (duplicate detection)
    - Reorg handling (invalidate by block hash)
    - Gap detection for parallel loading

    Uses two LMDB sub-databases for efficient queries:
    1. "batches" - Individual batch records keyed by batch_id
    2. "metadata" - Max block metadata per network for fast resume position queries

    Batch database layout:
    - Key: {connection_name}|{table_name}|{batch_id}
    - Value: JSON with {network, start_block, end_block, end_hash, start_parent_hash}

    Metadata database layout:
    - Key: {connection_name}|{table_name}|{network}
    - Value: JSON with {end_block, end_hash} (max processed block for resume)
    """

    env: lmdb.Environment

    def __init__(
        self,
        connection_name: str,
        data_dir: str = '.amp_state',
        map_size: int = 10 * 1024 * 1024 * 1024,
        sync: bool = True,
    ):
        """
        Initialize LMDB state store with two sub-databases.

        Args:
            connection_name: Name of the connection (for multi-connection support)
            data_dir: Directory to store LMDB database files
            map_size: Maximum database size in bytes (default: 10GB)
            sync: Whether to sync writes to disk (True for durability, False for speed)
        """
        self.connection_name = connection_name
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(__name__)

        self.env = lmdb.open(str(self.data_dir), map_size=map_size, sync=sync, max_dbs=2)

        self.batches_db = self.env.open_db(b'batches')
        self.metadata_db = self.env.open_db(b'metadata')

        self.logger.info(f'Initialized LMDB state store at {self.data_dir} with 2 sub-databases')

    def _make_batch_key(self, connection_name: str, table_name: str, batch_id: str) -> bytes:
        """Create composite key for batch database."""
        return f'{connection_name}|{table_name}|{batch_id}'.encode('utf-8')

    def _make_metadata_key(self, connection_name: str, table_name: str, network: str) -> bytes:
        """Create composite key for metadata database."""
        return f'{connection_name}|{table_name}|{network}'.encode('utf-8')

    def _parse_key(self, key: bytes) -> tuple[str, str, str]:
        """Parse composite key into (connection_name, table_name, batch_id/network)."""
        parts = key.decode('utf-8').split('|')
        return parts[0], parts[1], parts[2]

    def _serialize_batch(self, batch: BatchIdentifier) -> bytes:
        """Serialize BatchIdentifier to JSON bytes."""
        batch_value_dict = {
            'network': batch.network,
            'start_block': batch.start_block,
            'end_block': batch.end_block,
            'end_hash': batch.end_hash,
            'start_parent_hash': batch.start_parent_hash,
        }
        return json.dumps(batch_value_dict).encode('utf-8')

    def _serialize_metadata(self, end_block: int, end_hash: str) -> bytes:
        """Serialize metadata to JSON bytes."""
        meta_value_dict = {
            'end_block': end_block,
            'end_hash': end_hash,
        }
        return json.dumps(meta_value_dict).encode('utf-8')

    def _deserialize_batch(self, value: bytes) -> Dict:
        """Deserialize batch data from JSON bytes."""
        return json.loads(value.decode('utf-8'))

    def is_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> bool:
        """
        Check if all given batches have already been processed.

        Args:
            connection_name: Connection identifier
            table_name: Name of the table being loaded
            batch_ids: List of batch identifiers to check

        Returns:
            True only if ALL batches are already processed
        """
        if not batch_ids:
            return False

        with self.env.begin(db=self.batches_db) as txn:
            for batch_id in batch_ids:
                key = self._make_batch_key(connection_name, table_name, batch_id.unique_id)
                value = txn.get(key)
                if value is None:
                    return False

        return True

    def mark_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> None:
        """
        Mark batches as processed in durable storage.

        Atomically updates both batch records and metadata (max block per network).

        Args:
            connection_name: Connection identifier
            table_name: Name of the table being loaded
            batch_ids: List of batch identifiers to mark as processed
        """
        with self.env.begin(write=True) as txn:
            for batch in batch_ids:
                batch_key = self._make_batch_key(connection_name, table_name, batch.unique_id)
                batch_value = self._serialize_batch(batch)
                txn.put(batch_key, batch_value, db=self.batches_db)

                meta_key = self._make_metadata_key(connection_name, table_name, batch.network)
                current_meta = txn.get(meta_key, db=self.metadata_db)

                should_update = False
                if current_meta is None:
                    should_update = True
                else:
                    current_meta_dict = self._deserialize_batch(current_meta)
                    if batch.end_block > current_meta_dict['end_block']:
                        should_update = True

                if should_update:
                    meta_value = self._serialize_metadata(batch.end_block, batch.end_hash)
                    txn.put(meta_key, meta_value, db=self.metadata_db)

        self.logger.debug(f'Marked {len(batch_ids)} batches as processed in {table_name}')

    def get_resume_position(
        self, connection_name: str, table_name: str, detect_gaps: bool = False
    ) -> Optional[ResumeWatermark]:
        """
        Get the resume watermark (max processed block per network).

        Reads only from metadata database. Does not scan batch records.

        Args:
            connection_name: Connection identifier
            table_name: Destination table name
            detect_gaps: If True, detect gaps. Not implemented - raises error.

        Returns:
            ResumeWatermark with max block ranges for all networks, or None if no state exists

        Raises:
            NotImplementedError: If detect_gaps=True
        """
        if detect_gaps:
            raise NotImplementedError('Gap detection not implemented in LMDB state store')

        prefix = f'{connection_name}|{table_name}|'.encode('utf-8')
        ranges = []

        with self.env.begin(db=self.metadata_db) as txn:
            cursor = txn.cursor()

            if not cursor.set_range(prefix):
                return None

            for key, value in cursor:
                if not key.startswith(prefix):
                    break

                try:
                    _, _, network = self._parse_key(key)
                    meta_data = self._deserialize_batch(value)

                    # BlockRange here represents a resume watermark (open-ended), not a processed range.
                    # start=end=end_block means "resume streaming from this block onwards".
                    # See ReorgAwareStream for how this watermark is used for crash recovery.
                    ranges.append(
                        BlockRange(
                            network=network,
                            start=meta_data['end_block'],
                            end=meta_data['end_block'],
                            hash=meta_data.get('end_hash'),
                        )
                    )

                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.warning(f'Failed to parse metadata: {e}')
                    continue

        if not ranges:
            return None

        return ResumeWatermark(ranges=ranges)

    def invalidate_from_block(
        self, connection_name: str, table_name: str, network: str, from_block: int
    ) -> List[BatchIdentifier]:
        """
        Invalidate (delete) all batches from a specific block onwards.

        Used for reorg handling to remove invalidated data. Requires full scan
        of batches database to find matching batches.

        Args:
            connection_name: Connection identifier
            table_name: Name of the table
            network: Network name
            from_block: Block number to invalidate from (inclusive)

        Returns:
            List of BatchIdentifier objects that were invalidated
        """
        prefix = f'{connection_name}|{table_name}|'.encode('utf-8')
        invalidated_batch_ids = []
        keys_to_delete = []

        with self.env.begin(db=self.batches_db) as txn:
            cursor = txn.cursor()

            if not cursor.set_range(prefix):
                return []

            for key, value in cursor:
                if not key.startswith(prefix):
                    break

                try:
                    batch_data = self._deserialize_batch(value)

                    if batch_data['network'] == network and batch_data['end_block'] >= from_block:
                        batch_id = BatchIdentifier(
                            network=batch_data['network'],
                            start_block=batch_data['start_block'],
                            end_block=batch_data['end_block'],
                            end_hash=batch_data.get('end_hash'),
                            start_parent_hash=batch_data.get('start_parent_hash'),
                        )
                        invalidated_batch_ids.append(batch_id)
                        keys_to_delete.append(key)

                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.warning(f'Failed to parse batch data during invalidation: {e}')
                    continue

        if keys_to_delete:
            with self.env.begin(write=True) as txn:
                for key in keys_to_delete:
                    txn.delete(key, db=self.batches_db)

                meta_key = self._make_metadata_key(connection_name, table_name, network)

                remaining_batches = []
                cursor = txn.cursor(db=self.batches_db)
                if cursor.set_range(prefix):
                    for key, value in cursor:
                        if not key.startswith(prefix):
                            break
                        try:
                            batch_data = self._deserialize_batch(value)
                            if batch_data['network'] == network:
                                remaining_batches.append(batch_data)
                        except (json.JSONDecodeError, KeyError) as e:
                            self.logger.warning(f'Failed to parse batch data during metadata recalculation: {e}')
                            continue

                if remaining_batches:
                    remaining_batches.sort(key=lambda b: b['end_block'])
                    max_batch = remaining_batches[-1]
                    meta_value = self._serialize_metadata(max_batch['end_block'], max_batch.get('end_hash'))
                    txn.put(meta_key, meta_value, db=self.metadata_db)
                else:
                    txn.delete(meta_key, db=self.metadata_db)

            self.logger.info(
                f'Invalidated {len(invalidated_batch_ids)} batches from block {from_block} on {network} in {table_name}'
            )

        return invalidated_batch_ids

    def cleanup_before_block(self, connection_name: str, table_name: str, network: str, before_block: int) -> None:
        """
        Clean up old batch records before a specific block.

        Removes batches where end_block < before_block. Requires full scan
        to find matching batches for the given network.

        Args:
            connection_name: Connection identifier
            table_name: Name of the table
            network: Network name
            before_block: Block number to clean up before (exclusive)
        """
        prefix = f'{connection_name}|{table_name}|'.encode('utf-8')
        keys_to_delete = []

        with self.env.begin(db=self.batches_db) as txn:
            cursor = txn.cursor()

            if not cursor.set_range(prefix):
                return

            for key, value in cursor:
                if not key.startswith(prefix):
                    break

                try:
                    batch_data = self._deserialize_batch(value)

                    if batch_data['network'] == network and batch_data['end_block'] < before_block:
                        keys_to_delete.append(key)

                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.warning(f'Failed to parse batch data during cleanup: {e}')
                    continue

        if keys_to_delete:
            with self.env.begin(write=True, db=self.batches_db) as txn:
                for key in keys_to_delete:
                    txn.delete(key)

            self.logger.info(
                f'Cleaned up {len(keys_to_delete)} old batches before block {before_block} on {network} in {table_name}'
            )

    def close(self) -> None:
        """Close the LMDB environment."""
        if self.env:
            self.env.close()
            self.logger.info('Closed LMDB state store')

    def __enter__(self) -> 'LMDBStreamStateStore':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
