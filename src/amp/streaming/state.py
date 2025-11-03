"""
Unified stream state management for amp.

This module replaces the separate checkpoint and processed_ranges systems with a
single unified mechanism that provides both resumability and idempotency.
"""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Dict, List, Optional, Set, Tuple

from amp.streaming.types import BlockRange, ResumeWatermark


@dataclass(frozen=True, eq=True)
class BatchIdentifier:
    """
    Unique identifier for a microbatch based on its block range and chain state.

    This serves as the atomic unit of processing across the entire system:
    - Used for idempotency checks (prevent duplicate processing)
    - Stored as metadata in data tables (enable fast invalidation)
    - Tracked in state store (for resume position calculation)

    The unique_id is a hash of the block range + block hashes, making it unique
    across blockchain reorganizations (same range, different hash = different batch).
    """

    network: str
    start_block: int
    end_block: int
    end_hash: str  # Hash of the end block (required for uniqueness)
    start_parent_hash: str = ''  # Hash of block before start (optional for chain validation)

    @property
    def unique_id(self) -> str:
        """
        Generate a 16-character hex string as unique identifier.

        Uses SHA256 hash of canonical representation to ensure:
        - Deterministic (same input always produces same ID)
        - Collision-resistant (extremely unlikely to have duplicates)
        - Compact (16 hex chars = 64 bits, suitable for indexing)
        """
        canonical = f'{self.network}:{self.start_block}:{self.end_block}:{self.end_hash}:{self.start_parent_hash}'
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    @property
    def position_key(self) -> Tuple[str, int, int]:
        """Position-based key for range queries (network, start, end)."""
        return (self.network, self.start_block, self.end_block)

    @classmethod
    def from_block_range(cls, br: BlockRange) -> 'BatchIdentifier':
        """
        Create BatchIdentifier from a BlockRange metadata object.

        Supports two modes:
        1. Hash-based IDs: When BlockRange has server-provided block hash (streaming with reorg detection)
        2. Position-based IDs: When BlockRange lacks hash (parallel loads from regular queries)

        Both produce compact 16-char hex IDs, but position-based IDs are derived from
        block range coordinates only, making them suitable for immutable historical data.
        """
        if br.hash:
            # Hash-based ID: Include server-provided block hash for reorg detection
            end_hash = br.hash
        else:
            # Position-based ID: Generate synthetic hash from block range coordinates
            # This provides same compact format without requiring server-provided hashes
            import hashlib

            canonical = f'{br.network}:{br.start}:{br.end}'
            end_hash = hashlib.sha256(canonical.encode('utf-8')).hexdigest()

        return cls(
            network=br.network,
            start_block=br.start,
            end_block=br.end,
            end_hash=end_hash,
            start_parent_hash=br.prev_hash or '',
        )

    def to_block_range(self) -> BlockRange:
        """Convert back to BlockRange for server communication."""
        return BlockRange(
            network=self.network,
            start=self.start_block,
            end=self.end_block,
            hash=self.end_hash,
            prev_hash=self.start_parent_hash or None,
        )

    def overlaps_or_after(self, from_block: int) -> bool:
        """Check if this batch overlaps or comes after a given block number."""
        return self.end_block >= from_block


@dataclass
class ProcessedBatch:
    """
    Record of a successfully processed batch with full metadata.

    This is the persistence format used by database-backed StreamStateStore
    implementations. The in-memory store just uses BatchIdentifier directly.
    """

    batch_id: BatchIdentifier
    processed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    reorg_invalidation: bool = False  # Marks batches deleted due to reorg

    def to_dict(self) -> dict:
        """Serialize for database storage."""
        return {
            'network': self.batch_id.network,
            'start_block': self.batch_id.start_block,
            'end_block': self.batch_id.end_block,
            'end_hash': self.batch_id.end_hash,
            'start_parent_hash': self.batch_id.start_parent_hash,
            'unique_id': self.batch_id.unique_id,
            'processed_at': self.processed_at.isoformat(),
            'reorg_invalidation': self.reorg_invalidation,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ProcessedBatch':
        """Deserialize from database storage."""
        batch_id = BatchIdentifier(
            network=data['network'],
            start_block=data['start_block'],
            end_block=data['end_block'],
            end_hash=data['end_hash'],
            start_parent_hash=data.get('start_parent_hash', ''),
        )
        return cls(
            batch_id=batch_id,
            processed_at=datetime.fromisoformat(data['processed_at']),
            reorg_invalidation=data.get('reorg_invalidation', False),
        )


class StreamStateStore(ABC):
    """
    Abstract base class for unified stream state management.

    Replaces both CheckpointStore and ProcessedRangesStore with a single
    mechanism that provides:
    - Idempotency: Check if batches were already processed
    - Resumability: Calculate resume position from processed batches
    - Reorg handling: Invalidate batches affected by chain reorganizations
    """

    @abstractmethod
    def is_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> bool:
        """
        Check if all given batches have already been processed.

        Used for idempotency - prevents duplicate processing of the same data.
        Returns True only if ALL batches in the list are already processed.
        """
        pass

    @abstractmethod
    def mark_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> None:
        """
        Mark the given batches as successfully processed.

        Called after data has been committed to the target system.
        """
        pass

    @abstractmethod
    def get_resume_position(
        self, connection_name: str, table_name: str, detect_gaps: bool = False
    ) -> Optional[ResumeWatermark]:
        """
        Calculate the resume position from processed batches.

        Args:
            connection_name: Connection identifier
            table_name: Destination table name
            detect_gaps: If True, detect and return gaps in processed ranges.
                        If False, only return max processed position per network.

        Returns:
            ResumeWatermark with ranges. When detect_gaps=True:
            - Gap ranges: BlockRange(network, gap_start, gap_end, hash=None)
            - Remaining range markers: BlockRange(network, max_block+1, max_block+1, hash=end_hash)
              (start==end signals "process from here to max_block in config")

            When detect_gaps=False:
            - Returns only the maximum processed block for each network
        """
        pass

    @abstractmethod
    def invalidate_from_block(
        self, connection_name: str, table_name: str, network: str, from_block: int
    ) -> List[BatchIdentifier]:
        """
        Invalidate batches affected by a blockchain reorganization.

        Removes all batches for the given network where end_block >= from_block.
        Returns the list of invalidated batch IDs for use in deleting data.
        """
        pass

    @abstractmethod
    def cleanup_before_block(self, connection_name: str, table_name: str, network: str, before_block: int) -> None:
        """
        Remove old batch records before a given block number.

        Used for TTL-based cleanup to prevent unbounded state growth.
        """
        pass


class InMemoryStreamStateStore(StreamStateStore):
    """
    In-memory implementation of StreamStateStore.

    This is the default implementation that works immediately without any
    database dependencies. State is lost on process restart, but provides
    idempotency within a single session.

    Loaders can optionally implement persistent versions that survive restarts.
    """

    def __init__(self):
        # Key: (connection_name, table_name, network)
        # Value: Set of BatchIdentifier objects
        self._state: Dict[Tuple[str, str, str], Set[BatchIdentifier]] = {}

    def _get_key(
        self, connection_name: str, table_name: str, network: Optional[str] = None
    ) -> Tuple[str, str, str] | List[Tuple[str, str, str]]:
        """Get storage key(s) for the given parameters."""
        if network:
            return (connection_name, table_name, network)
        else:
            # Return all keys for this connection/table across all networks
            return [k for k in self._state.keys() if k[0] == connection_name and k[1] == table_name]

    def is_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> bool:
        """Check if all batches have been processed."""
        if not batch_ids:
            return False

        # Group by network
        by_network: Dict[str, List[BatchIdentifier]] = {}
        for batch_id in batch_ids:
            by_network.setdefault(batch_id.network, []).append(batch_id)

        # Check each network
        for network, network_batch_ids in by_network.items():
            key = self._get_key(connection_name, table_name, network)
            processed = self._state.get(key, set())

            # All batches for this network must be in the processed set
            for batch_id in network_batch_ids:
                if batch_id not in processed:
                    return False

        return True

    def mark_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> None:
        """Mark batches as processed."""
        # Group by network
        by_network: Dict[str, List[BatchIdentifier]] = {}
        for batch_id in batch_ids:
            by_network.setdefault(batch_id.network, []).append(batch_id)

        # Store in sets by network
        for network, network_batch_ids in by_network.items():
            key = self._get_key(connection_name, table_name, network)
            if key not in self._state:
                self._state[key] = set()

            self._state[key].update(network_batch_ids)

    def get_resume_position(
        self, connection_name: str, table_name: str, detect_gaps: bool = False
    ) -> Optional[ResumeWatermark]:
        """
        Calculate resume position from processed batches.

        Args:
            connection_name: Connection identifier
            table_name: Destination table name
            detect_gaps: If True, detect and return gaps in processed ranges

        Returns:
            ResumeWatermark with gap and/or continuation ranges
        """
        keys = self._get_key(connection_name, table_name)
        if not isinstance(keys, list):
            keys = [keys]

        if not detect_gaps:
            # Simple mode: Return max processed position per network
            return self._get_max_processed_position(keys)

        # Gap-aware mode: Detect gaps and combine with remaining range markers
        gaps = self._detect_gaps_in_memory(keys)
        max_positions = self._get_max_processed_position(keys)

        if not gaps and not max_positions:
            return None

        all_ranges = []

        # Add gap ranges
        all_ranges.extend(gaps)

        # Add remaining range markers (after max processed block, to finish historical catch-up)
        if max_positions:
            for br in max_positions.ranges:
                all_ranges.append(
                    BlockRange(
                        network=br.network,
                        start=br.end + 1,
                        end=br.end + 1,  # Same value = marker for remaining unprocessed range
                        hash=br.hash,
                        prev_hash=br.prev_hash,
                    )
                )

        return ResumeWatermark(ranges=all_ranges) if all_ranges else None

    def _get_max_processed_position(self, keys: List[Tuple[str, str, str]]) -> Optional[ResumeWatermark]:
        """Get max processed position for each network (simple mode)."""
        # Find max block for each network
        max_by_network: Dict[str, BatchIdentifier] = {}

        for key in keys:
            network = key[2]
            batches = self._state.get(key, set())

            if batches:
                # Find batch with highest end_block for this network
                max_batch = max(batches, key=lambda b: b.end_block)

                if network not in max_by_network or max_batch.end_block > max_by_network[network].end_block:
                    max_by_network[network] = max_batch

        if not max_by_network:
            return None

        # Convert to BlockRange list for ResumeWatermark
        ranges = [batch_id.to_block_range() for batch_id in max_by_network.values()]
        return ResumeWatermark(ranges=ranges)

    def _detect_gaps_in_memory(self, keys: List[Tuple[str, str, str]]) -> List[BlockRange]:
        """Detect gaps in processed ranges using in-memory analysis."""
        gaps = []

        for key in keys:
            network = key[2]
            batches = self._state.get(key, set())

            if not batches:
                continue

            # Sort batches by end_block
            sorted_batches = sorted(batches, key=lambda b: b.end_block)

            # Find gaps between consecutive batches
            for i in range(len(sorted_batches) - 1):
                current_batch = sorted_batches[i]
                next_batch = sorted_batches[i + 1]

                # Gap exists if next batch doesn't start immediately after current
                if next_batch.start_block > current_batch.end_block + 1:
                    gaps.append(
                        BlockRange(
                            network=network,
                            start=current_batch.end_block + 1,
                            end=next_batch.start_block - 1,
                            hash=None,  # Position-based for gaps
                            prev_hash=None,
                        )
                    )

        return gaps

    def invalidate_from_block(
        self, connection_name: str, table_name: str, network: str, from_block: int
    ) -> List[BatchIdentifier]:
        """Invalidate batches affected by reorg."""
        key = self._get_key(connection_name, table_name, network)
        batches = self._state.get(key, set())

        # Find batches that overlap or come after the reorg point
        affected = [b for b in batches if b.overlaps_or_after(from_block)]

        # Remove from state
        if affected:
            self._state[key] = batches - set(affected)

        return affected

    def cleanup_before_block(self, connection_name: str, table_name: str, network: str, before_block: int) -> None:
        """Remove old batches before a given block."""
        key = self._get_key(connection_name, table_name, network)
        batches = self._state.get(key, set())

        # Keep only batches that end at or after the cutoff
        kept = {b for b in batches if b.end_block >= before_block}

        if kept != batches:
            self._state[key] = kept


class NullStreamStateStore(StreamStateStore):
    """
    No-op implementation that disables state tracking.

    Used when state management is disabled entirely. All operations are no-ops,
    providing no resumability or idempotency guarantees.
    """

    def is_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> bool:
        """Always return False (never skip processing)."""
        return False

    def mark_processed(self, connection_name: str, table_name: str, batch_ids: List[BatchIdentifier]) -> None:
        """No-op."""
        pass

    def get_resume_position(
        self, connection_name: str, table_name: str, detect_gaps: bool = False
    ) -> Optional[ResumeWatermark]:
        """Always return None (no resume position available)."""
        return None

    def invalidate_from_block(
        self, connection_name: str, table_name: str, network: str, from_block: int
    ) -> List[BatchIdentifier]:
        """Return empty list (nothing to invalidate)."""
        return []

    def cleanup_before_block(self, connection_name: str, table_name: str, network: str, before_block: int) -> None:
        """No-op."""
        pass
