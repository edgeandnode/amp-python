"""
Core types for streaming data loading functionality.
"""

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pyarrow as pa


@dataclass
class BlockRange:
    """Represents a range of blocks for a specific network"""

    network: str
    start: int
    end: int
    hash: Optional[str] = None  # Block hash from server (for end block)
    prev_hash: Optional[str] = None  # Previous block hash (for chain validation)

    def __post_init__(self):
        if self.start > self.end:
            raise ValueError(f'Invalid range: start ({self.start}) > end ({self.end})')

    def overlaps_with(self, other: 'BlockRange') -> bool:
        """Check if this range overlaps with another range on the same network"""
        if self.network != other.network:
            return False
        return not (self.end < other.start or other.end < self.start)

    def invalidates(self, other: 'BlockRange') -> bool:
        """Return true if this range would invalidate the other range (same as overlaps_with)"""
        return self.overlaps_with(other)

    def contains_block(self, block_num: int) -> bool:
        """Check if a block number is within this range"""
        return self.start <= block_num <= self.end

    def merge_with(self, other: 'BlockRange') -> 'BlockRange':
        """Merge with another range on the same network"""
        if self.network != other.network:
            raise ValueError(f'Cannot merge ranges from different networks: {self.network} vs {other.network}')
        return BlockRange(
            network=self.network,
            start=min(self.start, other.start),
            end=max(self.end, other.end),
            hash=other.hash if other.end > self.end else self.hash,
            prev_hash=self.prev_hash,  # Keep original prev_hash
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BlockRange':
        """Create BlockRange from dictionary (supports both server and client formats)

        The server sends ranges with nested numbers: {"numbers": {"start": X, "end": Y}, ...}
        But our to_dict() outputs flat format: {"start": X, "end": Y, ...} for simplicity.

        Both formats must be supported because:
        - Server → Client: Uses nested "numbers" format (confirmed 2025-10-23)
        - Client → Storage: Uses flat format for checkpoints, watermarks, internal state
        - Backward compatibility: Existing stored state uses flat format
        """
        # Server format: {"numbers": {"start": X, "end": Y}, "network": ..., "hash": ..., "prev_hash": ...}
        if 'numbers' in data:
            numbers = data['numbers']
            return cls(
                network=data['network'],
                start=numbers.get('start') if isinstance(numbers, dict) else numbers['start'],
                end=numbers.get('end') if isinstance(numbers, dict) else numbers['end'],
                hash=data.get('hash'),
                prev_hash=data.get('prev_hash'),
            )
        else:
            # Client/internal format: {"network": ..., "start": ..., "end": ...}
            # Used by to_dict(), checkpoints, watermarks, and stored state
            return cls(
                network=data['network'],
                start=data['start'],
                end=data['end'],
                hash=data.get('hash'),
                prev_hash=data.get('prev_hash'),
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (client format for simplicity)"""
        result = {'network': self.network, 'start': self.start, 'end': self.end}
        if self.hash is not None:
            result['hash'] = self.hash
        if self.prev_hash is not None:
            result['prev_hash'] = self.prev_hash
        return result


@dataclass
class BatchMetadata:
    """Metadata associated with a response batch"""

    ranges: List[BlockRange]
    ranges_complete: bool = False  # Marks safe checkpoint boundaries
    extra: Optional[Dict[str, Any]] = None

    @classmethod
    def from_flight_data(cls, metadata_bytes: bytes) -> 'BatchMetadata':
        """Parse metadata from Flight data"""
        try:
            # Handle PyArrow Buffer objects
            if hasattr(metadata_bytes, 'to_pybytes'):
                metadata_str = metadata_bytes.to_pybytes().decode('utf-8')
            else:
                metadata_str = metadata_bytes.decode('utf-8')
            metadata_dict = json.loads(metadata_str)

            # Parse block ranges
            ranges = [BlockRange.from_dict(r) for r in metadata_dict.get('ranges', [])]

            # Extract ranges_complete flag (server sends this at microbatch boundaries)
            ranges_complete = metadata_dict.get('ranges_complete', False)

            # Store remaining fields in extra
            extra = {k: v for k, v in metadata_dict.items() if k not in ('ranges', 'ranges_complete')}

            return cls(
                ranges=ranges, ranges_complete=ranges_complete, extra=extra if extra else None
            )
        except (json.JSONDecodeError, KeyError) as e:
            # Fallback to empty metadata if parsing fails
            return cls(ranges=[], ranges_complete=False, extra={'parse_error': str(e)})


@dataclass
class ResponseBatch:
    """Response batch containing data and metadata"""

    data: pa.RecordBatch
    metadata: BatchMetadata

    @property
    def num_rows(self) -> int:
        """Number of rows in the batch"""
        return self.data.num_rows

    @property
    def networks(self) -> List[str]:
        """List of networks covered by this batch"""
        return list(set(r.network for r in self.metadata.ranges))


class ResponseBatchType(Enum):
    """Type of response batch"""

    DATA = 'data'
    REORG = 'reorg'


@dataclass
class ResponseBatchWithReorg:
    """Response that can be either a data batch or a reorg notification"""

    batch_type: ResponseBatchType
    data: Optional[ResponseBatch] = None
    invalidation_ranges: Optional[List[BlockRange]] = None

    @property
    def is_data(self) -> bool:
        """True if this is a data batch"""
        return self.batch_type == ResponseBatchType.DATA

    @property
    def is_reorg(self) -> bool:
        """True if this is a reorg notification"""
        return self.batch_type == ResponseBatchType.REORG

    @classmethod
    def data_batch(cls, batch: ResponseBatch) -> 'ResponseBatchWithReorg':
        """Create a data batch response"""
        return cls(batch_type=ResponseBatchType.DATA, data=batch)

    @classmethod
    def reorg_batch(cls, invalidation_ranges: List[BlockRange]) -> 'ResponseBatchWithReorg':
        """Create a reorg notification response"""
        return cls(batch_type=ResponseBatchType.REORG, invalidation_ranges=invalidation_ranges)


@dataclass
class ResumeWatermark:
    """Watermark for resuming streaming queries"""

    ranges: List[BlockRange]
    timestamp: Optional[str] = None
    sequence: Optional[int] = None

    def to_json(self) -> str:
        """Serialize to JSON string for HTTP headers"""
        data = {'ranges': [r.to_dict() for r in self.ranges]}
        if self.timestamp:
            data['timestamp'] = self.timestamp
        if self.sequence is not None:
            data['sequence'] = self.sequence
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> 'ResumeWatermark':
        """Deserialize from JSON string"""
        data = json.loads(json_str)
        ranges = [BlockRange.from_dict(r) for r in data['ranges']]
        return cls(ranges=ranges, timestamp=data.get('timestamp'), sequence=data.get('sequence'))
