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
        return BlockRange(network=self.network, start=min(self.start, other.start), end=max(self.end, other.end))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BlockRange':
        """Create BlockRange from dictionary"""
        return cls(network=data['network'], start=data['start'], end=data['end'])

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {'network': self.network, 'start': self.start, 'end': self.end}


@dataclass
class BatchMetadata:
    """Metadata associated with a response batch"""

    ranges: List[BlockRange]
    # Additional metadata fields can be added here
    extra: Optional[Dict[str, Any]] = None

    @classmethod
    def from_flight_data(cls, metadata_bytes: bytes) -> 'BatchMetadata':
        """Parse metadata from Flight data"""
        try:
            metadata_dict = json.loads(metadata_bytes.decode('utf-8'))
            ranges = [BlockRange.from_dict(r) for r in metadata_dict.get('ranges', [])]
            extra = {k: v for k, v in metadata_dict.items() if k != 'ranges'}
            return cls(ranges=ranges, extra=extra if extra else None)
        except (json.JSONDecodeError, KeyError) as e:
            # Fallback to empty metadata if parsing fails
            return cls(ranges=[], extra={'parse_error': str(e)})


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
