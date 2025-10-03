"""
Unit tests for streaming types and pure functions.
"""

import json

import pyarrow as pa
import pytest

from src.amp.streaming.reorg import ReorgAwareStream
from src.amp.streaming.types import (
    BatchMetadata,
    BlockRange,
    ResponseBatch,
    ResponseBatchType,
    ResponseBatchWithReorg,
    ResumeWatermark,
)


@pytest.mark.unit
class TestBlockRange:
    """Test BlockRange dataclass and methods"""

    def test_valid_range_creation(self):
        """Test creating valid block ranges"""
        br = BlockRange(network='ethereum', start=100, end=200)
        assert br.network == 'ethereum'
        assert br.start == 100
        assert br.end == 200

    def test_invalid_range_raises_error(self):
        """Test that start > end raises ValueError"""
        with pytest.raises(ValueError, match='Invalid range: start \\(200\\) > end \\(100\\)'):
            BlockRange(network='ethereum', start=200, end=100)

    def test_overlaps_with_same_network(self):
        """Test overlap detection on same network"""
        br1 = BlockRange(network='ethereum', start=100, end=200)
        br2 = BlockRange(network='ethereum', start=150, end=250)
        br3 = BlockRange(network='ethereum', start=250, end=300)
        br4 = BlockRange(network='ethereum', start=50, end=100)

        # Overlapping ranges
        assert br1.overlaps_with(br2) == True
        assert br2.overlaps_with(br1) == True
        assert br1.overlaps_with(br4) == True

        # Non-overlapping ranges
        assert br1.overlaps_with(br3) == False
        assert br3.overlaps_with(br1) == False

    def test_overlaps_with_different_network(self):
        """Test that ranges on different networks don't overlap"""
        br1 = BlockRange(network='ethereum', start=100, end=200)
        br2 = BlockRange(network='polygon', start=100, end=200)

        assert br1.overlaps_with(br2) == False
        assert br2.overlaps_with(br1) == False

    def test_invalidates_is_same_as_overlaps(self):
        """Test that invalidates() behaves same as overlaps_with()"""
        br1 = BlockRange(network='ethereum', start=100, end=200)
        br2 = BlockRange(network='ethereum', start=150, end=250)

        assert br1.invalidates(br2) == br1.overlaps_with(br2)

    def test_contains_block(self):
        """Test block number containment"""
        br = BlockRange(network='ethereum', start=100, end=200)

        # Inside range
        assert br.contains_block(100) == True
        assert br.contains_block(150) == True
        assert br.contains_block(200) == True

        # Outside range
        assert br.contains_block(99) == False
        assert br.contains_block(201) == False

    def test_merge_with_same_network(self):
        """Test merging ranges on same network"""
        br1 = BlockRange(network='ethereum', start=100, end=200)
        br2 = BlockRange(network='ethereum', start=150, end=300)

        merged = br1.merge_with(br2)
        assert merged.network == 'ethereum'
        assert merged.start == 100
        assert merged.end == 300

        # Test with non-overlapping ranges
        br3 = BlockRange(network='ethereum', start=400, end=500)
        merged2 = br1.merge_with(br3)
        assert merged2.start == 100
        assert merged2.end == 500

    def test_merge_with_different_network_raises_error(self):
        """Test that merging different networks raises ValueError"""
        br1 = BlockRange(network='ethereum', start=100, end=200)
        br2 = BlockRange(network='polygon', start=150, end=300)

        with pytest.raises(ValueError, match='Cannot merge ranges from different networks'):
            br1.merge_with(br2)

    def test_serialization(self):
        """Test to_dict and from_dict methods"""
        br = BlockRange(network='ethereum', start=100, end=200)

        # To dict
        data = br.to_dict()
        assert data == {'network': 'ethereum', 'start': 100, 'end': 200}

        # From dict
        br2 = BlockRange.from_dict(data)
        assert br2.network == br.network
        assert br2.start == br.start
        assert br2.end == br.end


@pytest.mark.unit
class TestBatchMetadata:
    """Test BatchMetadata parsing and handling"""

    def test_from_flight_data_valid_json(self):
        """Test parsing valid JSON metadata"""
        metadata_dict = {
            'ranges': [
                {'network': 'ethereum', 'start': 100, 'end': 200},
                {'network': 'polygon', 'start': 50, 'end': 150},
            ],
            'extra_field': 'value',
            'number': 42,
        }
        metadata_bytes = json.dumps(metadata_dict).encode('utf-8')

        bm = BatchMetadata.from_flight_data(metadata_bytes)

        assert len(bm.ranges) == 2
        assert bm.ranges[0].network == 'ethereum'
        assert bm.ranges[1].network == 'polygon'
        assert bm.extra == {'extra_field': 'value', 'number': 42}

    def test_from_flight_data_empty_ranges(self):
        """Test parsing metadata with no ranges"""
        metadata_bytes = json.dumps({'other': 'data'}).encode('utf-8')
        bm = BatchMetadata.from_flight_data(metadata_bytes)

        assert len(bm.ranges) == 0
        assert bm.extra == {'other': 'data'}

    def test_from_flight_data_invalid_json(self):
        """Test parsing invalid JSON falls back gracefully"""
        metadata_bytes = b'invalid json'
        bm = BatchMetadata.from_flight_data(metadata_bytes)

        assert len(bm.ranges) == 0
        assert bm.extra is not None
        assert 'parse_error' in bm.extra

    def test_from_flight_data_malformed_range(self):
        """Test parsing with malformed range data"""
        metadata_dict = {
            'ranges': [
                {'network': 'ethereum'}  # Missing start/end
            ]
        }
        metadata_bytes = json.dumps(metadata_dict).encode('utf-8')

        bm = BatchMetadata.from_flight_data(metadata_bytes)

        assert len(bm.ranges) == 0
        assert 'parse_error' in bm.extra


@pytest.mark.unit
class TestResponseBatch:
    """Test ResponseBatch properties"""

    def test_num_rows_property(self):
        """Test num_rows property delegates to data"""
        # Create a simple record batch
        data = pa.record_batch([pa.array([1, 2, 3, 4, 5]), pa.array(['a', 'b', 'c', 'd', 'e'])], names=['id', 'value'])

        metadata = BatchMetadata(ranges=[])
        rb = ResponseBatch(data=data, metadata=metadata)

        assert rb.num_rows == 5

    def test_networks_property(self):
        """Test networks property extracts unique networks"""
        metadata = BatchMetadata(
            ranges=[
                BlockRange(network='ethereum', start=100, end=200),
                BlockRange(network='polygon', start=50, end=150),
                BlockRange(network='ethereum', start=300, end=400),  # Duplicate network
            ]
        )

        data = pa.record_batch([pa.array([1])], names=['id'])
        rb = ResponseBatch(data=data, metadata=metadata)

        networks = rb.networks
        assert len(networks) == 2
        assert set(networks) == {'ethereum', 'polygon'}


@pytest.mark.unit
class TestResponseBatchWithReorg:
    """Test ResponseBatchWithReorg factory methods and properties"""

    def test_data_batch_creation(self):
        """Test creating a data batch response"""
        data = pa.record_batch([pa.array([1])], names=['id'])
        metadata = BatchMetadata(ranges=[])
        batch = ResponseBatch(data=data, metadata=metadata)

        response = ResponseBatchWithReorg.data_batch(batch)

        assert response.batch_type == ResponseBatchType.DATA
        assert response.is_data == True
        assert response.is_reorg == False
        assert response.data == batch
        assert response.invalidation_ranges is None

    def test_reorg_batch_creation(self):
        """Test creating a reorg notification response"""
        ranges = [BlockRange(network='ethereum', start=100, end=200), BlockRange(network='polygon', start=50, end=150)]

        response = ResponseBatchWithReorg.reorg_batch(ranges)

        assert response.batch_type == ResponseBatchType.REORG
        assert response.is_data == False
        assert response.is_reorg == True
        assert response.data is None
        assert response.invalidation_ranges == ranges


@pytest.mark.unit
class TestResumeWatermark:
    """Test ResumeWatermark serialization"""

    def test_to_json_full_data(self):
        """Test serializing watermark with all fields"""
        watermark = ResumeWatermark(
            ranges=[
                BlockRange(network='ethereum', start=100, end=200),
                BlockRange(network='polygon', start=50, end=150),
            ],
            timestamp='2024-01-01T00:00:00Z',
            sequence=42,
        )

        json_str = watermark.to_json()
        data = json.loads(json_str)

        assert len(data['ranges']) == 2
        assert data['ranges'][0]['network'] == 'ethereum'
        assert data['timestamp'] == '2024-01-01T00:00:00Z'
        assert data['sequence'] == 42

    def test_to_json_minimal_data(self):
        """Test serializing watermark with only ranges"""
        watermark = ResumeWatermark(ranges=[BlockRange(network='ethereum', start=100, end=200)])

        json_str = watermark.to_json()
        data = json.loads(json_str)

        assert len(data['ranges']) == 1
        assert 'timestamp' not in data
        assert 'sequence' not in data

    def test_from_json_full_data(self):
        """Test deserializing watermark with all fields"""
        json_str = json.dumps(
            {
                'ranges': [
                    {'network': 'ethereum', 'start': 100, 'end': 200},
                    {'network': 'polygon', 'start': 50, 'end': 150},
                ],
                'timestamp': '2024-01-01T00:00:00Z',
                'sequence': 42,
            }
        )

        watermark = ResumeWatermark.from_json(json_str)

        assert len(watermark.ranges) == 2
        assert watermark.ranges[0].network == 'ethereum'
        assert watermark.timestamp == '2024-01-01T00:00:00Z'
        assert watermark.sequence == 42

    def test_round_trip_serialization(self):
        """Test that serialization round-trip preserves data"""
        original = ResumeWatermark(
            ranges=[
                BlockRange(network='ethereum', start=100, end=200),
                BlockRange(network='polygon', start=50, end=150),
            ],
            timestamp='2024-01-01T00:00:00Z',
            sequence=42,
        )

        json_str = original.to_json()
        restored = ResumeWatermark.from_json(json_str)

        assert len(restored.ranges) == len(original.ranges)
        assert restored.timestamp == original.timestamp
        assert restored.sequence == original.sequence


@pytest.mark.unit
class TestReorgDetection:
    """Test ReorgAwareStream._detect_reorg method"""

    def test_detect_reorg_no_previous_ranges(self):
        """Test reorg detection with no previous ranges"""

        # Create a minimal ReorgAwareStream instance
        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        current_ranges = [
            BlockRange(network='ethereum', start=100, end=200),
            BlockRange(network='polygon', start=50, end=150),
        ]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 0

    def test_detect_reorg_normal_progression(self):
        """Test no reorg detected with normal block progression"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        # Set up previous ranges
        stream.prev_ranges_by_network = {
            'ethereum': BlockRange(network='ethereum', start=100, end=200),
            'polygon': BlockRange(network='polygon', start=50, end=150),
        }

        # Current ranges continue where previous left off
        current_ranges = [
            BlockRange(network='ethereum', start=201, end=300),
            BlockRange(network='polygon', start=151, end=250),
        ]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 0

    def test_detect_reorg_overlap_detected(self):
        """Test reorg detection when ranges overlap"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        # Set up previous ranges
        stream.prev_ranges_by_network = {
            'ethereum': BlockRange(network='ethereum', start=100, end=200),
            'polygon': BlockRange(network='polygon', start=50, end=150),
        }

        # Current ranges start before previous ended (reorg!)
        current_ranges = [
            BlockRange(network='ethereum', start=180, end=280),  # Reorg
            BlockRange(network='polygon', start=151, end=250),  # Normal progression
        ]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 1
        assert invalidations[0].network == 'ethereum'
        assert invalidations[0].start == 180
        assert invalidations[0].end == 280  # max(280, 200)

    def test_detect_reorg_multiple_networks(self):
        """Test reorg detection across multiple networks"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        stream.prev_ranges_by_network = {
            'ethereum': BlockRange(network='ethereum', start=100, end=200),
            'polygon': BlockRange(network='polygon', start=50, end=150),
            'arbitrum': BlockRange(network='arbitrum', start=500, end=600),
        }

        # Multiple reorgs
        current_ranges = [
            BlockRange(network='ethereum', start=150, end=250),  # Reorg
            BlockRange(network='polygon', start=140, end=240),  # Reorg
            BlockRange(network='arbitrum', start=601, end=700),  # Normal
        ]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 2

        # Check ethereum reorg
        eth_inv = next(inv for inv in invalidations if inv.network == 'ethereum')
        assert eth_inv.start == 150
        assert eth_inv.end == 250

        # Check polygon reorg
        poly_inv = next(inv for inv in invalidations if inv.network == 'polygon')
        assert poly_inv.start == 140
        assert poly_inv.end == 240

    def test_detect_reorg_same_range_no_reorg(self):
        """Test that identical ranges don't trigger reorg"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        prev_range = BlockRange(network='ethereum', start=100, end=200)
        stream.prev_ranges_by_network = {'ethereum': prev_range}

        # Same range repeated
        current_ranges = [BlockRange(network='ethereum', start=100, end=200)]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 0

    def test_detect_reorg_extends_to_max_end(self):
        """Test that invalidation range extends to max of both ranges"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        stream.prev_ranges_by_network = {'ethereum': BlockRange(network='ethereum', start=100, end=300)}

        # Current range starts before previous but ends earlier
        current_ranges = [BlockRange(network='ethereum', start=250, end=280)]

        invalidations = stream._detect_reorg(current_ranges)

        assert len(invalidations) == 1
        assert invalidations[0].start == 250
        assert invalidations[0].end == 300  # max(280, 300)

    def test_is_duplicate_batch_all_same(self):
        """Test duplicate detection when all ranges are the same"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        # Set up previous ranges
        stream.prev_ranges_by_network = {
            'ethereum': BlockRange(network='ethereum', start=100, end=200),
            'polygon': BlockRange(network='polygon', start=50, end=150),
        }

        # Same ranges
        current_ranges = [
            BlockRange(network='ethereum', start=100, end=200),
            BlockRange(network='polygon', start=50, end=150),
        ]

        assert stream._is_duplicate_batch(current_ranges) == True

    def test_is_duplicate_batch_one_different(self):
        """Test duplicate detection when one range is different"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        stream.prev_ranges_by_network = {
            'ethereum': BlockRange(network='ethereum', start=100, end=200),
            'polygon': BlockRange(network='polygon', start=50, end=150),
        }

        # One range is different
        current_ranges = [
            BlockRange(network='ethereum', start=100, end=200),  # Same
            BlockRange(network='polygon', start=151, end=250),  # Different
        ]

        assert stream._is_duplicate_batch(current_ranges) == False

    def test_is_duplicate_batch_new_network(self):
        """Test duplicate detection with new network"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        stream.prev_ranges_by_network = {'ethereum': BlockRange(network='ethereum', start=100, end=200)}

        # Includes a new network
        current_ranges = [
            BlockRange(network='ethereum', start=100, end=200),
            BlockRange(network='polygon', start=50, end=150),  # New network
        ]

        assert stream._is_duplicate_batch(current_ranges) == False

    def test_is_duplicate_batch_empty_ranges(self):
        """Test duplicate detection with empty ranges"""

        class MockIterator:
            pass

        stream = ReorgAwareStream(MockIterator())

        assert stream._is_duplicate_batch([]) == False
