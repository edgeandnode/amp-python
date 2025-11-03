"""Tests for label joining functionality in base DataLoader"""

import tempfile
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pytest

from amp.config.label_manager import LabelManager
from amp.loaders.base import DataLoader


class MockLoader(DataLoader):
    """Mock loader for testing"""

    def __init__(self, config: Dict[str, Any], label_manager=None):
        super().__init__(config, label_manager=label_manager)

    def _parse_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override to just return the dict without parsing"""
        return config

    def connect(self) -> None:
        self._is_connected = True

    def disconnect(self) -> None:
        self._is_connected = False

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        return batch.num_rows


class TestLabelJoining:
    """Test label joining functionality"""

    @pytest.fixture
    def label_manager(self):
        """Create a label manager with test data"""
        # Create a temporary CSV file with token labels (valid 40-char Ethereum addresses)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('address,symbol,decimals\n')
            f.write('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,USDC,6\n')
            f.write('0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,WETH,18\n')
            f.write('0xcccccccccccccccccccccccccccccccccccccccc,DAI,18\n')
            csv_path = f.name

        try:
            manager = LabelManager()
            manager.add_label('tokens', csv_path)
            yield manager
        finally:
            Path(csv_path).unlink()

    def test_get_effective_schema(self, label_manager):
        """Test schema merging with label columns"""
        loader = MockLoader({}, label_manager=label_manager)

        # Original schema
        original_schema = pa.schema([('address', pa.string()), ('amount', pa.int64())])

        # Get effective schema with labels
        effective_schema = loader._get_effective_schema(original_schema, 'tokens', 'address')

        # Should have original columns plus label columns (excluding join key)
        assert 'address' in effective_schema.names
        assert 'amount' in effective_schema.names
        assert 'symbol' in effective_schema.names  # From label
        assert 'decimals' in effective_schema.names  # From label

        # Total: 2 original + 2 label columns (join key 'address' already in original) = 4
        assert len(effective_schema) == 4

    def test_get_effective_schema_no_labels(self, label_manager):
        """Test schema without labels returns original schema"""
        loader = MockLoader({}, label_manager=label_manager)

        original_schema = pa.schema([('address', pa.string()), ('amount', pa.int64())])

        # No label specified
        effective_schema = loader._get_effective_schema(original_schema, None, None)

        assert effective_schema == original_schema

    def test_join_with_labels(self, label_manager):
        """Test joining batch data with labels"""
        loader = MockLoader({}, label_manager=label_manager)

        # Create test batch with transfers (using full 40-char addresses)
        batch = pa.RecordBatch.from_pydict(
            {
                'address': [
                    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                    '0xffffffffffffffffffffffffffffffffffffffff',
                ],  # Last one doesn't exist in labels
                'amount': [100, 200, 300],
            }
        )

        # Join with labels (inner join should filter out 0xfff...)
        joined_batch = loader._join_with_labels(batch, 'tokens', 'address', 'address')

        # Should only have 2 rows (first two addresses, last one filtered out)
        assert joined_batch.num_rows == 2

        # Should have original columns plus label columns
        assert 'address' in joined_batch.schema.names
        assert 'amount' in joined_batch.schema.names
        assert 'symbol' in joined_batch.schema.names
        assert 'decimals' in joined_batch.schema.names

        # Verify joined data - after type conversion and join, addresses should be binary
        joined_dict = joined_batch.to_pydict()
        # Convert binary back to hex for comparison
        addresses_hex = [addr.hex() for addr in joined_dict['address']]
        assert addresses_hex == ['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb']
        assert joined_dict['amount'] == [100, 200]
        assert joined_dict['symbol'] == ['USDC', 'WETH']
        # Decimals are strings because we force all CSV columns to strings for type safety
        assert joined_dict['decimals'] == ['6', '18']

    def test_join_with_all_matching_keys(self, label_manager):
        """Test join when all keys match"""
        loader = MockLoader({}, label_manager=label_manager)

        batch = pa.RecordBatch.from_pydict(
            {
                'address': [
                    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                    '0xcccccccccccccccccccccccccccccccccccccccc',
                ],
                'amount': [100, 200, 300],
            }
        )

        joined_batch = loader._join_with_labels(batch, 'tokens', 'address', 'address')

        # All 3 rows should be present
        assert joined_batch.num_rows == 3

    def test_join_with_no_matching_keys(self, label_manager):
        """Test join when no keys match"""
        loader = MockLoader({}, label_manager=label_manager)

        batch = pa.RecordBatch.from_pydict(
            {
                'address': [
                    '0xdddddddddddddddddddddddddddddddddddddddd',
                    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                    '0xffffffffffffffffffffffffffffffffffffffff',
                ],
                'amount': [100, 200, 300],
            }
        )

        joined_batch = loader._join_with_labels(batch, 'tokens', 'address', 'address')

        # Should have 0 rows (all filtered out)
        assert joined_batch.num_rows == 0

    def test_join_invalid_label_name(self, label_manager):
        """Test join with non-existent label"""
        loader = MockLoader({}, label_manager=label_manager)

        batch = pa.RecordBatch.from_pydict({'address': ['0xA'], 'amount': [100]})

        with pytest.raises(ValueError, match="Label 'nonexistent' not found"):
            loader._join_with_labels(batch, 'nonexistent', 'address', 'address')

    def test_join_invalid_stream_key(self, label_manager):
        """Test join with invalid stream key column"""
        loader = MockLoader({}, label_manager=label_manager)

        batch = pa.RecordBatch.from_pydict({'address': ['0xA'], 'amount': [100]})

        with pytest.raises(ValueError, match="Stream key column 'nonexistent' not found"):
            loader._join_with_labels(batch, 'tokens', 'address', 'nonexistent')

    def test_join_invalid_label_key(self, label_manager):
        """Test join with invalid label key column"""
        loader = MockLoader({}, label_manager=label_manager)

        batch = pa.RecordBatch.from_pydict({'address': ['0xA'], 'amount': [100]})

        with pytest.raises(ValueError, match="Label key column 'nonexistent' not found"):
            loader._join_with_labels(batch, 'tokens', 'nonexistent', 'address')

    def test_join_no_label_manager(self):
        """Test join when label manager not configured"""
        loader = MockLoader({}, label_manager=None)

        batch = pa.RecordBatch.from_pydict({'address': ['0xA'], 'amount': [100]})

        with pytest.raises(ValueError, match='Label manager not configured'):
            loader._join_with_labels(batch, 'tokens', 'address', 'address')
