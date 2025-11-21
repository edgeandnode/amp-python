"""Unit tests for registry dataset inspection methods."""

import pytest

from amp.registry.datasets import RegistryDatasetsClient
from amp.utils.manifest_inspector import format_arrow_type


class MockRegistryClient:
    """Mock registry client for testing."""

    def __init__(self, manifest):
        self.manifest = manifest

    def _request(self, method, path, params=None):
        """Mock HTTP request."""

        class MockResponse:
            def json(self):
                return manifest

        return MockResponse()


# Sample manifest for testing
manifest = {
    'kind': 'manifest',
    'dependencies': {},
    'tables': {
        'blocks': {
            'schema': {
                'arrow': {
                    'fields': [
                        {'name': 'block_num', 'type': 'UInt64', 'nullable': False},
                        {'name': 'timestamp', 'type': {'Timestamp': ['Nanosecond', '+00:00']}, 'nullable': False},
                        {'name': 'hash', 'type': {'FixedSizeBinary': 32}, 'nullable': False},
                        {'name': 'base_fee_per_gas', 'type': {'Decimal128': [38, 0]}, 'nullable': True},
                    ]
                }
            },
        },
        'transactions': {
            'schema': {
                'arrow': {
                    'fields': [
                        {'name': 'tx_hash', 'type': {'FixedSizeBinary': 32}, 'nullable': False},
                        {'name': 'from', 'type': {'FixedSizeBinary': 20}, 'nullable': False},
                        {'name': 'value', 'type': {'Decimal128': [38, 0]}, 'nullable': True},
                    ]
                }
            },
        },
    },
}


@pytest.mark.unit
class TestDatasetInspection:
    """Test dataset inspection methods."""

    def test_format_arrow_type_primitive(self):
        """Test formatting primitive Arrow types."""
        assert format_arrow_type('UInt64') == 'UInt64'
        assert format_arrow_type('Binary') == 'Binary'
        assert format_arrow_type('Boolean') == 'Boolean'

    def test_format_arrow_type_timestamp(self):
        """Test formatting Timestamp types."""
        result = format_arrow_type({'Timestamp': ['Nanosecond', '+00:00']})
        assert result == 'Timestamp(Nanosecond)'

        result = format_arrow_type({'Timestamp': ['Microsecond', '+00:00']})
        assert result == 'Timestamp(Microsecond)'

    def test_format_arrow_type_fixed_binary(self):
        """Test formatting FixedSizeBinary types."""
        result = format_arrow_type({'FixedSizeBinary': 32})
        assert result == 'FixedSizeBinary(32)'

        result = format_arrow_type({'FixedSizeBinary': 20})
        assert result == 'FixedSizeBinary(20)'

    def test_format_arrow_type_decimal(self):
        """Test formatting Decimal128 types."""
        result = format_arrow_type({'Decimal128': [38, 0]})
        assert result == 'Decimal128(38,0)'

        result = format_arrow_type({'Decimal128': [18, 6]})
        assert result == 'Decimal128(18,6)'

    def test_describe_returns_correct_structure(self):
        """Test that describe returns the expected structure."""
        # Create mock client with test manifest
        mock_registry = MockRegistryClient(manifest)
        client = RegistryDatasetsClient(mock_registry)

        # Mock get_manifest to return our test manifest
        client.get_manifest = lambda ns, name, ver: manifest

        # Call describe
        schema = client.describe('test', 'dataset', 'latest')

        # Verify structure
        assert 'blocks' in schema
        assert 'transactions' in schema

        # Check blocks table
        blocks = schema['blocks']
        assert len(blocks) == 4
        assert blocks[0]['name'] == 'block_num'
        assert blocks[0]['type'] == 'UInt64'
        assert blocks[0]['nullable'] is False

        # Check formatted complex types
        assert blocks[1]['name'] == 'timestamp'
        assert blocks[1]['type'] == 'Timestamp(Nanosecond)'

        assert blocks[2]['name'] == 'hash'
        assert blocks[2]['type'] == 'FixedSizeBinary(32)'

        assert blocks[3]['name'] == 'base_fee_per_gas'
        assert blocks[3]['type'] == 'Decimal128(38,0)'
        assert blocks[3]['nullable'] is True

    def test_describe_handles_empty_manifest(self):
        """Test that describe handles manifests with no tables."""
        empty_manifest = {'kind': 'manifest', 'dependencies': {}, 'tables': {}}

        mock_registry = MockRegistryClient(empty_manifest)
        client = RegistryDatasetsClient(mock_registry)
        client.get_manifest = lambda ns, name, ver: empty_manifest

        schema = client.describe('test', 'dataset', 'latest')
        assert schema == {}

    def test_describe_handles_nullable_field(self):
        """Test that describe correctly identifies nullable fields."""
        mock_registry = MockRegistryClient(manifest)
        client = RegistryDatasetsClient(mock_registry)
        client.get_manifest = lambda ns, name, ver: manifest

        schema = client.describe('test', 'dataset', 'latest')

        # Check nullable fields
        transactions = schema['transactions']
        value_field = next(col for col in transactions if col['name'] == 'value')
        assert value_field['nullable'] is True

        from_field = next(col for col in transactions if col['name'] == 'from')
        assert from_field['nullable'] is False
