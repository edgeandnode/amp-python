"""Unit tests for SchemaClient."""

from unittest.mock import Mock

import pytest
from amp.admin import models
from amp.admin.schema import SchemaClient


@pytest.mark.unit
class TestSchemaClient:
    """Test SchemaClient operations."""

    def test_get_output_schema_tables(self):
        """Test get_output_schema with tables."""
        mock_admin = Mock()
        client = SchemaClient(mock_admin)

        # Mock response
        mock_response = Mock()
        expected_response = {
            'schemas': {'t1': {'schema': {'fields': [{'name': 'col1', 'type': 'int64'}]}, 'networks': ['mainnet']}}
        }
        mock_response.json.return_value = expected_response
        mock_admin._request.return_value = mock_response

        # Call method
        response = client.get_output_schema(tables={'t1': 'SELECT * FROM t1'})

        # Verify request
        mock_admin._request.assert_called_once()
        call_kwargs = mock_admin._request.call_args[1]
        assert call_kwargs['json']['tables'] == {'t1': 'SELECT * FROM t1'}

        # Verify response
        assert isinstance(response, models.SchemaResponse)
        assert 't1' in response.schemas
        assert response.schemas['t1'].networks == ['mainnet']

    def test_get_output_schema_full_request(self):
        """Test get_output_schema with all parameters."""
        mock_admin = Mock()
        client = SchemaClient(mock_admin)

        mock_response = Mock()
        mock_response.json.return_value = {'schemas': {}}
        mock_admin._request.return_value = mock_response

        client.get_output_schema(
            tables={'t1': 'SELECT 1'}, dependencies={'d1': 'ns/d1@1.0.0'}, functions={'f1': {'body': '...'}}
        )

        # Verify request structure
        call_kwargs = mock_admin._request.call_args[1]
        request_json = call_kwargs['json']
        assert request_json['tables'] == {'t1': 'SELECT 1'}
        assert request_json['dependencies'] == {'d1': 'ns/d1@1.0.0'}
        assert request_json['functions'] == {'f1': {'body': '...'}}
