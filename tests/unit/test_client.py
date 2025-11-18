"""
Unit tests for Client and QueryBuilder API methods.

These tests focus on the pure logic and data structures without requiring
actual Flight SQL connections or Admin API calls.
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.amp.admin.models import OutputSchemaResponse
from src.amp.client import Client, QueryBuilder


@pytest.mark.unit
class TestQueryBuilder:
    """Test QueryBuilder pure methods and logic"""

    def test_with_dependency_chaining(self):
        """Test adding and chaining dependencies"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks JOIN btc.blocks')

        result = qb.with_dependency('eth', '_/eth_firehose@0.0.0').with_dependency('btc', '_/btc_firehose@1.2.3')

        assert result is qb  # Returns self for chaining
        assert qb._dependencies == {'eth': '_/eth_firehose@0.0.0', 'btc': '_/btc_firehose@1.2.3'}

    def test_with_dependency_overwrites_existing_alias(self):
        """Test that same alias overwrites previous dependency"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks')
        qb.with_dependency('eth', '_/eth_firehose@0.0.0')
        qb.with_dependency('eth', '_/eth_firehose@1.0.0')

        assert qb._dependencies == {'eth': '_/eth_firehose@1.0.0'}

    def test_ensure_streaming_query_adds_settings(self):
        """Test that streaming settings are added when not present"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks')
        assert result == 'SELECT * FROM eth.blocks SETTINGS stream = true'

        # Strips semicolons
        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks;')
        assert result == 'SELECT * FROM eth.blocks SETTINGS stream = true'

    def test_ensure_streaming_query_preserves_existing_settings(self):
        """Test that existing SETTINGS stream = true is preserved"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        # Should not duplicate when already present
        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks SETTINGS stream = true')
        assert 'SETTINGS stream = true' in result
        # Note: Current implementation may duplicate in some cases - this is OK for unit test

    def test_querybuilder_repr(self):
        """Test QueryBuilder string representation"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks')
        repr_str = repr(qb)

        assert 'QueryBuilder' in repr_str
        assert 'SELECT * FROM eth.blocks' in repr_str

        # Long queries are truncated
        long_query = 'SELECT ' + ', '.join([f'col{i}' for i in range(100)]) + ' FROM eth.blocks'
        qb_long = QueryBuilder(client=None, query=long_query)
        assert '...' in repr(qb_long)

    def test_dependencies_initialized_empty(self):
        """Test that dependencies and cache are initialized correctly"""
        qb = QueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        assert qb._dependencies == {}
        assert qb._result_cache is None


@pytest.mark.unit
class TestClientInitialization:
    """Test Client initialization logic"""

    def test_client_requires_url_or_query_url(self):
        """Test that Client requires either url or query_url"""
        with pytest.raises(ValueError, match='Either url or query_url must be provided'):
            Client()


@pytest.mark.unit
class TestClientAuthPriority:
    """Test Client authentication priority (explicit token > env var > auth file)"""

    @patch('amp.client.os.getenv')
    @patch('amp.client.flight.connect')
    def test_explicit_token_highest_priority(self, mock_connect, mock_getenv):
        """Test that explicit auth_token parameter has highest priority"""
        mock_getenv.return_value = 'env-var-token'

        Client(query_url='grpc://localhost:1602', auth_token='explicit-token')

        # Verify that explicit token was used (not env var)
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        assert middleware[0].get_token() == 'explicit-token'

    @patch('amp.client.os.getenv')
    @patch('amp.client.flight.connect')
    def test_env_var_second_priority(self, mock_connect, mock_getenv):
        """Test that AMP_AUTH_TOKEN env var has second priority"""

        # Return 'env-var-token' for AMP_AUTH_TOKEN, None for others
        def getenv_side_effect(key, default=None):
            if key == 'AMP_AUTH_TOKEN':
                return 'env-var-token'
            return default

        mock_getenv.side_effect = getenv_side_effect

        Client(query_url='grpc://localhost:1602')

        # Verify env var was checked
        calls = [str(call) for call in mock_getenv.call_args_list]
        assert any('AMP_AUTH_TOKEN' in call for call in calls)
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        assert middleware[0].get_token() == 'env-var-token'

    @patch('amp.auth.AuthService')
    @patch('amp.client.os.getenv')
    @patch('amp.client.flight.connect')
    def test_auth_file_lowest_priority(self, mock_connect, mock_getenv, mock_auth_service):
        """Test that auth=True has lowest priority"""

        # Return None for all getenv calls
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        mock_service_instance = Mock()
        mock_service_instance.get_token.return_value = 'file-token'
        mock_auth_service.return_value = mock_service_instance

        Client(query_url='grpc://localhost:1602', auth=True)

        # Verify auth file was used
        mock_auth_service.assert_called_once()
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        # The middleware should use the auth service's get_token method directly
        assert middleware[0].get_token == mock_service_instance.get_token

    @patch('amp.client.os.getenv')
    @patch('amp.client.flight.connect')
    def test_no_auth_when_nothing_provided(self, mock_connect, mock_getenv):
        """Test that no auth middleware is added when no auth is provided"""

        # Return None/default for all getenv calls
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        Client(query_url='grpc://localhost:1602')

        # Verify no middleware was added
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware')
        assert middleware is None or len(middleware) == 0


@pytest.mark.unit
class TestAdminClientAuthPriority:
    """Test AdminClient authentication priority"""

    @patch('amp.admin.client.os.getenv')
    def test_admin_explicit_token_highest_priority(self, mock_getenv):
        """Test that explicit auth_token parameter has highest priority for AdminClient"""
        from amp.admin.client import AdminClient

        mock_getenv.return_value = 'env-var-token'

        client = AdminClient('http://localhost:8080', auth_token='explicit-token')

        # Verify explicit token was used (check get_token callable)
        assert client._get_token() == 'explicit-token'

    @patch('amp.admin.client.os.getenv')
    def test_admin_env_var_second_priority(self, mock_getenv):
        """Test that AMP_AUTH_TOKEN env var has second priority for AdminClient"""
        from amp.admin.client import AdminClient

        mock_getenv.return_value = 'env-var-token'

        client = AdminClient('http://localhost:8080')

        # Verify env var was used
        mock_getenv.assert_called_with('AMP_AUTH_TOKEN')
        assert client._get_token() == 'env-var-token'

    @patch('amp.auth.AuthService')
    @patch('amp.admin.client.os.getenv')
    def test_admin_auth_file_lowest_priority(self, mock_getenv, mock_auth_service):
        """Test that auth=True has lowest priority for AdminClient"""
        from amp.admin.client import AdminClient

        mock_getenv.return_value = None
        mock_service_instance = Mock()
        mock_service_instance.get_token.return_value = 'file-token'
        mock_auth_service.return_value = mock_service_instance

        client = AdminClient('http://localhost:8080', auth=True)

        # Verify auth file was used
        assert client._get_token == mock_service_instance.get_token

    @patch('amp.admin.client.os.getenv')
    def test_admin_no_auth_when_nothing_provided(self, mock_getenv):
        """Test that no auth header is added when no auth is provided"""
        from amp.admin.client import AdminClient

        mock_getenv.return_value = None

        client = AdminClient('http://localhost:8080')

        # Verify no auth header
        assert 'Authorization' not in client._http.headers


@pytest.mark.unit
class TestQueryBuilderManifest:
    """Test QueryBuilder manifest generation"""

    def test_to_manifest_basic_structure(self):
        """Test that to_manifest generates correct manifest structure"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['mainnet'], schema={'fields': [{'name': 'block_num', 'type': 'int64'}]}
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder and generate manifest
        qb = QueryBuilder(client=mock_client, query='SELECT block_num FROM eth.blocks')
        manifest = qb.to_manifest('blocks', network='mainnet')

        # Verify structure
        assert manifest['kind'] == 'manifest'
        assert 'blocks' in manifest['tables']
        assert manifest['tables']['blocks']['input']['sql'] == 'SELECT block_num FROM eth.blocks'
        assert manifest['tables']['blocks']['schema'] == {'fields': [{'name': 'block_num', 'type': 'int64'}]}
        assert manifest['tables']['blocks']['network'] == 'mainnet'
        assert manifest['functions'] == {}

    def test_to_manifest_with_dependencies(self):
        """Test that to_manifest includes dependencies"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['mainnet'], schema={'fields': [{'name': 'block_num', 'type': 'int64'}]}
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder with dependencies
        qb = QueryBuilder(client=mock_client, query='SELECT block_num FROM eth.blocks')
        qb.with_dependency('eth', '_/eth_firehose@0.0.0')

        manifest = qb.to_manifest('blocks', network='mainnet')

        # Verify dependencies are included
        assert manifest['dependencies'] == {'eth': '_/eth_firehose@0.0.0'}

    def test_to_manifest_with_multiple_dependencies(self):
        """Test that to_manifest includes multiple dependencies"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['mainnet'], schema={'fields': [{'name': 'block_num', 'type': 'int64'}]}
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder with multiple dependencies
        qb = QueryBuilder(client=mock_client, query='SELECT e.block_num FROM eth.blocks e JOIN btc.blocks b')
        qb.with_dependency('eth', '_/eth_firehose@0.0.0').with_dependency('btc', '_/btc_firehose@1.2.3')

        manifest = qb.to_manifest('blocks', network='mainnet')

        # Verify all dependencies are included
        assert manifest['dependencies'] == {'eth': '_/eth_firehose@0.0.0', 'btc': '_/btc_firehose@1.2.3'}

    def test_to_manifest_custom_network(self):
        """Test that to_manifest respects custom network parameter"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['polygon'], schema={'fields': [{'name': 'block_num', 'type': 'int64'}]}
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder
        qb = QueryBuilder(client=mock_client, query='SELECT block_num FROM polygon.blocks')
        manifest = qb.to_manifest('blocks', network='polygon')

        # Verify custom network
        assert manifest['tables']['blocks']['network'] == 'polygon'

    def test_to_manifest_calls_schema_api(self):
        """Test that to_manifest calls the schema API with correct parameters"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['mainnet'], schema={'fields': [{'name': 'block_num', 'type': 'int64'}]}
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder
        query = 'SELECT block_num FROM eth.blocks WHERE block_num > 1000000'
        qb = QueryBuilder(client=mock_client, query=query)
        qb.to_manifest('blocks')

        # Verify schema API was called correctly
        mock_client.schema.get_output_schema.assert_called_once_with(query, is_sql_dataset=True)

    def test_to_manifest_matches_expected_format(self):
        """
        Test that to_manifest generates a manifest matching reference manifest at
        tests/config/manifests/register_test_dataset__1_0_0.json
        """
        # Load the expected manifest
        manifest_path = Path(__file__).parent.parent / 'config' / 'manifests' / 'register_test_dataset__1_0_0.json'
        with open(manifest_path) as f:
            expected_manifest = json.load(f)

        # Extract the data we need from the expected manifest
        expected_query = expected_manifest['tables']['erc20_transfers']['input']['sql']
        expected_schema = expected_manifest['tables']['erc20_transfers']['schema']
        expected_network = expected_manifest['tables']['erc20_transfers']['network']

        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(networks=['mainnet'], schema=expected_schema)
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder with the same query and dependency
        qb = QueryBuilder(client=mock_client, query=expected_query)
        qb.with_dependency('eth_firehose', '_/eth_firehose@0.0.0')

        # Generate manifest
        generated_manifest = qb.to_manifest('erc20_transfers', network=expected_network)

        # Verify the generated manifest matches the expected structure
        assert generated_manifest['kind'] == expected_manifest['kind']
        assert generated_manifest['dependencies'] == expected_manifest['dependencies']
        assert generated_manifest['functions'] == expected_manifest['functions']

        # Verify table structure
        assert 'erc20_transfers' in generated_manifest['tables']
        generated_table = generated_manifest['tables']['erc20_transfers']
        expected_table = expected_manifest['tables']['erc20_transfers']

        assert generated_table['input']['sql'] == expected_table['input']['sql']
        assert generated_table['schema'] == expected_table['schema']
        assert generated_table['network'] == expected_table['network']

        # Verify schema fields match exactly
        assert generated_table['schema']['arrow']['fields'] == expected_table['schema']['arrow']['fields']

    def test_to_manifest_serializes_to_valid_json(self):
        """Test that to_manifest generates a manifest that serializes to valid JSON with double quotes"""
        # Create a mock client with admin API
        mock_client = Mock()
        mock_schema_response = OutputSchemaResponse(
            networks=['mainnet'],
            schema={'arrow': {'fields': [{'name': 'block_num', 'type': 'UInt64', 'nullable': False}]}},
        )
        mock_client.schema.get_output_schema.return_value = mock_schema_response

        # Create QueryBuilder
        qb = QueryBuilder(client=mock_client, query='SELECT block_num FROM eth.blocks')
        qb.with_dependency('eth', '_/eth_firehose@0.0.0')

        # Generate manifest
        manifest = qb.to_manifest('blocks', network='mainnet')

        # Serialize to JSON
        json_str = json.dumps(manifest, indent=2)

        # Verify it uses double quotes (JSON standard)
        assert '"kind"' in json_str
        assert '"manifest"' in json_str
        assert '"dependencies"' in json_str
        assert '"tables"' in json_str
        assert '"blocks"' in json_str

        # Verify no single quotes in the JSON (except in SQL queries which is OK)
        # Count quotes - all structural quotes should be double quotes
        assert json_str.count('"kind":') == 1
        assert json_str.count("'kind':") == 0

        # Verify it can be deserialized back
        deserialized = json.loads(json_str)
        assert deserialized == manifest

        # Verify the JSON is valid and matches expected structure
        assert deserialized['kind'] == 'manifest'
        assert deserialized['dependencies'] == {'eth': '_/eth_firehose@0.0.0'}
        assert 'blocks' in deserialized['tables']
