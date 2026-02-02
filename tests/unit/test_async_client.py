"""
Unit tests for AsyncAmpClient and AsyncQueryBuilder API methods.

These tests focus on the pure logic and data structures without requiring
actual Flight SQL connections or Admin API calls.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.amp.async_client import AsyncAmpClient, AsyncQueryBuilder


@pytest.mark.unit
class TestAsyncQueryBuilder:
    """Test AsyncQueryBuilder pure methods and logic"""

    def test_with_dependency_chaining(self):
        """Test adding and chaining dependencies"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks JOIN btc.blocks')

        result = qb.with_dependency('eth', '_/eth_firehose@0.0.0').with_dependency('btc', '_/btc_firehose@1.2.3')

        assert result is qb  # Returns self for chaining
        assert qb._dependencies == {'eth': '_/eth_firehose@0.0.0', 'btc': '_/btc_firehose@1.2.3'}

    def test_with_dependency_overwrites_existing_alias(self):
        """Test that same alias overwrites previous dependency"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks')
        qb.with_dependency('eth', '_/eth_firehose@0.0.0')
        qb.with_dependency('eth', '_/eth_firehose@1.0.0')

        assert qb._dependencies == {'eth': '_/eth_firehose@1.0.0'}

    def test_ensure_streaming_query_adds_settings(self):
        """Test that streaming settings are added when not present"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks')
        assert result == 'SELECT * FROM eth.blocks SETTINGS stream = true'

        # Strips semicolons
        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks;')
        assert result == 'SELECT * FROM eth.blocks SETTINGS stream = true'

    def test_ensure_streaming_query_preserves_existing_settings(self):
        """Test that existing SETTINGS stream = true is preserved"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        # Should not duplicate when already present
        result = qb._ensure_streaming_query('SELECT * FROM eth.blocks SETTINGS stream = true')
        assert 'SETTINGS stream = true' in result

    def test_querybuilder_repr(self):
        """Test AsyncQueryBuilder string representation"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks')
        repr_str = repr(qb)

        assert 'AsyncQueryBuilder' in repr_str
        assert 'SELECT * FROM eth.blocks' in repr_str

        # Long queries are truncated
        long_query = 'SELECT ' + ', '.join([f'col{i}' for i in range(100)]) + ' FROM eth.blocks'
        qb_long = AsyncQueryBuilder(client=None, query=long_query)
        assert '...' in repr(qb_long)

    def test_dependencies_initialized_empty(self):
        """Test that dependencies and cache are initialized correctly"""
        qb = AsyncQueryBuilder(client=None, query='SELECT * FROM eth.blocks')

        assert qb._dependencies == {}
        assert qb._result_cache is None


@pytest.mark.unit
class TestAsyncClientInitialization:
    """Test AsyncAmpClient initialization logic"""

    def test_client_requires_url_or_query_url(self):
        """Test that AsyncAmpClient requires either url or query_url"""
        with pytest.raises(ValueError, match='Either url or query_url must be provided'):
            AsyncAmpClient()


@pytest.mark.unit
class TestAsyncClientAuthPriority:
    """Test AsyncAmpClient authentication priority (explicit token > env var > auth file)"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_explicit_token_highest_priority(self, mock_connect, mock_getenv):
        """Test that explicit auth_token parameter has highest priority"""
        mock_getenv.return_value = 'env-var-token'

        AsyncAmpClient(query_url='grpc://localhost:1602', auth_token='explicit-token')

        # Verify that explicit token was used (not env var)
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        assert middleware[0].get_token() == 'explicit-token'

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_env_var_second_priority(self, mock_connect, mock_getenv):
        """Test that AMP_AUTH_TOKEN env var has second priority"""

        # Return 'env-var-token' for AMP_AUTH_TOKEN, None for others
        def getenv_side_effect(key, default=None):
            if key == 'AMP_AUTH_TOKEN':
                return 'env-var-token'
            return default

        mock_getenv.side_effect = getenv_side_effect

        AsyncAmpClient(query_url='grpc://localhost:1602')

        # Verify env var was checked
        calls = [str(call) for call in mock_getenv.call_args_list]
        assert any('AMP_AUTH_TOKEN' in call for call in calls)
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        assert middleware[0].get_token() == 'env-var-token'

    @patch('amp.auth.AuthService')
    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_auth_file_lowest_priority(self, mock_connect, mock_getenv, mock_auth_service):
        """Test that auth=True has lowest priority"""

        # Return None for all getenv calls
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        mock_service_instance = Mock()
        mock_service_instance.get_token.return_value = 'file-token'
        mock_auth_service.return_value = mock_service_instance

        AsyncAmpClient(query_url='grpc://localhost:1602', auth=True)

        # Verify auth file was used
        mock_auth_service.assert_called_once()
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware', [])
        assert len(middleware) == 1
        # The middleware should use the auth service's get_token method directly
        assert middleware[0].get_token == mock_service_instance.get_token

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_no_auth_when_nothing_provided(self, mock_connect, mock_getenv):
        """Test that no auth middleware is added when no auth is provided"""

        # Return None/default for all getenv calls
        def getenv_side_effect(key, default=None):
            return default

        mock_getenv.side_effect = getenv_side_effect

        AsyncAmpClient(query_url='grpc://localhost:1602')

        # Verify no middleware was added
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        middleware = call_args[1].get('middleware')
        assert middleware is None or len(middleware) == 0


@pytest.mark.unit
class TestAsyncClientSqlMethod:
    """Test AsyncAmpClient.sql() method"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_sql_returns_async_query_builder(self, mock_connect, mock_getenv):
        """Test that sql() returns an AsyncQueryBuilder instance"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')
        result = client.sql('SELECT * FROM eth.blocks')

        assert isinstance(result, AsyncQueryBuilder)
        assert result.query == 'SELECT * FROM eth.blocks'
        assert result.client is client


@pytest.mark.unit
class TestAsyncClientProperties:
    """Test AsyncAmpClient properties for Admin and Registry access"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_datasets_raises_without_admin_url(self, mock_connect, mock_getenv):
        """Test that datasets property raises when admin_url not provided"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')

        with pytest.raises(ValueError, match='Admin API not configured'):
            _ = client.datasets

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_jobs_raises_without_admin_url(self, mock_connect, mock_getenv):
        """Test that jobs property raises when admin_url not provided"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')

        with pytest.raises(ValueError, match='Admin API not configured'):
            _ = client.jobs

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_schema_raises_without_admin_url(self, mock_connect, mock_getenv):
        """Test that schema property raises when admin_url not provided"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')

        with pytest.raises(ValueError, match='Admin API not configured'):
            _ = client.schema

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_registry_raises_without_registry_url(self, mock_connect, mock_getenv):
        """Test that registry property raises when registry_url not provided"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602', registry_url=None)

        with pytest.raises(ValueError, match='Registry API not configured'):
            _ = client.registry


@pytest.mark.unit
class TestAsyncClientConfigurationMethods:
    """Test AsyncAmpClient configuration methods"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_configure_connection(self, mock_connect, mock_getenv):
        """Test that configure_connection stores connection config"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')
        client.configure_connection('test_conn', 'postgresql', {'host': 'localhost', 'database': 'test'})

        # Verify connection was stored in manager
        connections = client.list_connections()
        assert 'test_conn' in connections

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    def test_get_available_loaders(self, mock_connect, mock_getenv):
        """Test that get_available_loaders returns list of loaders"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')
        loaders = client.get_available_loaders()

        assert isinstance(loaders, list)
        # Should have at least postgresql and redis loaders
        assert 'postgresql' in loaders or len(loaders) > 0


@pytest.mark.unit
@pytest.mark.asyncio
class TestAsyncQueryBuilderLoad:
    """Test AsyncQueryBuilder.load() method"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    async def test_load_raises_for_parallel_config_without_stream(self, mock_connect, mock_getenv):
        """Test that load() raises error when parallel_config used without stream=True"""
        mock_getenv.return_value = None

        client = AsyncAmpClient(query_url='grpc://localhost:1602')
        qb = client.sql('SELECT * FROM eth.blocks')

        with pytest.raises(ValueError, match='parallel_config requires stream=True'):
            await qb.load(
                connection='test_conn',
                destination='test_table',
                parallel_config={'partitions': 4},
            )


@pytest.mark.unit
class TestAsyncClientContextManager:
    """Test AsyncAmpClient context manager support"""

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_connect, mock_getenv):
        """Test that AsyncAmpClient works as async context manager"""
        mock_getenv.return_value = None
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        async with AsyncAmpClient(query_url='grpc://localhost:1602') as client:
            assert client is not None
            assert client.conn is mock_conn

        # Verify connection was closed on exit
        mock_conn.close.assert_called_once()

    @patch('amp.async_client.os.getenv')
    @patch('amp.async_client.flight.connect')
    @pytest.mark.asyncio
    async def test_close_method(self, mock_connect, mock_getenv):
        """Test that close() properly closes all connections"""
        mock_getenv.return_value = None
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        client = AsyncAmpClient(query_url='grpc://localhost:1602')
        await client.close()

        mock_conn.close.assert_called_once()
