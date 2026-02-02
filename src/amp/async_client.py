"""Async Flight SQL client with data loading capabilities.

This module provides the AsyncAmpClient class for async operations
with the Flight SQL server and Admin/Registry APIs.

The async client is optimized for:
- Non-blocking HTTP API calls (Admin, Registry)
- Concurrent operations using asyncio
- Streaming data with async iteration

Note: Flight SQL (gRPC) operations currently remain synchronous as PyArrow's
Flight client doesn't have native async support. For streaming operations,
consider using run_in_executor or the sync Client.
"""

import asyncio
import logging
import os
from typing import AsyncIterator, Dict, Iterator, List, Optional, Union

import pyarrow as pa
from google.protobuf.any_pb2 import Any
from pyarrow import flight
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory

from . import FlightSql_pb2
from .config.connection_manager import ConnectionManager
from .config.label_manager import LabelManager
from .loaders.registry import create_loader, get_available_loaders
from .loaders.types import LabelJoinConfig, LoadConfig, LoadMode, LoadResult
from .streaming import (
    ReorgAwareStream,
    ResumeWatermark,
    StreamingResultIterator,
)


class AuthMiddleware(ClientMiddleware):
    """Flight middleware to add Bearer token authentication header."""

    def __init__(self, get_token):
        """Initialize auth middleware.

        Args:
            get_token: Callable that returns the current access token
        """
        self.get_token = get_token

    def sending_headers(self):
        """Add Authorization header to outgoing requests."""
        return {'authorization': f'Bearer {self.get_token()}'}


class AuthMiddlewareFactory(ClientMiddlewareFactory):
    """Factory for creating auth middleware instances."""

    def __init__(self, get_token):
        """Initialize auth middleware factory.

        Args:
            get_token: Callable that returns the current access token
        """
        self.get_token = get_token

    def start_call(self, info):
        """Create auth middleware for each call."""
        return AuthMiddleware(self.get_token)


class AsyncQueryBuilder:
    """Async chainable query builder for data loading operations.

    Provides async versions of query operations.
    """

    def __init__(self, client: 'AsyncAmpClient', query: str):
        self.client = client
        self.query = query
        self._result_cache = None
        self._dependencies: Dict[str, str] = {}
        self.logger = logging.getLogger(__name__)

    async def load(
        self,
        connection: str,
        destination: str,
        config: Dict[str, any] = None,
        label_config: Optional[LabelJoinConfig] = None,
        **kwargs,
    ) -> Union[LoadResult, AsyncIterator[LoadResult]]:
        """
        Async load query results to specified destination.

        Note: The actual data loading operations run synchronously in a thread
        pool executor since PyArrow Flight doesn't support native async.

        Args:
            connection: Named connection or connection name for auto-discovery
            destination: Target destination (table name, key, path, etc.)
            config: Inline configuration dict (alternative to connection)
            label_config: Optional LabelJoinConfig for joining with label data
            **kwargs: Additional loader-specific options

        Returns:
            LoadResult or async iterator of LoadResults
        """
        # Handle streaming mode
        if kwargs.get('stream', False):
            kwargs.pop('stream')
            streaming_query = self._ensure_streaming_query(self.query)
            return await self.client.query_and_load_streaming(
                query=streaming_query,
                destination=destination,
                connection_name=connection,
                config=config,
                label_config=label_config,
                **kwargs,
            )

        # Validate that parallel_config is only used with stream=True
        if kwargs.get('parallel_config'):
            raise ValueError('parallel_config requires stream=True')

        kwargs.setdefault('read_all', False)

        return await self.client.query_and_load(
            query=self.query,
            destination=destination,
            connection_name=connection,
            config=config,
            label_config=label_config,
            **kwargs,
        )

    def _ensure_streaming_query(self, query: str) -> str:
        """Ensure query has SETTINGS stream = true"""
        query = query.strip().rstrip(';')
        if 'SETTINGS stream = true' not in query.upper():
            query += ' SETTINGS stream = true'
        return query

    async def stream(self) -> AsyncIterator[pa.RecordBatch]:
        """Stream query results as Arrow batches asynchronously."""
        self.logger.debug(f'Starting async stream for query: {self.query[:50]}...')
        # Run synchronous Flight SQL operation in executor
        loop = asyncio.get_event_loop()
        batches = await loop.run_in_executor(None, lambda: list(self.client.get_sql_sync(self.query, read_all=False)))
        for batch in batches:
            yield batch

    async def to_arrow(self) -> pa.Table:
        """Get query results as Arrow table asynchronously."""
        if self._result_cache is None:
            self.logger.debug(f'Executing query for Arrow table: {self.query[:50]}...')
            loop = asyncio.get_event_loop()
            self._result_cache = await loop.run_in_executor(
                None, lambda: self.client.get_sql_sync(self.query, read_all=True)
            )
        return self._result_cache

    async def to_manifest(self, table_name: str, network: str = 'mainnet') -> dict:
        """Generate a dataset manifest from this query asynchronously.

        Automatically fetches the Arrow schema using the Admin API /schema endpoint.
        Requires the Client to be initialized with admin_url.

        Args:
            table_name: Name for the table in the manifest
            network: Network name (default: 'mainnet')

        Returns:
            Complete manifest dict ready for registration
        """
        # Get schema from Admin API
        schema_response = await self.client.schema.get_output_schema(self.query, is_sql_dataset=True)

        # Build manifest structure
        manifest = {
            'kind': 'manifest',
            'dependencies': self._dependencies,
            'tables': {
                table_name: {
                    'input': {'sql': self.query},
                    'schema': schema_response.schema_,
                    'network': network,
                }
            },
            'functions': {},
        }
        return manifest

    def with_dependency(self, alias: str, reference: str) -> 'AsyncQueryBuilder':
        """Add a dataset dependency for manifest generation."""
        self._dependencies[alias] = reference
        return self

    def __repr__(self):
        return f"AsyncQueryBuilder(query='{self.query[:50]}{'...' if len(self.query) > 50 else ''}')"


class AsyncAmpClient:
    """Async Flight SQL client with data loading capabilities.

    Supports both query operations (via Flight SQL) and optional admin operations
    (via async HTTP Admin API) and registry operations (via async Registry API).

    The Flight SQL operations are run in a thread pool executor since PyArrow's
    Flight client doesn't have native async support. HTTP operations (Admin,
    Registry) are fully async.

    Args:
        url: Flight SQL URL (for backward compatibility, treated as query_url)
        query_url: Query endpoint URL via Flight SQL (e.g., 'grpc://localhost:1602')
        admin_url: Optional Admin API URL (e.g., 'http://localhost:8080')
        registry_url: Optional Registry API URL (default: staging registry)
        auth_token: Optional Bearer token for authentication (highest priority)
        auth: If True, load auth token from ~/.amp/cache (shared with TS CLI)

    Authentication Priority (highest to lowest):
        1. Explicit auth_token parameter
        2. AMP_AUTH_TOKEN environment variable
        3. auth=True - reads from ~/.amp/cache/amp_cli_auth

    Example:
        >>> # Query with async admin operations
        >>> async with AsyncAmpClient(
        ...     query_url='grpc://localhost:1602',
        ...     admin_url='http://localhost:8080',
        ...     auth=True
        ... ) as client:
        ...     datasets = await client.datasets.list_all()
        ...     table = await client.sql("SELECT * FROM eth.blocks LIMIT 10").to_arrow()
    """

    def __init__(
        self,
        url: Optional[str] = None,
        query_url: Optional[str] = None,
        admin_url: Optional[str] = None,
        registry_url: str = 'https://api.registry.amp.staging.thegraph.com',
        auth_token: Optional[str] = None,
        auth: bool = False,
    ):
        # Backward compatibility: url parameter → query_url
        if url and not query_url:
            query_url = url

        # Resolve auth token provider with priority: explicit param > env var > auth file
        get_token = None
        if auth_token:
            def get_token():
                return auth_token
        elif os.getenv('AMP_AUTH_TOKEN'):
            env_token = os.getenv('AMP_AUTH_TOKEN')

            def get_token():
                return env_token
        elif auth:
            from amp.auth import AuthService

            auth_service = AuthService()
            get_token = auth_service.get_token

        # Initialize Flight SQL client
        if query_url:
            if get_token:
                middleware = [AuthMiddlewareFactory(get_token)]
                self.conn = flight.connect(query_url, middleware=middleware)
            else:
                self.conn = flight.connect(query_url)
        else:
            raise ValueError('Either url or query_url must be provided for Flight SQL connection')

        # Initialize managers
        self.connection_manager = ConnectionManager()
        self.label_manager = LabelManager()
        self.logger = logging.getLogger(__name__)

        # Store URLs and auth params for lazy initialization of async clients
        self._admin_url = admin_url
        self._registry_url = registry_url
        self._auth_token = auth_token
        self._auth = auth

        # Lazy-initialized async clients
        self._admin_client = None
        self._registry_client = None

    def sql(self, query: str) -> AsyncQueryBuilder:
        """
        Create an async chainable query builder.

        Args:
            query: SQL query string

        Returns:
            AsyncQueryBuilder instance for chaining operations
        """
        return AsyncQueryBuilder(self, query)

    def configure_connection(self, name: str, loader: str, config: Dict[str, any]) -> None:
        """Configure a named connection for reuse."""
        self.connection_manager.add_connection(name, loader, config)

    def configure_label(self, name: str, csv_path: str, binary_columns: Optional[List[str]] = None) -> None:
        """Configure a label dataset from a CSV file for joining with streaming data."""
        self.label_manager.add_label(name, csv_path, binary_columns)

    def list_connections(self) -> Dict[str, str]:
        """List all configured connections."""
        return self.connection_manager.list_connections()

    def get_available_loaders(self) -> List[str]:
        """Get list of available data loaders."""
        return get_available_loaders()

    # Async Admin API access (optional, requires admin_url)
    @property
    def datasets(self):
        """Access async datasets client for Admin API operations.

        Returns:
            AsyncDatasetsClient for dataset registration, deployment, and management

        Raises:
            ValueError: If admin_url was not provided during Client initialization
        """
        if not self._admin_url:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to AsyncAmpClient() '
                'to enable dataset management operations.'
            )
        if not self._admin_client:
            from amp.admin.async_client import AsyncAdminClient

            if self._auth:
                self._admin_client = AsyncAdminClient(self._admin_url, auth=True)
            elif self._auth_token or os.getenv('AMP_AUTH_TOKEN'):
                token = self._auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._admin_client = AsyncAdminClient(self._admin_url, auth_token=token)
            else:
                self._admin_client = AsyncAdminClient(self._admin_url)
        return self._admin_client.datasets

    @property
    def jobs(self):
        """Access async jobs client for Admin API operations.

        Returns:
            AsyncJobsClient for job monitoring and management

        Raises:
            ValueError: If admin_url was not provided during Client initialization
        """
        if not self._admin_url:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to AsyncAmpClient() '
                'to enable job monitoring operations.'
            )
        if not self._admin_client:
            from amp.admin.async_client import AsyncAdminClient

            if self._auth:
                self._admin_client = AsyncAdminClient(self._admin_url, auth=True)
            elif self._auth_token or os.getenv('AMP_AUTH_TOKEN'):
                token = self._auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._admin_client = AsyncAdminClient(self._admin_url, auth_token=token)
            else:
                self._admin_client = AsyncAdminClient(self._admin_url)
        return self._admin_client.jobs

    @property
    def schema(self):
        """Access async schema client for Admin API operations.

        Returns:
            AsyncSchemaClient for SQL query schema analysis

        Raises:
            ValueError: If admin_url was not provided during Client initialization
        """
        if not self._admin_url:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to AsyncAmpClient() '
                'to enable schema analysis operations.'
            )
        if not self._admin_client:
            from amp.admin.async_client import AsyncAdminClient

            if self._auth:
                self._admin_client = AsyncAdminClient(self._admin_url, auth=True)
            elif self._auth_token or os.getenv('AMP_AUTH_TOKEN'):
                token = self._auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._admin_client = AsyncAdminClient(self._admin_url, auth_token=token)
            else:
                self._admin_client = AsyncAdminClient(self._admin_url)
        return self._admin_client.schema

    @property
    def registry(self):
        """Access async registry client for Registry API operations.

        Returns:
            AsyncRegistryClient for dataset discovery, search, and publishing

        Raises:
            ValueError: If registry_url was not provided during Client initialization
        """
        if not self._registry_url:
            raise ValueError(
                'Registry API not configured. Provide registry_url parameter to AsyncAmpClient() '
                'to enable dataset discovery and search operations.'
            )
        if not self._registry_client:
            from amp.registry.async_client import AsyncRegistryClient

            if self._auth:
                self._registry_client = AsyncRegistryClient(self._registry_url, auth=True)
            elif self._auth_token or os.getenv('AMP_AUTH_TOKEN'):
                token = self._auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._registry_client = AsyncRegistryClient(self._registry_url, auth_token=token)
            else:
                self._registry_client = AsyncRegistryClient(self._registry_url)
        return self._registry_client

    # Synchronous Flight SQL methods (run in executor for async context)
    def get_sql_sync(self, query: str, read_all: bool = False):
        """Execute SQL query and return Arrow data (synchronous).

        This is the underlying synchronous method used by async wrappers.
        """
        command_query = FlightSql_pb2.CommandStatementQuery()
        command_query.query = query

        any_command = Any()
        any_command.Pack(command_query)
        cmd = any_command.SerializeToString()

        flight_descriptor = flight.FlightDescriptor.for_command(cmd)
        info = self.conn.get_flight_info(flight_descriptor)
        reader = self.conn.do_get(info.endpoints[0].ticket)

        if read_all:
            return reader.read_all()
        else:
            return self._batch_generator(reader)

    def _batch_generator(self, reader) -> Iterator[pa.RecordBatch]:
        """Generate batches from Flight reader."""
        while True:
            try:
                chunk = reader.read_chunk()
                yield chunk.data
            except StopIteration:
                break

    async def get_sql(self, query: str, read_all: bool = False):
        """Execute SQL query asynchronously and return Arrow data.

        Runs the synchronous Flight SQL operation in a thread pool executor.
        """
        loop = asyncio.get_event_loop()
        if read_all:
            return await loop.run_in_executor(None, lambda: self.get_sql_sync(query, read_all=True))
        else:
            batches = await loop.run_in_executor(None, lambda: list(self.get_sql_sync(query, read_all=False)))
            return batches

    async def query_and_load(
        self,
        query: str,
        destination: str,
        connection_name: str,
        config: Optional[Dict[str, any]] = None,
        label_config: Optional[LabelJoinConfig] = None,
        **kwargs,
    ) -> Union[LoadResult, AsyncIterator[LoadResult]]:
        """Execute query and load results directly into target system asynchronously.

        Runs the data loading operation in a thread pool executor.
        """
        loop = asyncio.get_event_loop()

        # Run the synchronous query_and_load in executor
        def sync_load():
            # Get connection configuration and determine loader type
            if connection_name:
                try:
                    connection_info = self.connection_manager.get_connection_info(connection_name)
                    loader_config = connection_info['config']
                    loader_type = connection_info['loader']
                except ValueError as e:
                    self.logger.error(f'Connection error: {e}')
                    raise
            elif config:
                loader_type = config.pop('loader_type', None)
                if not loader_type:
                    raise ValueError("When using inline config, 'loader_type' must be specified")
                loader_config = config
            else:
                raise ValueError('Either connection_name or config must be provided')

            # Extract load options
            read_all = kwargs.pop('read_all', False)
            load_config = LoadConfig(
                batch_size=kwargs.pop('batch_size', 10000),
                mode=LoadMode(kwargs.pop('mode', 'append')),
                create_table=kwargs.pop('create_table', True),
                schema_evolution=kwargs.pop('schema_evolution', False),
                **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']},
            )

            for key in ['max_retries', 'retry_delay']:
                kwargs.pop(key, None)

            loader_specific_kwargs = kwargs

            if read_all:
                table = self.get_sql_sync(query, read_all=True)
                return self._load_table_sync(
                    table,
                    loader_type,
                    destination,
                    loader_config,
                    load_config,
                    label_config=label_config,
                    **loader_specific_kwargs,
                )
            else:
                batch_stream = self.get_sql_sync(query, read_all=False)
                return list(
                    self._load_stream_sync(
                        batch_stream,
                        loader_type,
                        destination,
                        loader_config,
                        load_config,
                        label_config=label_config,
                        **loader_specific_kwargs,
                    )
                )

        return await loop.run_in_executor(None, sync_load)

    def _load_table_sync(
        self,
        table: pa.Table,
        loader: str,
        table_name: str,
        config: Dict[str, any],
        load_config: LoadConfig,
        **kwargs,
    ) -> LoadResult:
        """Load a complete Arrow Table synchronously."""
        try:
            loader_instance = create_loader(loader, config, label_manager=self.label_manager)

            with loader_instance:
                return loader_instance.load_table(table, table_name, **load_config.__dict__, **kwargs)
        except Exception as e:
            self.logger.error(f'Failed to load table: {e}')
            return LoadResult(
                rows_loaded=0,
                duration=0.0,
                ops_per_second=0.0,
                table_name=table_name,
                loader_type=loader,
                success=False,
                error=str(e),
            )

    def _load_stream_sync(
        self,
        batch_stream: Iterator[pa.RecordBatch],
        loader: str,
        table_name: str,
        config: Dict[str, any],
        load_config: LoadConfig,
        **kwargs,
    ) -> Iterator[LoadResult]:
        """Load from a stream of batches synchronously."""
        try:
            loader_instance = create_loader(loader, config, label_manager=self.label_manager)

            with loader_instance:
                yield from loader_instance.load_stream(batch_stream, table_name, **load_config.__dict__, **kwargs)
        except Exception as e:
            self.logger.error(f'Failed to load stream: {e}')
            yield LoadResult(
                rows_loaded=0,
                duration=0.0,
                ops_per_second=0.0,
                table_name=table_name,
                loader_type=loader,
                success=False,
                error=str(e),
            )

    async def query_and_load_streaming(
        self,
        query: str,
        destination: str,
        connection_name: str,
        config: Optional[Dict[str, any]] = None,
        label_config: Optional[LabelJoinConfig] = None,
        with_reorg_detection: bool = True,
        resume_watermark: Optional[ResumeWatermark] = None,
        **kwargs,
    ) -> AsyncIterator[LoadResult]:
        """Execute a streaming query and continuously load results asynchronously.

        Runs the streaming operation in a thread pool executor and yields results.
        """
        loop = asyncio.get_event_loop()

        # Run streaming query synchronously and collect results
        def sync_streaming():
            # Get connection configuration
            if connection_name:
                try:
                    connection_info = self.connection_manager.get_connection_info(connection_name)
                    loader_config = connection_info['config']
                    loader_type = connection_info['loader']
                except ValueError as e:
                    self.logger.error(f'Connection error: {e}')
                    raise
            elif config:
                loader_type = config.pop('loader_type', None)
                if not loader_type:
                    raise ValueError("When using inline config, 'loader_type' must be specified")
                loader_config = config
            else:
                raise ValueError('Either connection_name or config must be provided')

            # Extract load config
            load_config = LoadConfig(
                batch_size=kwargs.pop('batch_size', 10000),
                mode=LoadMode(kwargs.pop('mode', 'append')),
                create_table=kwargs.pop('create_table', True),
                schema_evolution=kwargs.pop('schema_evolution', False),
                **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']},
            )

            self.logger.info(f'Starting async streaming query to {loader_type}:{destination}')

            loader_instance = create_loader(loader_type, loader_config, label_manager=self.label_manager)

            results = []

            try:
                # Execute streaming query with Flight SQL
                command_query = FlightSql_pb2.CommandStatementQuery()
                command_query.query = query

                any_command = Any()
                any_command.Pack(command_query)
                cmd = any_command.SerializeToString()

                flight_descriptor = flight.FlightDescriptor.for_command(cmd)
                info = self.conn.get_flight_info(flight_descriptor)
                reader = self.conn.do_get(info.endpoints[0].ticket)

                stream_iterator = StreamingResultIterator(reader)

                if with_reorg_detection:
                    stream_iterator = ReorgAwareStream(stream_iterator)

                with loader_instance:
                    for result in loader_instance.load_stream_continuous(
                        stream_iterator, destination, connection_name=connection_name, **load_config.__dict__
                    ):
                        results.append(result)

            except Exception as e:
                self.logger.error(f'Streaming query failed: {e}')
                results.append(
                    LoadResult(
                        rows_loaded=0,
                        duration=0.0,
                        ops_per_second=0.0,
                        table_name=destination,
                        loader_type=loader_type,
                        success=False,
                        error=str(e),
                        metadata={'streaming_error': True},
                    )
                )

            return results

        results = await loop.run_in_executor(None, sync_streaming)
        for result in results:
            yield result

    async def close(self):
        """Close all connections and release resources."""
        # Close Flight SQL connection
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except Exception as e:
                self.logger.warning(f'Error closing Flight connection: {e}')

        # Close async admin client if initialized
        if self._admin_client:
            await self._admin_client.close()

        # Close async registry client if initialized
        if self._registry_client:
            await self._registry_client.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
