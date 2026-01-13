import logging
import os
from typing import Dict, Iterator, List, Optional, Union

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
    ParallelConfig,
    ParallelStreamExecutor,
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


class QueryBuilder:
    """Chainable query builder for data loading operations.

    Supports both data loading to various destinations and manifest generation
    for dataset registration via the Admin API.
    """

    def __init__(self, client: 'Client', query: str):
        self.client = client
        self.query = query
        self._result_cache = None
        self._dependencies: Dict[str, str] = {}  # For manifest generation
        self.logger = logging.getLogger(__name__)

    def load(
        self,
        connection: str,
        destination: str,
        config: Dict[str, Any] = None,
        label_config: Optional[LabelJoinConfig] = None,
        **kwargs,
    ) -> Union[LoadResult, Iterator[LoadResult]]:
        """
        Load query results to specified destination

        Args:
            loader: Name of the loader (e.g., 'postgresql', 'redis')
            destination: Target destination (table name, key, path, etc.)
            connection: Named connection or connection name for auto-discovery
            config: Inline configuration dict (alternative to connection)
            label_config: Optional LabelJoinConfig for joining with label data
            **kwargs: Additional loader-specific options including:
                - read_all: bool = False (if True, loads entire table at once; if False, streams batch by batch)
                - batch_size: int = 10000 (size of each batch for streaming)
                - stream: bool = False (if True, enables continuous streaming with reorg detection)
                - with_reorg_detection: bool = True (enable reorg detection for streaming queries)
                - resume_watermark: Optional[ResumeWatermark] = None (resume streaming from specific point)
                - label: str (deprecated, use label_config instead)
                - label_key_column: str (deprecated, use label_config instead)
                - stream_key_column: str (deprecated, use label_config instead)

        Returns:
            - If read_all=True: Single LoadResult with operation details
            - If read_all=False (default): Iterator of LoadResults, one per batch
            - If stream=True: Iterator of LoadResults with continuous streaming and reorg support
        """
        # Handle streaming mode
        if kwargs.get('stream', False):
            # Remove stream from kwargs to avoid passing it down
            kwargs.pop('stream')
            # Ensure query has streaming settings
            # TODO: Add validation that the specific query uses features supported by streaming
            streaming_query = self._ensure_streaming_query(self.query)
            return self.client.query_and_load_streaming(
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

        # Default to batch streaming (read_all=False) for memory efficiency
        kwargs.setdefault('read_all', False)

        return self.client.query_and_load(
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

    def stream(self) -> Iterator[pa.RecordBatch]:
        """Stream query results as Arrow batches"""
        self.logger.debug(f'Starting stream for query: {self.query[:50]}...')
        return self.client.get_sql(self.query, read_all=False)

    def to_arrow(self) -> pa.Table:
        """Get query results as Arrow table"""
        if self._result_cache is None:
            self.logger.debug(f'Executing query for Arrow table: {self.query[:50]}...')
            self._result_cache = self.client.get_sql(self.query, read_all=True)
        return self._result_cache

    def get_sql(self, read_all: bool = False):
        """Backward compatibility with existing method"""
        return self.client.get_sql(self.query, read_all=read_all)

    # Admin API manifest methods (require admin_url in Client)
    def with_dependency(self, alias: str, reference: str) -> 'QueryBuilder':
        """Add a dataset dependency for manifest generation.

        Use this to declare dependencies when generating manifests for derived datasets.
        The alias should match the dataset prefix used in your SQL query.

        Args:
            alias: Local alias used in SQL (e.g., 'eth' for 'eth.blocks')
            reference: Full dataset reference (e.g., '_/eth_firehose@0.0.0')

        Returns:
            Self for method chaining

        Example:
            >>> client.sql("SELECT block_num FROM eth.blocks WHERE block_num > 1000000") \\
            ...     .with_dependency("eth", "_/eth_firehose@0.0.0") \\
            ...     .to_manifest("recent_blocks")
        """
        self._dependencies[alias] = reference
        return self

    def to_manifest(self, table_name: str, network: str = 'mainnet') -> dict:
        """Generate a dataset manifest from this query.

        Automatically fetches the Arrow schema using the Admin API /schema endpoint.
        Requires the Client to be initialized with admin_url.

        Args:
            table_name: Name for the table in the manifest
            network: Network name (default: 'mainnet')

        Returns:
            Complete manifest dict ready for registration

        Raises:
            ValueError: If admin_url not configured in Client
            GetOutputSchemaError: If schema fetch fails

        Example:
            >>> manifest = client.sql("SELECT block_num, hash FROM eth.blocks") \\
            ...     .with_dependency("eth", "_/eth_firehose@0.0.0") \\
            ...     .to_manifest("blocks", network="mainnet")
            >>> print(manifest['kind'])
            'manifest'
        """
        # Get schema from Admin API
        schema_response = self.client.schema.get_output_schema(self.query, is_sql_dataset=True)

        # Build manifest structure matching tests/config/manifests/*.json format
        manifest = {
            'kind': 'manifest',
            'dependencies': self._dependencies,
            'tables': {
                table_name: {
                    'input': {'sql': self.query},
                    'schema': schema_response.schema_,  # Use schema_ field (schema is aliased in Pydantic)
                    'network': network,
                }
            },
            'functions': {},
        }
        return manifest

    def register_as(self, namespace: str, name: str, version: str, table_name: str, network: str = 'mainnet'):
        """Register this query as a new dataset.

        Generates manifest and registers with Admin API in one call.
        Returns a DeploymentContext for optional chained deployment.

        Args:
            namespace: Dataset namespace (e.g., '_')
            name: Dataset name
            version: Semantic version (e.g., '1.0.0')
            table_name: Table name in manifest
            network: Network name (default: 'mainnet')

        Returns:
            DeploymentContext for optional deployment

        Raises:
            ValueError: If admin_url not configured in Client
            InvalidManifestError: If manifest is invalid
            DependencyValidationError: If dependencies are invalid

        Example:
            >>> # Register and deploy in one chain
            >>> client.sql("SELECT block_num, hash FROM eth.blocks WHERE block_num > 18000000") \\
            ...     .with_dependency("eth", "_/eth_firehose@0.0.0") \\
            ...     .register_as("_", "recent_blocks", "1.0.0", "blocks") \\
            ...     .deploy(parallelism=4, wait=True)
        """
        from amp.admin.deployment import DeploymentContext

        # Generate manifest
        manifest = self.to_manifest(table_name, network)

        # Register with Admin API
        self.client.datasets.register(namespace, name, version, manifest)

        # Return deployment context for optional chaining
        return DeploymentContext(self.client, namespace, name, version)

    def __repr__(self):
        return f"QueryBuilder(query='{self.query[:50]}{'...' if len(self.query) > 50 else ''}')"


class Client:
    """Enhanced Flight SQL client with data loading capabilities.

    Supports both query operations (via Flight SQL) and optional admin operations
    (via HTTP Admin API) and registry operations (via Registry API).

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
        >>> # Query-only client (backward compatible)
        >>> client = Client(url='grpc://localhost:1602')
        >>>
        >>> # Client with amp auth from file
        >>> client = Client(
        ...     query_url='grpc://localhost:1602',
        ...     admin_url='http://localhost:8080',
        ...     auth=True
        ... )
        >>>
        >>> # Client with auth from environment variable
        >>> # export AMP_AUTH_TOKEN="eyJhbGci..."
        >>> client = Client(query_url='grpc://localhost:1602')
        >>>
        >>> # Client with registry support
        >>> client = Client(
        ...     query_url='grpc://localhost:1602',
        ...     admin_url='http://localhost:8080',
        ...     registry_url='https://api.registry.amp.staging.thegraph.com',
        ...     auth=True
        ... )
        >>> results = client.registry.datasets.search('ethereum')
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
            # Priority 1: Explicit auth_token parameter (static token)
            def get_token():
                return auth_token
        elif os.getenv('AMP_AUTH_TOKEN'):
            # Priority 2: AMP_AUTH_TOKEN environment variable (static token)
            env_token = os.getenv('AMP_AUTH_TOKEN')

            def get_token():
                return env_token
        elif auth:
            # Priority 3: Load from ~/.amp/cache/amp_cli_auth (auto-refreshing)
            from amp.auth import AuthService

            auth_service = AuthService()
            get_token = auth_service.get_token  # Callable that auto-refreshes

        # Initialize Flight SQL client
        if query_url:
            # Add auth middleware if token provider exists
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

        # Initialize optional Admin API client
        if admin_url:
            from amp.admin.client import AdminClient

            # Pass through auth parameters to AdminClient so it can set up its own auth
            # (AdminClient needs to manage its own AuthService for token refresh)
            if auth:
                # Use auth file - AdminClient will set up AuthService for auto-refresh
                self._admin_client = AdminClient(admin_url, auth=True)
            elif auth_token or os.getenv('AMP_AUTH_TOKEN'):
                # Use static token (explicit param takes priority)
                token = auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._admin_client = AdminClient(admin_url, auth_token=token)
            else:
                # No authentication
                self._admin_client = AdminClient(admin_url)
        else:
            self._admin_client = None

        # Initialize optional Registry API client
        if registry_url:
            from amp.registry import RegistryClient

            # Pass through auth parameters to RegistryClient so it can set up its own auth
            # (RegistryClient needs to manage its own AuthService for token refresh)
            if auth:
                # Use auth file - RegistryClient will set up AuthService for auto-refresh
                self._registry_client = RegistryClient(registry_url, auth=True)
            elif auth_token or os.getenv('AMP_AUTH_TOKEN'):
                # Use static token (explicit param takes priority)
                token = auth_token or os.getenv('AMP_AUTH_TOKEN')
                self._registry_client = RegistryClient(registry_url, auth_token=token)
            else:
                # No authentication
                self._registry_client = RegistryClient(registry_url)
        else:
            self._registry_client = None

    def sql(self, query: str) -> QueryBuilder:
        """
        Create a chainable query builder

        Args:
            query: SQL query string

        Returns:
            QueryBuilder instance for chaining operations
        """
        return QueryBuilder(self, query)

    def configure_connection(self, name: str, loader: str, config: Dict[str, Any]) -> None:
        """Configure a named connection for reuse"""
        self.connection_manager.add_connection(name, loader, config)

    def configure_label(self, name: str, csv_path: str, binary_columns: Optional[List[str]] = None) -> None:
        """
        Configure a label dataset from a CSV file for joining with streaming data.

        Args:
            name: Unique name for this label dataset
            csv_path: Path to the CSV file
            binary_columns: List of column names containing hex addresses to convert to binary.
                          If None, auto-detects columns with 'address' in the name.
        """
        self.label_manager.add_label(name, csv_path, binary_columns)

    def list_connections(self) -> Dict[str, str]:
        """List all configured connections"""
        return self.connection_manager.list_connections()

    def get_available_loaders(self) -> List[str]:
        """Get list of available data loaders"""
        return get_available_loaders()

    # Admin API access (optional, requires admin_url)
    @property
    def datasets(self):
        """Access datasets client for Admin API operations.

        Returns:
            DatasetsClient for dataset registration, deployment, and management

        Raises:
            ValueError: If admin_url was not provided during Client initialization

        Example:
            >>> client = Client(query_url='...', admin_url='http://localhost:8080')
            >>> datasets = client.datasets.list_all()
        """
        if not self._admin_client:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to Client() '
                'to enable dataset management operations.'
            )
        return self._admin_client.datasets

    @property
    def jobs(self):
        """Access jobs client for Admin API operations.

        Returns:
            JobsClient for job monitoring and management

        Raises:
            ValueError: If admin_url was not provided during Client initialization

        Example:
            >>> client = Client(query_url='...', admin_url='http://localhost:8080')
            >>> job = client.jobs.get(123)
        """
        if not self._admin_client:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to Client() to enable job monitoring operations.'
            )
        return self._admin_client.jobs

    @property
    def schema(self):
        """Access schema client for Admin API operations.

        Returns:
            SchemaClient for SQL query schema analysis

        Raises:
            ValueError: If admin_url was not provided during Client initialization

        Example:
            >>> client = Client(query_url='...', admin_url='http://localhost:8080')
            >>> schema_resp = client.schema.get_output_schema('SELECT * FROM eth.blocks', True)
        """
        if not self._admin_client:
            raise ValueError(
                'Admin API not configured. Provide admin_url parameter to Client() '
                'to enable schema analysis operations.'
            )
        return self._admin_client.schema

    @property
    def registry(self):
        """Access registry client for Registry API operations.

        Returns:
            RegistryClient for dataset discovery, search, and publishing

        Raises:
            ValueError: If registry_url was not provided during Client initialization

        Example:
            >>> client = Client(
            ...     query_url='grpc://localhost:1602',
            ...     registry_url='https://api.registry.amp.staging.thegraph.com'
            ... )
            >>> # Search for datasets
            >>> results = client.registry.datasets.search('ethereum blocks')
            >>> # Get a specific dataset
            >>> dataset = client.registry.datasets.get('edgeandnode', 'ethereum-mainnet')
            >>> # Fetch manifest
            >>> manifest = client.registry.datasets.get_manifest('edgeandnode', 'ethereum-mainnet', 'latest')
        """
        if not self._registry_client:
            raise ValueError(
                'Registry API not configured. Provide registry_url parameter to Client() '
                'to enable dataset discovery and search operations.'
            )
        return self._registry_client

    # Existing methods for backward compatibility
    def get_sql(self, query, read_all=False):
        """Execute SQL query and return Arrow data"""
        # Create a CommandStatementQuery message
        command_query = FlightSql_pb2.CommandStatementQuery()
        command_query.query = query

        # Wrap the CommandStatementQuery in an Any type
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

    def _batch_generator(self, reader):
        """Generate batches from Flight reader"""
        while True:
            try:
                chunk = reader.read_chunk()
                yield chunk.data
            except StopIteration:
                break

    def query_and_load(
        self,
        query: str,
        destination: str,
        connection_name: str,
        config: Optional[Dict[str, Any]] = None,
        label_config: Optional[LabelJoinConfig] = None,
        **kwargs,
    ) -> Union[LoadResult, Iterator[LoadResult]]:
        """
        Execute query and load results directly into target system

        Args:
            query: SQL query to execute
            destination: Target destination name (table name, key, path, etc.)
            connection_name: Named connection (which specifies both loader type and config)
            config: Inline configuration dict (alternative to named connection)
            **kwargs: Additional load options including:
                - read_all: bool = False (default streams batch by batch for memory efficiency)
                - batch_size: int = 10000 (size of each batch for streaming)
                - mode: str = 'append' (loading mode)
                - create_table: bool = True (whether to create target table)

        Returns:
            - If read_all=False (default): Iterator of LoadResults, one per batch (memory efficient)
            - If read_all=True: Single LoadResult with operation details (loads entire table)
        """
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
            # For inline config, we need to determine the loader type
            # This is a fallback for advanced usage
            loader_type = config.pop('loader_type', None)
            if not loader_type:
                raise ValueError("When using inline config, 'loader_type' must be specified")
            loader_config = config
        else:
            raise ValueError('Either connection_name or config must be provided')

        # Extract load options from kwargs - streaming is now the default
        read_all = kwargs.pop('read_all', False)  # Default to streaming
        load_config = LoadConfig(
            batch_size=kwargs.pop('batch_size', 10000),
            mode=LoadMode(kwargs.pop('mode', 'append')),
            create_table=kwargs.pop('create_table', True),
            schema_evolution=kwargs.pop('schema_evolution', False),
            **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']},
        )

        # Remove known LoadConfig params from kwargs, leaving loader-specific params
        for key in ['max_retries', 'retry_delay']:
            kwargs.pop(key, None)

        # Remaining kwargs are loader-specific (e.g., channel_suffix for Snowflake)
        loader_specific_kwargs = kwargs

        if read_all:
            self.logger.info(f'Loading entire query result to {loader_type}:{destination}')
        else:
            self.logger.info(
                f'Streaming query results to {loader_type}:{destination} (batch_size={load_config.batch_size})'
            )

        # Get the data and load
        if read_all:
            table = self.get_sql(query, read_all=True)
            return self._load_table(
                table,
                loader_type,
                destination,
                loader_config,
                load_config,
                label_config=label_config,
                **loader_specific_kwargs,
            )
        else:
            batch_stream = self.get_sql(query, read_all=False)
            return self._load_stream(
                batch_stream,
                loader_type,
                destination,
                loader_config,
                load_config,
                label_config=label_config,
                **loader_specific_kwargs,
            )

    def _load_table(
        self, table: pa.Table, loader: str, table_name: str, config: Dict[str, Any], load_config: LoadConfig, **kwargs
    ) -> LoadResult:
        """Load a complete Arrow Table"""
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

    def _load_stream(
        self,
        batch_stream: Iterator[pa.RecordBatch],
        loader: str,
        table_name: str,
        config: Dict[str, Any],
        load_config: LoadConfig,
        **kwargs,
    ) -> Iterator[LoadResult]:
        """Load from a stream of batches"""
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

    def query_and_load_streaming(
        self,
        query: str,
        destination: str,
        connection_name: str,
        config: Optional[Dict[str, Any]] = None,
        label_config: Optional[LabelJoinConfig] = None,
        with_reorg_detection: bool = True,
        resume_watermark: Optional[ResumeWatermark] = None,
        parallel_config: Optional[ParallelConfig] = None,
        **kwargs,
    ) -> Iterator[LoadResult]:
        """
        Execute a streaming query and continuously load results into target system.

        Args:
            query: SQL query with 'SETTINGS stream = true'
            destination: Target destination name (table name, key, path, etc.)
            connection_name: Named connection (which specifies both loader type and config)
            config: Inline configuration dict (alternative to named connection)
            with_reorg_detection: Enable blockchain reorganization detection (default: True)
            resume_watermark: Optional watermark to resume streaming from a specific point
            parallel_config: Configuration for parallel execution (enables parallel mode if provided)
            **kwargs: Additional load options

        Returns:
            Iterator of LoadResults, including both data loads and reorg events

        Yields:
            LoadResult for each batch loaded or reorg event detected
        """
        # Handle parallel streaming mode (enabled by presence of parallel_config)
        if parallel_config:
            executor = ParallelStreamExecutor(self, parallel_config)

            load_config_dict = {
                'batch_size': kwargs.pop('batch_size', 10000),
                'mode': kwargs.pop('mode', 'append'),
                'create_table': kwargs.pop('create_table', True),
                'schema_evolution': kwargs.pop('schema_evolution', False),
                **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']},
            }

            # Add label_config if provided
            if label_config:
                load_config_dict['label_config'] = label_config

            yield from executor.execute_parallel_stream(query, destination, connection_name, load_config_dict)
            return

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

        # Extract load config
        load_config = LoadConfig(
            batch_size=kwargs.pop('batch_size', 10000),
            mode=LoadMode(kwargs.pop('mode', 'append')),
            create_table=kwargs.pop('create_table', True),
            schema_evolution=kwargs.pop('schema_evolution', False),
            **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']},
        )

        self.logger.info(f'Starting streaming query to {loader_type}:{destination}')

        # Create loader instance early to access checkpoint store
        loader_instance = create_loader(loader_type, loader_config, label_manager=self.label_manager)

        # Load checkpoint and create resume watermark if enabled (default: enabled)
        if resume_watermark is None and kwargs.get('resume', True):
            try:
                checkpoint = loader_instance.checkpoint_store.load(connection_name, destination)

                if checkpoint:
                    resume_watermark = checkpoint.to_resume_watermark()
                    checkpoint_type = 'reorg checkpoint' if checkpoint.is_reorg else 'checkpoint'
                    self.logger.info(
                        f'Resuming from {checkpoint_type}: {len(checkpoint.ranges)} ranges, '
                        f'timestamp {checkpoint.timestamp}'
                    )
                    if checkpoint.is_reorg:
                        resume_points = ', '.join(f'{r.network}:{r.start}' for r in checkpoint.ranges)
                        self.logger.info(f'Reorg resume points: {resume_points}')
            except Exception as e:
                self.logger.warning(f'Failed to load checkpoint, starting from beginning: {e}')

        try:
            # Execute streaming query with Flight SQL
            # Create a CommandStatementQuery message
            command_query = FlightSql_pb2.CommandStatementQuery()
            command_query.query = query

            # Add resume watermark if provided
            if resume_watermark:
                # TODO: Add watermark to query metadata when Flight SQL supports it
                self.logger.info(f'Resuming stream from watermark: {resume_watermark}')

            # Wrap the CommandStatementQuery in an Any type
            any_command = Any()
            any_command.Pack(command_query)
            cmd = any_command.SerializeToString()

            self.logger.info('Establishing Flight SQL connection...')
            flight_descriptor = flight.FlightDescriptor.for_command(cmd)
            info = self.conn.get_flight_info(flight_descriptor)
            reader = self.conn.do_get(info.endpoints[0].ticket)

            # Create streaming iterator
            stream_iterator = StreamingResultIterator(reader)
            self.logger.info('Stream connection established, waiting for data...')

            # Optionally wrap with reorg detection
            if with_reorg_detection:
                stream_iterator = ReorgAwareStream(stream_iterator, resume_watermark=resume_watermark)
                self.logger.info('Reorg detection enabled for streaming query')

            # Start continuous loading with checkpoint support
            with loader_instance:
                self.logger.info(f'Starting continuous load to {destination}. Press Ctrl+C to stop.')
                # Pass connection_name for checkpoint saving
                yield from loader_instance.load_stream_continuous(
                    stream_iterator, destination, connection_name=connection_name, **load_config.__dict__
                )

        except Exception as e:
            self.logger.error(f'Streaming query failed: {e}')
            yield LoadResult(
                rows_loaded=0,
                duration=0.0,
                ops_per_second=0.0,
                table_name=destination,
                loader_type=loader_type,
                success=False,
                error=str(e),
                metadata={'streaming_error': True},
            )
