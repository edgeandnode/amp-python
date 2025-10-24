import io
import threading
import time
import uuid
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.csv as pa_csv
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

try:
    import pandas as pd
except ImportError:
    pd = None  # pandas is optional, only needed for pandas loading method

from ...streaming.types import BlockRange
from ..base import DataLoader, LoadMode


@dataclass
class SnowflakeConnectionConfig:
    """Configuration for Snowflake connection with required and optional parameters"""

    account: str
    user: str
    warehouse: str
    database: str
    password: Optional[str] = None  # Optional - required only for password auth
    schema: str = 'PUBLIC'
    role: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[Any] = None
    private_key_passphrase: Optional[str] = None
    token: Optional[str] = None
    okta_account_name: Optional[str] = None
    login_timeout: int = 60
    network_timeout: int = 300
    socket_timeout: int = 300
    ocsp_response_cache_filename: Optional[str] = None
    validate_default_parameters: bool = True
    paramstyle: str = 'qmark'
    timezone: Optional[str] = None
    connection_params: Dict[str, Any] = None

    # Loading method configuration
    loading_method: str = 'stage'  # 'stage', 'insert', 'pandas', or 'snowpipe_streaming'

    # Connection pooling configuration
    use_connection_pool: bool = True
    pool_size: int = 5

    # Pandas loading specific options
    pandas_compression: str = 'gzip'  # Compression for pandas staging files ('gzip', 'snappy', or 'none')
    pandas_parallel_threads: int = 4  # Number of parallel threads for pandas uploads

    # Snowpipe Streaming specific options
    streaming_channel_prefix: str = 'amp'
    streaming_max_retries: int = 3
    streaming_buffer_flush_interval: int = 1

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}

        # Parse private key if it's a PEM string
        # The Snowflake connector requires a cryptography key object, not a string
        if self.private_key and isinstance(self.private_key, str):
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            try:
                pem_bytes = self.private_key.encode('utf-8')
                if self.private_key_passphrase:
                    passphrase = self.private_key_passphrase.encode('utf-8')
                    self.private_key = serialization.load_pem_private_key(
                        pem_bytes, password=passphrase, backend=default_backend()
                    )
                else:
                    self.private_key = serialization.load_pem_private_key(
                        pem_bytes, password=None, backend=default_backend()
                    )
            except Exception as e:
                raise ValueError(
                    f'Failed to parse private key: {e}. '
                    'Ensure the key is in PKCS#8 PEM format (unencrypted or with passphrase).'
                ) from e


class SnowflakeConnectionPool:
    """
    Thread-safe connection pool for Snowflake connections.

    Manages a pool of reusable Snowflake connections to avoid the overhead
    of creating new connections for each parallel worker.

    Features:
    - Connection health validation before reuse
    - Automatic connection refresh when stale
    - Connection age tracking to prevent credential expiration
    """

    _pools: Dict[str, 'SnowflakeConnectionPool'] = {}
    _pools_lock = threading.Lock()

    # Connection lifecycle settings
    MAX_CONNECTION_AGE = 3600  # Max age in seconds (1 hour) before refresh
    CONNECTION_VALIDATION_TIMEOUT = 5  # Seconds to wait for validation query

    def __init__(self, config: SnowflakeConnectionConfig, pool_size: int = 5):
        """
        Initialize connection pool.

        Args:
            config: Snowflake connection configuration
            pool_size: Maximum number of connections in the pool
        """
        self.config = config
        self.pool_size = pool_size
        self._pool: Queue[tuple[SnowflakeConnection, float]] = Queue(maxsize=pool_size)  # (connection, created_at)
        self._active_connections = 0
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def get_pool(cls, config: SnowflakeConnectionConfig, pool_size: int = 5) -> 'SnowflakeConnectionPool':
        """
        Get or create a connection pool for the given configuration.

        Uses connection config as key to share pools across loader instances
        with the same configuration.
        """
        # Create a hashable key from config
        key = f"{config.account}:{config.user}:{config.database}:{config.schema}"

        with cls._pools_lock:
            if key not in cls._pools:
                cls._pools[key] = SnowflakeConnectionPool(config, pool_size)
            return cls._pools[key]

    def _validate_connection(self, connection: SnowflakeConnection) -> bool:
        """
        Validate that a connection is still healthy and responsive.

        Args:
            connection: The connection to validate

        Returns:
            True if connection is healthy, False otherwise
        """
        if connection.is_closed():
            return False

        try:
            # Execute a simple query with timeout to verify connection is responsive
            cursor = connection.cursor()
            cursor.execute("SELECT 1", timeout=self.CONNECTION_VALIDATION_TIMEOUT)
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            # Any error means connection is not healthy
            return False

    def _create_connection(self) -> SnowflakeConnection:
        """Create a new Snowflake connection"""
        # Set defaults for connection parameters
        # Increase timeouts for long-running operations (pandas loading, large datasets)
        default_params = {
            'login_timeout': 60,
            'network_timeout': 600,  # Increased from 300 to 600 (10 minutes)
            'socket_timeout': 600,  # Increased from 300 to 600 (10 minutes)
            'validate_default_parameters': True,
            'paramstyle': 'qmark',
        }

        # Build connection parameters
        conn_params = {
            'account': self.config.account,
            'user': self.config.user,
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema,
            **default_params,
            **self.config.connection_params,
        }

        # Add authentication parameters
        if self.config.authenticator:
            conn_params['authenticator'] = self.config.authenticator
            if self.config.authenticator == 'oauth':
                conn_params['token'] = self.config.token
            elif self.config.authenticator == 'okta' and self.config.okta_account_name:
                conn_params['authenticator'] = f'https://{self.config.okta_account_name}.okta.com'
        elif self.config.private_key:
            conn_params['private_key'] = self.config.private_key
            if self.config.private_key_passphrase:
                conn_params['private_key_passphrase'] = self.config.private_key_passphrase
        else:
            conn_params['password'] = self.config.password

        # Optional parameters
        if self.config.role:
            conn_params['role'] = self.config.role

        return snowflake.connector.connect(**conn_params)

    def acquire(self, timeout: Optional[float] = 30.0) -> SnowflakeConnection:
        """
        Acquire a connection from the pool with health validation.

        Args:
            timeout: Maximum time to wait for a connection (seconds)

        Returns:
            A healthy Snowflake connection

        Raises:
            RuntimeError: If pool is closed or timeout exceeded
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        try:
            # Try to get an existing connection from the pool
            connection, created_at = self._pool.get(block=False)
            connection_age = time.time() - created_at

            # Check if connection is too old or unhealthy
            if connection_age > self.MAX_CONNECTION_AGE:
                # Connection too old, close and create new one
                try:
                    connection.close()
                except Exception:
                    pass
                with self._lock:
                    self._active_connections -= 1
                # Create new connection below
            elif self._validate_connection(connection):
                # Connection is healthy, return it
                return connection
            else:
                # Connection unhealthy, close and create new one
                try:
                    connection.close()
                except Exception:
                    pass
                with self._lock:
                    self._active_connections -= 1
                # Create new connection below

        except Empty:
            # No connections available in pool
            pass

        # Create new connection if under pool size limit
        with self._lock:
            if self._active_connections < self.pool_size:
                connection = self._create_connection()
                self._active_connections += 1
                return connection

        # Pool is at capacity, wait for a connection to be released
        try:
            connection, created_at = self._pool.get(block=True, timeout=timeout)
            connection_age = time.time() - created_at

            # Validate the connection we got
            if connection_age > self.MAX_CONNECTION_AGE or not self._validate_connection(connection):
                # Connection too old or unhealthy, create new one
                try:
                    connection.close()
                except Exception:
                    pass
                with self._lock:
                    self._active_connections -= 1
                connection = self._create_connection()
                with self._lock:
                    self._active_connections += 1

            return connection
        except Empty:
            raise RuntimeError(f"Failed to acquire connection from pool within {timeout}s")

    def release(self, connection: SnowflakeConnection) -> None:
        """
        Release a connection back to the pool with updated timestamp.

        Args:
            connection: The connection to release
        """
        if self._closed:
            # Pool is closed, close the connection
            try:
                connection.close()
            except Exception:
                pass
            return

        # Return connection to pool if it's still open
        # Store current time as "created_at" - connection stays fresh when actively used
        if not connection.is_closed():
            try:
                self._pool.put((connection, time.time()), block=False)
            except Exception:
                # Pool is full, close the connection
                try:
                    connection.close()
                except Exception:
                    pass
                with self._lock:
                    self._active_connections -= 1
        else:
            # Connection is closed, decrement counter
            with self._lock:
                self._active_connections -= 1

    def close(self) -> None:
        """Close all connections in the pool"""
        self._closed = True

        # Close all connections in the queue
        while not self._pool.empty():
            try:
                connection, _ = self._pool.get(block=False)  # Unpack tuple
                connection.close()
            except Exception:
                pass

        with self._lock:
            self._active_connections = 0


class SnowflakeLoader(DataLoader[SnowflakeConnectionConfig]):
    """
    Snowflake data loader optimized for bulk loading operations.

    Features:
    - Zero-copy operations using COPY INTO
    - Efficient data staging through internal stages
    - Support for various authentication methods
    - Automatic schema creation
    - Comprehensive error handling
    - Support for all Snowflake data types
    """

    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND}  # Snowflake loader only supports APPEND
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        super().__init__(config, label_manager=label_manager)
        self.connection: Optional[SnowflakeConnection] = None
        self.cursor = None
        self._created_tables = set()  # Track created tables
        self._connection_pool: Optional[SnowflakeConnectionPool] = None
        self._owns_connection = False  # Track if we own the connection or got it from pool
        self._worker_id = str(uuid.uuid4())[:8]  # Unique identifier for this loader instance

        # Loading configuration
        self.stage_name = config.get('stage_name', 'amp_STAGE')
        self.compression = config.get('compression', 'gzip')

        # Connection pooling configuration (use config object values)
        self.use_connection_pool = self.config.use_connection_pool
        self.pool_size = self.config.pool_size

        # Determine loading method from config
        self.loading_method = self.config.loading_method

        # Snowpipe Streaming clients and channels (one client per table)
        self.streaming_clients: Dict[str, Any] = {}  # table_name -> StreamingIngestClient
        self.streaming_channels: Dict[str, Any] = {}  # table_name:channel_name -> channel

    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['account', 'user', 'warehouse', 'database']

    def connect(self) -> None:
        """Establish connection to Snowflake using connection pool if enabled"""
        try:
            if self.use_connection_pool:
                # Get or create connection pool
                self._connection_pool = SnowflakeConnectionPool.get_pool(self.config, self.pool_size)

                # Acquire a connection from the pool
                self.connection = self._connection_pool.acquire()
                self._owns_connection = False  # Pool owns the connection

                self.logger.info(f'Acquired connection from pool (worker {self._worker_id})')

            else:
                # Create dedicated connection (legacy behavior)
                # Set defaults for connection parameters
                default_params = {
                    'login_timeout': 60,
                    'network_timeout': 300,
                    'socket_timeout': 300,
                    'validate_default_parameters': True,
                    'paramstyle': 'qmark',
                }

                conn_params = {
                    'account': self.config.account,
                    'user': self.config.user,
                    'warehouse': self.config.warehouse,
                    'database': self.config.database,
                    'schema': self.config.schema,
                    'login_timeout': self.config.login_timeout,
                    'network_timeout': self.config.network_timeout,
                    'socket_timeout': self.config.socket_timeout,
                    'ocsp_response_cache_filename': self.config.ocsp_response_cache_filename,
                    'validate_default_parameters': self.config.validate_default_parameters,
                    'paramstyle': self.config.paramstyle,
                    **self.config.connection_params,
                }

                # Add authentication parameters
                if self.config.authenticator:
                    conn_params['authenticator'] = self.config.authenticator
                    if self.config.authenticator == 'oauth':
                        conn_params['token'] = self.config.token
                    elif self.config.authenticator == 'externalbrowser':
                        pass  # No additional params needed
                    elif self.config.authenticator == 'okta' and self.config.okta_account_name:
                        conn_params['authenticator'] = f'https://{self.config.okta_account_name}.okta.com'
                elif self.config.private_key:
                    conn_params['private_key'] = self.config.private_key
                    if self.config.private_key_passphrase:
                        conn_params['private_key_passphrase'] = self.config.private_key_passphrase
                else:
                    conn_params['password'] = self.config.password

                # Optional parameters
                if self.config.role:
                    conn_params['role'] = self.config.role
                if self.config.timezone:
                    conn_params['timezone'] = self.config.timezone

                self.connection = snowflake.connector.connect(**conn_params)
                self._owns_connection = True  # We own this connection

                self.logger.info('Created dedicated Snowflake connection')

            # Create cursor
            self.cursor = self.connection.cursor(DictCursor)

            # Log connection info
            self.cursor.execute('SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
            result = self.cursor.fetchone()

            self.logger.info(f'Connected to Snowflake {result["CURRENT_VERSION()"]}')
            self.logger.info(f'Warehouse: {result["CURRENT_WAREHOUSE()"]}')
            self.logger.info(f'Database: {result["CURRENT_DATABASE()"]}.{result["CURRENT_SCHEMA()"]}')

            # Initialize stage for stage loading (streaming client is created lazily per table)
            if self.loading_method == 'stage':
                self._create_stage()

            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to Snowflake: {str(e)}')
            raise

    def _init_streaming_client(self, table_name: str) -> None:
        """
        Initialize Snowpipe Streaming client.

        Each table gets its own pipe and streaming client because the pipe's
        COPY INTO clause is tied to a specific table.

        Args:
            table_name: The target table name (for pipe naming)
        """
        try:
            from snowflake.ingest.streaming import StreamingIngestClient

            # Add authentication - Snowpipe Streaming requires key-pair auth
            if not self.config.private_key:
                raise ValueError(
                    'Snowpipe Streaming requires private_key authentication. '
                    'Password authentication is not supported.'
                )

            from cryptography.hazmat.primitives import serialization

            # Private key is already parsed as a cryptography object in __post_init__
            # Convert to PEM string for Snowpipe Streaming SDK
            pem_bytes = self.config.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            private_key_pem = pem_bytes.decode('utf-8')

            # Build properties dict for authentication
            # Snowpipe Streaming needs host in addition to account
            properties = {
                'account': self.config.account,
                'user': self.config.user,
                'private_key': private_key_pem,
                'host': f'{self.config.account}.snowflakecomputing.com',
            }

            if self.config.role:
                properties['role'] = self.config.role

            # Each table gets its own pipe (pipe's COPY INTO is tied to one table)
            pipe_name = f'{self.config.streaming_channel_prefix}_{table_name}_pipe'

            # Create the streaming pipe before initializing the client
            # The pipe must exist before the SDK can use it
            self._create_streaming_pipe(pipe_name, table_name)

            # Create client using Snowpipe Streaming API
            client = StreamingIngestClient(
                client_name=f'amp_{self.config.database}_{self.config.schema}_{table_name}',
                db_name=self.config.database,
                schema_name=self.config.schema,
                pipe_name=pipe_name,
                properties=properties,
            )

            # Store client for this table
            self.streaming_clients[table_name] = client

            self.logger.info(f'Initialized Snowpipe Streaming client with pipe {pipe_name} for table {table_name}')

        except ImportError:
            raise ImportError(
                'snowpipe-streaming package required for Snowpipe Streaming. '
                'Install with: pip install snowpipe-streaming'
            )
        except Exception as e:
            self.logger.error(f'Failed to initialize Snowpipe Streaming client for {table_name}: {e}')
            raise

    def _create_streaming_pipe(self, pipe_name: str, table_name: str) -> None:
        """
        Create Snowpipe Streaming pipe if it doesn't exist.

        Uses DATA_SOURCE(TYPE => 'STREAMING') to create a streaming-compatible pipe
        (not a traditional file-based pipe). The pipe maps VARIANT data from the stream
        to table columns.

        Args:
            pipe_name: Name of the pipe to create
            table_name: Target table for the pipe (table must already exist)
        """
        try:
            # Query table schema to get column names and types
            # Table must exist before creating the pipe
            self.cursor.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """,
                (self.config.schema, table_name.upper()),
            )
            column_info = [(row['COLUMN_NAME'], row['DATA_TYPE']) for row in self.cursor.fetchall()]

            if not column_info:
                raise RuntimeError(f"Table {table_name} does not exist or has no columns")

            # Build SELECT clause: map $1:column_name::TYPE for each column
            # The streaming data comes in as VARIANT ($1) and needs to be parsed
            select_columns = [f"$1:{col}::{dtype}" for col, dtype in column_info]
            column_names = [col for col, _ in column_info]

            # Create streaming pipe using DATA_SOURCE(TYPE => 'STREAMING')
            # This creates a streaming-compatible pipe (not file-based)
            create_pipe_sql = f"""
            CREATE PIPE IF NOT EXISTS {pipe_name}
            AS COPY INTO {table_name} ({', '.join(f'"{col}"' for col in column_names)})
            FROM (
                SELECT {', '.join(select_columns)}
                FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
            )
            """
            self.cursor.execute(create_pipe_sql)
            self.logger.info(f"Created or verified Snowpipe Streaming pipe '{pipe_name}' for table {table_name} with {len(column_info)} columns")
        except Exception as e:
            # Pipe creation might fail if it already exists or if we don't have permissions
            # Log warning but continue - the SDK will validate if the pipe is accessible
            self.logger.warning(f"Could not create streaming pipe '{pipe_name}': {e}")

    def _get_or_create_channel(self, table_name: str, channel_suffix: str = 'default') -> Any:
        """
        Get or create a Snowpipe Streaming channel for a table.

        Args:
            table_name: Target table name (must already exist in Snowflake)
            channel_suffix: Suffix for channel name (e.g., 'default', 'partition_0')

        Returns:
            Streaming channel instance
        """
        channel_name = f'{self.config.streaming_channel_prefix}_{table_name}_{channel_suffix}'
        channel_key = f'{table_name}:{channel_name}'

        if channel_key not in self.streaming_channels:
            # Get the client for this table
            client = self.streaming_clients[table_name]

            # Open channel - returns (channel, status) tuple
            channel, status = client.open_channel(channel_name=channel_name)

            self.logger.info(f'Opened Snowpipe Streaming channel: {channel_name} with status: {status}')

            self.streaming_channels[channel_key] = channel

        return self.streaming_channels[channel_key]

    def disconnect(self) -> None:
        """Close Snowflake connection and streaming channels"""
        # Close all streaming channels
        if self.streaming_channels:
            self.logger.info(f'Closing {len(self.streaming_channels)} streaming channels...')
            for channel_key, channel in self.streaming_channels.items():
                try:
                    channel.close()
                    self.logger.debug(f'Closed channel: {channel_key}')
                except Exception as e:
                    self.logger.warning(f'Error closing channel {channel_key}: {e}')

            self.streaming_channels.clear()

        # Close all streaming clients
        if self.streaming_clients:
            self.logger.info(f'Closing {len(self.streaming_clients)} Snowpipe Streaming clients...')
            for table_name, client in self.streaming_clients.items():
                try:
                    client.close()
                    self.logger.debug(f'Closed Snowpipe Streaming client for table {table_name}')
                except Exception as e:
                    self.logger.warning(f'Error closing streaming client for {table_name}: {e}')

            self.streaming_clients.clear()

        # Close cursor
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        # Release connection back to pool or close it
        if self.connection:
            if self._connection_pool and not self._owns_connection:
                # Return connection to pool
                self._connection_pool.release(self.connection)
                self.logger.info(f'Released connection to pool (worker {self._worker_id})')
            else:
                # Close owned connection
                self.connection.close()
                self.logger.info('Closed Snowflake connection')

            self.connection = None

        self._is_connected = False

    def _clear_table(self, table_name: str) -> None:
        """Clear table for overwrite mode"""
        # Snowflake loader doesn't support overwrite mode
        raise ValueError('Snowflake loader does not support OVERWRITE mode')

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Internal method to load data - used by both load_batch and load_table"""
        mode = kwargs.get('mode', LoadMode.APPEND)
        create_table = kwargs.get('create_table', True)

        # Snowflake loader only supports APPEND mode
        if mode == LoadMode.OVERWRITE:
            raise ValueError(
                'Snowflake loader does not support OVERWRITE mode. '
                'Please use APPEND mode or manually truncate/drop the table before loading.'
            )

        # Table creation is now handled by base class or pre-flight creation in parallel mode
        # For pandas loading, we skip manual table creation and let write_pandas handle it
        if create_table and table_name.upper() not in self._created_tables:
            # For pandas, skip table creation - write_pandas will handle it
            if self.loading_method != 'pandas':
                self._create_table_from_schema(batch.schema, table_name)
            self._created_tables.add(table_name.upper())

        # Route to appropriate loading method based on loading_method setting
        if self.loading_method == 'snowpipe_streaming':
            rows_loaded = self._load_via_streaming(batch, table_name, **kwargs)
        elif self.loading_method == 'insert':
            rows_loaded = self._load_via_insert(batch, table_name)
        elif self.loading_method == 'pandas':
            rows_loaded = self._load_via_pandas(batch, table_name)
        else:  # default to 'stage'
            rows_loaded = self._load_via_stage(batch, table_name)

        # Commit only for non-streaming methods (streaming commits automatically)
        if self.loading_method != 'snowpipe_streaming':
            self.connection.commit()

        return rows_loaded

    def _create_stage(self) -> None:
        """Create internal stage for data loading"""
        try:
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {self.stage_name}
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = '|'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE = '\\\\'
                ESCAPE_UNENCLOSED_FIELD = '\\\\'
                NULL_IF = ('\\\\N', 'NULL', 'null')
                EMPTY_FIELD_AS_NULL = TRUE
                COMPRESSION = {self.compression.upper()}
                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            )
            """
            self.cursor.execute(create_stage_sql)
            self.logger.info(f"Created or verified stage '{self.stage_name}'")
        except Exception as e:
            error_msg = f"Failed to create stage '{self.stage_name}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _load_via_stage(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Load data via Snowflake internal stage using COPY INTO with binary data support"""
        import datetime

        t_start = time.time()

        # Identify binary columns and convert to hex for CSV compatibility
        binary_columns = {}
        # Track VARIANT columns so we can use PARSE_JSON in COPY INTO
        variant_columns = set()
        modified_arrays = []
        modified_fields = []

        t_conversion_start = time.time()
        for i, field in enumerate(batch.schema):
            col_array = batch.column(i)

            # Track _meta_block_ranges as VARIANT column for JSON parsing
            if field.name == '_meta_block_ranges':
                variant_columns.add(field.name)

            # Check if this is a binary type that needs hex encoding
            if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type) or pa.types.is_fixed_size_binary(field.type):
                binary_columns[field.name] = field.type

                # Convert binary data to hex strings using list comprehension (faster)
                pylist = col_array.to_pylist()
                hex_values = [val.hex() if val is not None else None for val in pylist]

                # Create string array for CSV
                modified_arrays.append(pa.array(hex_values, type=pa.string()))
                modified_fields.append(pa.field(field.name, pa.string()))

            # Convert timestamps to string for CSV compatibility
            elif pa.types.is_timestamp(field.type):
                # Convert to Python list and format as ISO strings (faster)
                pylist = col_array.to_pylist()
                timestamp_values = [
                    dt.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(dt, datetime.datetime) else (str(dt) if dt is not None else None)
                    for dt in pylist
                ]

                modified_arrays.append(pa.array(timestamp_values, type=pa.string()))
                modified_fields.append(pa.field(field.name, pa.string()))

            else:
                # Keep other columns as-is
                modified_arrays.append(col_array)
                modified_fields.append(field)

        t_conversion_end = time.time()
        self.logger.debug(f'Data conversion took {t_conversion_end - t_conversion_start:.2f}s for {batch.num_rows} rows')

        # Create modified batch with hex-encoded binary columns
        t_batch_start = time.time()
        modified_schema = pa.schema(modified_fields)
        modified_batch = pa.RecordBatch.from_arrays(modified_arrays, schema=modified_schema)
        t_batch_end = time.time()
        self.logger.debug(f'Batch creation took {t_batch_end - t_batch_start:.2f}s')

        # Write to CSV
        t_csv_start = time.time()
        csv_buffer = io.BytesIO()
        write_options = pa_csv.WriteOptions(include_header=False, delimiter='|', quoting_style='needed')
        pa_csv.write_csv(modified_batch, csv_buffer, write_options=write_options)

        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        t_csv_end = time.time()
        self.logger.debug(f'CSV writing took {t_csv_end - t_csv_start:.2f}s ({len(csv_content)} bytes)')

        # Add worker_id to make file names unique across parallel workers
        stage_path = f'@{self.stage_name}/temp_{table_name}_{self._worker_id}_{int(time.time() * 1000000)}.csv'

        t_put_start = time.time()
        self.cursor.execute(f"PUT 'file://-' {stage_path} OVERWRITE = TRUE", file_stream=io.BytesIO(csv_content))
        t_put_end = time.time()
        self.logger.debug(f'PUT command took {t_put_end - t_put_start:.2f}s')

        # Build column list with transformations - convert hex strings back to binary, parse JSON for VARIANT
        final_column_specs = []
        for i, field in enumerate(batch.schema, start=1):
            if field.name in binary_columns:
                # Use TO_BINARY to convert hex string back to binary
                final_column_specs.append(f'TO_BINARY(${i}, \'HEX\')')
            elif field.name in variant_columns:
                # Use PARSE_JSON to convert JSON string to VARIANT
                final_column_specs.append(f'PARSE_JSON(${i})')
            else:
                final_column_specs.append(f'${i}')

        column_names = [f'"{field.name}"' for field in batch.schema]

        copy_sql = f"""
        COPY INTO {table_name} ({', '.join(column_names)})
        FROM (
            SELECT {', '.join(final_column_specs)}
            FROM {stage_path}
        )
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
        """

        t_copy_start = time.time()
        result = self.cursor.execute(copy_sql).fetchone()
        rows_loaded = result['rows_loaded'] if result else batch.num_rows
        t_copy_end = time.time()
        self.logger.debug(f'COPY INTO took {t_copy_end - t_copy_start:.2f}s ({rows_loaded} rows)')

        t_end = time.time()
        self.logger.info(f'Total _load_via_stage took {t_end - t_start:.2f}s for {rows_loaded} rows ({rows_loaded/(t_end - t_start):.0f} rows/sec)')

        return rows_loaded

    def _load_via_insert(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Load data via INSERT statements with proper type conversions for Snowflake"""
        import datetime

        column_names = [field.name for field in batch.schema]
        quoted_column_names = [f'"{field.name}"' for field in batch.schema]
        schema_fields = {field.name: field.type for field in batch.schema}

        placeholders = ', '.join(['?'] * len(quoted_column_names))
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(quoted_column_names)})
        VALUES ({placeholders})
        """

        rows = []
        data_dict = batch.to_pydict()

        # Transpose to row-wise format with type conversions
        for i in range(batch.num_rows):
            row = []
            for col_name in column_names:
                value = data_dict[col_name][i]
                field_type = schema_fields[col_name]

                # Convert Arrow nulls to None
                if value is None or (hasattr(value, 'is_valid') and not value.is_valid):
                    row.append(None)
                    continue

                # Convert Arrow scalars to Python types if needed
                if hasattr(value, 'as_py'):
                    value = value.as_py()

                # Now handle type-specific conversions
                if value is None:
                    row.append(None)
                # Convert timestamps to ISO string for Snowflake
                # Snowflake connector has issues with datetime objects in qmark paramstyle
                elif pa.types.is_timestamp(field_type):
                    if isinstance(value, datetime.datetime):
                        # Convert to ISO format string that Snowflake can parse
                        # Format: 'YYYY-MM-DD HH:MM:SS.ffffff'
                        row.append(value.strftime('%Y-%m-%d %H:%M:%S.%f'))
                    else:
                        # Shouldn't reach here after as_py() conversion
                        row.append(str(value) if value is not None else None)
                # Keep binary data as bytes (Snowflake handles bytes directly)
                elif pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type) or pa.types.is_fixed_size_binary(field_type):
                    row.append(value)
                else:
                    row.append(value)
            rows.append(row)

        self.cursor.executemany(insert_sql, rows)

        return len(rows)

    def _load_via_pandas(self, batch: pa.RecordBatch, table_name: str) -> int:
        """
        Load data via pandas DataFrame using Snowflake's write_pandas().

        This method leverages Snowflake's native pandas integration which handles
        type conversions automatically, including binary data.

        Optimizations:
        - Uses PyArrow-backed DataFrames to avoid unnecessary type conversions
        - Enables compression for staging files to reduce network transfer
        - Configures optimal chunk size for parallel uploads
        - Uses logical types for proper timestamp handling
        - Retries on transient errors (connection resets, credential expiration)

        Args:
            batch: PyArrow RecordBatch to load
            table_name: Target table name (must already exist)

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If write_pandas fails after retries
            ImportError: If pandas or snowflake.connector.pandas_tools not available
        """
        try:
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError:
            raise ImportError(
                'pandas and snowflake.connector.pandas_tools are required for pandas loading. '
                'Install with: pip install pandas'
            )

        t_start = time.time()
        max_retries = 3  # Retry on transient errors

        # Convert PyArrow RecordBatch to pandas DataFrame
        # Use PyArrow-backed DataFrame for zero-copy conversion (more efficient)
        t_conversion_start = time.time()
        try:
            # PyArrow-backed DataFrames avoid unnecessary type conversions
            # Requires pandas >= 1.5.0 with PyArrow support
            if pd is not None and hasattr(pd, 'ArrowDtype'):
                df = batch.to_pandas(types_mapper=pd.ArrowDtype)
            else:
                df = batch.to_pandas()
        except Exception:
            # Fallback to regular pandas if PyArrow backend not available
            df = batch.to_pandas()
        t_conversion_end = time.time()
        self.logger.debug(f'Pandas conversion took {t_conversion_end - t_conversion_start:.2f}s for {batch.num_rows} rows')

        # Use Snowflake's write_pandas to load data with retry logic
        # This handles all type conversions internally and is optimized for bulk loading
        # Let write_pandas handle table creation for better compatibility
        t_write_start = time.time()

        # Build write_pandas parameters
        write_params = {
            'df': df,
            'table_name': table_name,
            'database': self.config.database,
            'schema': self.config.schema,
            'quote_identifiers': True,  # Quote identifiers for safety
            'auto_create_table': True,  # Let write_pandas create the table
            'overwrite': False,  # Append mode - don't overwrite existing data
            'use_logical_type': True,  # Use proper logical types for timestamps and other complex types
        }

        # Add compression if configured
        if self.config.pandas_compression and self.config.pandas_compression != 'none':
            write_params['compression'] = self.config.pandas_compression

        # Add parallel parameter (may not be supported in all versions)
        try:
            write_params['parallel'] = self.config.pandas_parallel_threads
        except TypeError:
            # parallel parameter not supported in this version, skip it
            pass

        # Retry loop for transient errors
        for attempt in range(max_retries + 1):
            try:
                # Pass current connection
                write_params['conn'] = self.connection
                success, num_chunks, num_rows, output = write_pandas(**write_params)

                if not success:
                    raise RuntimeError(f'write_pandas failed: {output}')

                # Success! Break out of retry loop
                break

            except Exception as e:
                error_str = str(e).lower()
                # Check if error is transient (connection reset, credential expiration, timeout)
                is_transient = any(pattern in error_str for pattern in [
                    'connection reset', 'econnreset', '403', 'forbidden',
                    'timeout', 'credential', 'expired', 'connection aborted',
                    'jwt', 'invalid'  # JWT token expiration
                ])

                if attempt < max_retries and is_transient:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    self.logger.warning(
                        f'Pandas loading error (attempt {attempt + 1}/{max_retries + 1}), '
                        f'refreshing connection and retrying in {wait_time}s: {e}'
                    )
                    time.sleep(wait_time)

                    # Get a fresh connection from the pool
                    # This will trigger connection validation and potential refresh
                    if self._connection_pool:
                        self._connection_pool.release(self.connection)
                        self.connection = self._connection_pool.acquire()
                        self.cursor = self.connection.cursor(DictCursor)
                else:
                    # Final attempt failed or non-transient error
                    self.logger.error(f'Pandas loading failed after {attempt + 1} attempts: {e}')
                    raise

        t_write_end = time.time()

        t_end = time.time()
        write_time = t_write_end - t_write_start
        total_time = t_end - t_start
        throughput = num_rows / total_time if total_time > 0 else 0

        self.logger.debug(f'write_pandas took {write_time:.2f}s for {num_rows} rows in {num_chunks} chunks')
        self.logger.info(f'Total _load_via_pandas took {total_time:.2f}s for {num_rows} rows ({throughput:.0f} rows/sec)')

        return num_rows

    def _arrow_batch_to_snowflake_rows(self, batch: pa.RecordBatch) -> List[Dict[str, Any]]:
        """
        Convert PyArrow RecordBatch to list of row dictionaries for Snowpipe Streaming.

        Arrow-optimized implementation:
        - Uses Arrow's C++ optimized to_pylist() instead of to_pydict()
        - Pre-identifies column types at schema level (once per batch)
        - Processes columns independently (better cache locality)
        - Transposes efficiently using zip()

        Snowpipe Streaming expects row-oriented data as JSON-serializable dictionaries:
        [
            {'col1': val1, 'col2': val2, ...},
            {'col1': val1, 'col2': val2, ...},
        ]
        """
        import datetime

        t_start = time.perf_counter()

        # OPTIMIZATION 1: Pre-identify columns that need conversions (schema analysis once)
        t_schema_start = time.perf_counter()
        timestamp_columns = set()
        binary_columns = set()

        for field in batch.schema:
            if pa.types.is_timestamp(field.type):
                timestamp_columns.add(field.name)
            elif (pa.types.is_binary(field.type) or
                  pa.types.is_large_binary(field.type) or
                  pa.types.is_fixed_size_binary(field.type)):
                binary_columns.add(field.name)
        t_schema_end = time.perf_counter()

        # OPTIMIZATION 2: Column-wise processing using Arrow's optimized to_pylist()
        t_conversion_start = time.perf_counter()
        columns = {}
        column_names = []

        for field in batch.schema:
            col_name = field.name
            column_names.append(col_name)

            # Arrow's to_pylist() is C++ optimized (much faster than Python loops)
            col_array = batch.column(col_name)
            pylist = col_array.to_pylist()

            # OPTIMIZATION 3: Vectorized conversions per column (list comprehension)
            if col_name in timestamp_columns:
                # Convert timestamps to ISO 8601 strings
                columns[col_name] = [
                    v.isoformat() if isinstance(v, datetime.datetime) else None
                    for v in pylist
                ]
            elif col_name in binary_columns:
                # Convert binary data to hex strings
                columns[col_name] = [
                    v.hex() if isinstance(v, bytes) else None
                    for v in pylist
                ]
            else:
                # No conversion needed
                columns[col_name] = pylist
        t_conversion_end = time.perf_counter()

        # OPTIMIZATION 4: Efficient transpose using zip() (C-optimized built-in)
        t_transpose_start = time.perf_counter()
        rows = [
            dict(zip(column_names, row_values))
            for row_values in zip(*[columns[col] for col in column_names])
        ]
        t_transpose_end = time.perf_counter()

        t_end = time.perf_counter()

        # Log timing breakdown with explicit stderr output and flushing
        import sys
        total_time = t_end - t_start
        schema_time = t_schema_end - t_schema_start
        conversion_time = t_conversion_end - t_conversion_start
        transpose_time = t_transpose_end - t_transpose_start

        timing_msg = (
            f'⏱️  Row conversion timing for {batch.num_rows} rows: '
            f'total={total_time*1000:.2f}ms '
            f'(schema={schema_time*1000:.2f}ms, '
            f'conversion={conversion_time*1000:.2f}ms, '
            f'transpose={transpose_time*1000:.2f}ms)\n'
        )
        sys.stderr.write(timing_msg)
        sys.stderr.flush()

        return rows

    def _is_transient_error(self, error: Exception) -> bool:
        """
        Check if error is transient and worth retrying.

        Transient errors include network issues, rate limiting, and temporary service issues.
        """
        transient_patterns = [
            'timeout',
            'throttle',
            'rate limit',
            'service unavailable',
            'connection reset',
            'connection refused',
            'temporarily unavailable',
            'network',
        ]

        error_str = str(error).lower()
        return any(pattern in error_str for pattern in transient_patterns)

    def _append_with_retry(self, channel: Any, rows: List[Dict[str, Any]]) -> None:
        """
        Append rows to Snowpipe Streaming channel with automatic retry on transient failures.

        Args:
            channel: Snowpipe Streaming channel instance
            rows: List of row dictionaries to append

        Raises:
            Exception: If insertion fails after all retries
        """
        max_retries = self.config.streaming_max_retries

        for attempt in range(max_retries + 1):
            try:
                channel.append_rows(rows)
                self.logger.debug(f'Inserted {len(rows)} rows to Snowpipe Streaming channel')
                return
            except Exception as e:
                # Check if we should retry
                if attempt < max_retries and self._is_transient_error(e):
                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    self.logger.warning(
                        f'Snowpipe Streaming error (attempt {attempt + 1}/{max_retries + 1}), '
                        f'retrying in {wait_time}s: {e}'
                    )
                    time.sleep(wait_time)
                else:
                    # Final attempt failed or non-transient error
                    self.logger.error(f'Snowpipe Streaming insertion failed after {attempt + 1} attempts: {e}')
                    raise

    def _load_via_streaming(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """
        Load data via Snowpipe Streaming API with optimal batch sizes and retry logic.

        Optimizations:
        - Splits large batches into optimal chunk sizes (50K rows) for Snowpipe Streaming
        - Uses Arrow's zero-copy slice() operation for efficient chunking
        - Delegates retry logic to helper method

        Args:
            batch: PyArrow RecordBatch to load
            table_name: Target table name (must already exist)
            **kwargs: Additional options including:
                - channel_suffix: Optional channel suffix for parallel loading
                - offset_token: Optional offset token for exactly-once semantics (currently unused)

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If insertion fails after all retries
        """
        # Initialize streaming client for this table if needed (lazy initialization, one client per table)
        if table_name not in self.streaming_clients:
            self._init_streaming_client(table_name)

        # Get channel (create if needed)
        channel_suffix = kwargs.get('channel_suffix', 'default')
        channel = self._get_or_create_channel(table_name, channel_suffix)

        # OPTIMIZATION: Split large batches into optimal chunks for Snowpipe Streaming
        # Snowpipe Streaming works best with chunks of 10K-50K rows
        MAX_ROWS_PER_CHUNK = 50000

        if batch.num_rows > MAX_ROWS_PER_CHUNK:
            # Process in chunks using Arrow's zero-copy slice operation
            total_loaded = 0
            for offset in range(0, batch.num_rows, MAX_ROWS_PER_CHUNK):
                chunk_size = min(MAX_ROWS_PER_CHUNK, batch.num_rows - offset)
                chunk = batch.slice(offset, chunk_size)  # Zero-copy slice!

                # Convert chunk to row-oriented format
                rows = self._arrow_batch_to_snowflake_rows(chunk)

                # Append with retry logic
                self._append_with_retry(channel, rows)
                total_loaded += len(rows)

            self.logger.debug(
                f'Loaded {total_loaded} rows to Snowpipe Streaming in '
                f'{(batch.num_rows + MAX_ROWS_PER_CHUNK - 1) // MAX_ROWS_PER_CHUNK} chunks'
            )
            return total_loaded
        else:
            # Single batch (small enough to process at once)
            rows = self._arrow_batch_to_snowflake_rows(batch)
            self._append_with_retry(channel, rows)
            return len(rows)

    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create Snowflake table from Arrow schema"""

        # Check if table already exists
        self.cursor.execute(
            """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            (self.config.schema, table_name.upper()),
        )

        result = self.cursor.fetchone()
        count = result['COUNT'] if result else 0
        if count > 0:
            self.logger.debug(f"Table '{table_name}' already exists, skipping creation")
            return

        # Arrow to Snowflake type mapping
        type_mapping = {
            # Integer types
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INTEGER',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'SMALLINT',
            pa.uint16(): 'INTEGER',
            pa.uint32(): 'BIGINT',
            pa.uint64(): 'BIGINT',
            # Floating point types
            pa.float32(): 'FLOAT',
            pa.float64(): 'DOUBLE',
            pa.float16(): 'FLOAT',
            # String types
            pa.string(): 'VARCHAR',
            pa.large_string(): 'VARCHAR',
            pa.utf8(): 'VARCHAR',
            # Binary types
            pa.binary(): 'BINARY',
            pa.large_binary(): 'BINARY',
            # Boolean type
            pa.bool_(): 'BOOLEAN',
            # Date and time types
            pa.date32(): 'DATE',
            pa.date64(): 'DATE',
            pa.time32('s'): 'TIME',
            pa.time32('ms'): 'TIME',
            pa.time64('us'): 'TIME',
            pa.time64('ns'): 'TIME',
        }

        # Build CREATE TABLE statement
        columns = []
        for field in schema:
            # Special case: _meta_block_ranges should be VARIANT for optimal JSON querying
            if field.name == '_meta_block_ranges':
                snowflake_type = 'VARIANT'
            # Handle complex types
            elif pa.types.is_timestamp(field.type):
                if field.type.tz is not None:
                    snowflake_type = 'TIMESTAMP_TZ'
                else:
                    snowflake_type = 'TIMESTAMP_NTZ'
            elif pa.types.is_date(field.type):
                snowflake_type = 'DATE'
            elif pa.types.is_time(field.type):
                snowflake_type = 'TIME'
            elif pa.types.is_decimal(field.type):
                decimal_type = field.type
                snowflake_type = f'NUMBER({decimal_type.precision},{decimal_type.scale})'
            elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                snowflake_type = 'VARIANT'
            elif pa.types.is_struct(field.type):
                snowflake_type = 'OBJECT'
            elif pa.types.is_map(field.type):
                snowflake_type = 'OBJECT'
            elif pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
                snowflake_type = 'BINARY'
            elif pa.types.is_fixed_size_binary(field.type):
                snowflake_type = f'BINARY({field.type.byte_width})'
            elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                # Use VARCHAR with no length limit for flexibility
                snowflake_type = 'VARCHAR'
            else:
                # Use mapping or default to VARCHAR
                snowflake_type = type_mapping.get(field.type, 'VARCHAR')

            # Handle nullability
            nullable = '' if field.nullable else ' NOT NULL'

            # Add column definition - quote column name for safety with special characters
            columns.append(f'"{field.name}" {snowflake_type}{nullable}')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """

        try:
            self.logger.info(f"Creating table '{table_name}' with {len(columns)} columns")
            self.cursor.execute(create_sql)
            self.logger.debug(f"Successfully created table '{table_name}'")
        except Exception as e:
            raise RuntimeError(f"Failed to create table '{table_name}': {str(e)}") from e

    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get Snowflake-specific metadata for batch operation"""
        return {
            'loading_method': self.loading_method,
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema,
        }

    def _get_loader_table_metadata(
        self, table: pa.Table, duration: float, batch_count: int, **kwargs
    ) -> Dict[str, Any]:
        """Get Snowflake-specific metadata for table operation"""
        return {
            'loading_method': self.loading_method,
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema,
        }

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a Snowflake table"""
        try:
            # Get table metadata
            self.cursor.execute(
                """
                SELECT
                    TABLE_NAME,
                    TABLE_SCHEMA,
                    TABLE_CATALOG,
                    TABLE_TYPE,
                    ROW_COUNT,
                    BYTES,
                    CLUSTERING_KEY,
                    COMMENT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                (self.config.schema, table_name.upper()),
            )

            table_info = self.cursor.fetchone()
            if not table_info:
                return None

            # Get column information
            self.cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """,
                (self.config.schema, table_name.upper()),
            )

            columns = self.cursor.fetchall()

            return {
                'table_name': table_info['TABLE_NAME'],
                'schema': table_info['TABLE_SCHEMA'],
                'database': table_info['TABLE_CATALOG'],
                'type': table_info['TABLE_TYPE'],
                'row_count': table_info['ROW_COUNT'],
                'size_bytes': table_info['BYTES'],
                'size_mb': round(table_info['BYTES'] / 1024 / 1024, 2) if table_info['BYTES'] else 0,
                'clustering_key': table_info['CLUSTERING_KEY'],
                'comment': table_info['COMMENT'],
                'columns': [
                    {
                        'name': col['COLUMN_NAME'],
                        'type': col['DATA_TYPE'],
                        'nullable': col['IS_NULLABLE'] == 'YES',
                        'default': col['COLUMN_DEFAULT'],
                        'max_length': col['CHARACTER_MAXIMUM_LENGTH'],
                        'precision': col['NUMERIC_PRECISION'],
                        'scale': col['NUMERIC_SCALE'],
                    }
                    for col in columns
                ],
            }

        except Exception as e:
            self.logger.error(f"Failed to get table info for '{table_name}': {str(e)}")
            return None

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str) -> None:
        """
        Handle blockchain reorganization by deleting affected rows from Snowflake.

        For Snowpipe Streaming mode:
        - Closes all streaming channels for the affected table
        - Performs SQL-based deletion of affected rows
        - Channels will be recreated on next insert with new offset tokens

        For stage/insert modes:
        - Uses SQL-based deletion with JSON functions to identify affected rows

        Args:
            invalidation_ranges: List of block ranges to invalidate (reorg points)
            table_name: The table containing the data to invalidate
        """
        if not invalidation_ranges:
            return

        try:
            # For Snowpipe Streaming mode, close all channels for this table before deletion
            if self.loading_method == 'snowpipe_streaming' and self.streaming_channels:
                channels_to_close = []

                # Find all channels for this table
                for channel_key, channel in list(self.streaming_channels.items()):
                    if channel_key.startswith(f'{table_name}:'):
                        channels_to_close.append((channel_key, channel))

                # Close and remove the channels
                if channels_to_close:
                    self.logger.info(
                        f'Closing {len(channels_to_close)} streaming channels for table '
                        f"'{table_name}' due to blockchain reorg"
                    )

                    for channel_key, channel in channels_to_close:
                        try:
                            channel.close()
                            del self.streaming_channels[channel_key]
                            self.logger.debug(f'Closed streaming channel: {channel_key}')
                        except Exception as e:
                            self.logger.warning(f'Error closing channel {channel_key}: {e}')
                            # Continue closing other channels even if one fails

                    self.logger.info(
                        f'All streaming channels for table \'{table_name}\' closed. '
                        'Channels will be recreated on next insert with new offset tokens.'
                    )
            # First check if the table has the metadata column
            self.cursor.execute(
                """
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = '_meta_block_ranges'
                """,
                (self.config.schema, table_name.upper()),
            )

            result = self.cursor.fetchone()
            if not result or result['COUNT'] == 0:
                self.logger.warning(
                    f"Table '{table_name}' doesn't have '_meta_block_ranges' column, skipping reorg handling"
                )
                return

            # Build WHERE conditions for FLATTEN-based deletion
            # Since Snowflake doesn't support complex subqueries in DELETE WHERE,
            # we use a CTE-based approach with row identification
            where_conditions = []

            for range_obj in invalidation_ranges:
                network = range_obj.network
                reorg_start = range_obj.start

                # Create condition for this network's reorg
                where_conditions.append(f"""
                    (f.value:network::STRING = '{network}' AND f.value:end::NUMBER >= {reorg_start})
                """)

            if where_conditions:
                # Use a CTE to identify rows to delete, then delete using METADATA$ROW_ID
                where_clause = ' OR '.join(where_conditions)

                # Create DELETE SQL using CTE for row identification
                delete_sql = f"""
                DELETE FROM {table_name}
                WHERE "_meta_block_ranges" IN (
                    SELECT DISTINCT "_meta_block_ranges"
                    FROM {table_name}, LATERAL FLATTEN(input => "_meta_block_ranges") f
                    WHERE {where_clause}
                )
                """

                self.logger.info(
                    f'Executing blockchain reorg deletion for {len(invalidation_ranges)} networks '
                    f"in Snowflake table '{table_name}'"
                )

                # Execute the delete and get row count
                self.cursor.execute(delete_sql)
                deleted_rows = self.cursor.rowcount

                # Commit the transaction
                self.connection.commit()

                self.logger.info(f"Blockchain reorg deleted {deleted_rows} rows from table '{table_name}'")

        except Exception as e:
            self.logger.error(f"Failed to handle blockchain reorg for table '{table_name}': {str(e)}")
            # Rollback on error
            if self.connection:
                self.connection.rollback()
            raise
