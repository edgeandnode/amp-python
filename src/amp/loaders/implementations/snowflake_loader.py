import io
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.csv as pa_csv
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

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
    connection_params: Dict[str, Any] = None

    # Loading method configuration
    loading_method: str = 'stage'  # 'stage', 'insert', or 'snowpipe_streaming'

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

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.connection: SnowflakeConnection = None
        self.cursor = None
        self._created_tables = set()  # Track created tables

        # Loading configuration (backward compatible with use_stage)
        self.use_stage = config.get('use_stage', True)
        self.stage_name = config.get('stage_name', 'amp_STAGE')
        self.compression = config.get('compression', 'gzip')

        # Determine loading method (use_stage is deprecated in favor of loading_method)
        self.loading_method = self.config.loading_method

        # Snowpipe Streaming client and channels
        self.streaming_client: Optional[Any] = None  # SnowflakeStreamingIngestClient
        self.streaming_channels: Dict[str, Any] = {}  # table_name:channel_name -> channel

    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['account', 'user', 'warehouse', 'database']

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            # Set defaults for connection parameters (can be overridden via connection_params)
            default_params = {
                'login_timeout': 60,
                'network_timeout': 300,
                'socket_timeout': 300,
                'validate_default_parameters': True,
                'paramstyle': 'qmark',
            }

            # Build connection parameters with required fields
            conn_params = {
                'account': self.config.account,
                'user': self.config.user,
                'warehouse': self.config.warehouse,
                'database': self.config.database,
                'schema': self.config.schema,
                **default_params,
                **self.config.connection_params,  # User params override defaults
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

            self.connection = snowflake.connector.connect(**conn_params)
            self.cursor = self.connection.cursor(DictCursor)

            self.cursor.execute('SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
            result = self.cursor.fetchone()

            self.logger.info(f'Connected to Snowflake {result["CURRENT_VERSION()"]}')
            self.logger.info(f'Warehouse: {result["CURRENT_WAREHOUSE()"]}')
            self.logger.info(f'Database: {result["CURRENT_DATABASE()"]}.{result["CURRENT_SCHEMA()"]}')

            # Initialize stage for stage loading (streaming client is created lazily per table)
            if self.loading_method == 'stage' or self.use_stage:
                self._create_stage()

            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to Snowflake: {str(e)}')
            raise

    def _init_streaming_client(self, table_name: str) -> None:
        """
        Initialize Snowpipe Streaming client for a specific table.

        Args:
            table_name: The target table name to use for the pipe
        """
        try:
            from snowflake.ingest.streaming import StreamingIngestClient

            # Pipe name for the Snowpipe Streaming client
            # NOTE: Don't pre-create the pipe - Snowpipe Streaming SDK manages pipes internally
            pipe_name = f'{self.config.streaming_channel_prefix}_{table_name}_pipe'

            # Build properties for authentication
            properties = {
                'account': self.config.account,
                'user': self.config.user,
                'url': f'https://{self.config.account}.snowflakecomputing.com',
            }

            # Add authentication - Snowpipe Streaming requires key-pair auth
            if self.config.private_key:
                from cryptography.hazmat.primitives import serialization

                # Private key is already parsed as a cryptography object in __post_init__
                # Convert to PEM string for Snowpipe Streaming SDK
                pem_bytes = self.config.private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                properties['private_key'] = pem_bytes.decode('utf-8')
            else:
                # Snowpipe Streaming requires key-pair auth
                raise ValueError(
                    'Snowpipe Streaming requires private_key authentication. '
                    'Password authentication is not supported.'
                )

            # Add role if specified
            if self.config.role:
                properties['role'] = self.config.role

            # Create client with the pipe name
            self.streaming_client = StreamingIngestClient(
                client_name=f'amp_{self.config.database}_{self.config.schema}',
                db_name=self.config.database,
                schema_name=self.config.schema,
                pipe_name=pipe_name,
                properties=properties,
            )

            self.logger.info(f'Initialized Snowpipe Streaming client for table {table_name} with pipe {pipe_name}')

        except ImportError:
            raise ImportError(
                'snowpipe-streaming package required for Snowpipe Streaming. '
                'Install with: pip install snowpipe-streaming'
            )
        except Exception as e:
            self.logger.error(f'Failed to initialize Snowpipe Streaming client: {e}')
            raise

    def _create_streaming_pipe(self, pipe_name: str, table_name: str) -> None:
        """
        Create Snowpipe Streaming pipe if it doesn't exist.

        Args:
            pipe_name: Name of the pipe to create
            table_name: Target table for the pipe (table must already exist)
        """
        try:
            # Create the pipe using the actual target table
            create_pipe_sql = f"""
            CREATE PIPE IF NOT EXISTS {pipe_name}
            AS COPY INTO {table_name} FROM @{self.stage_name}
            """
            self.cursor.execute(create_pipe_sql)
            self.logger.info(f"Created or verified Snowpipe '{pipe_name}' for table {table_name}")
        except Exception as e:
            # Pipe creation might fail if it already exists or if we don't have permissions
            # Log warning but continue - the SDK will validate if the pipe is accessible
            self.logger.warning(f"Could not create pipe '{pipe_name}': {e}")

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
            # Create new channel - returns (channel, status)
            channel, status = self.streaming_client.open_channel(channel_name)

            if status != 'OPEN':
                raise RuntimeError(f'Failed to open streaming channel {channel_name}: status={status}')

            self.streaming_channels[channel_key] = channel
            self.logger.info(f'Opened Snowpipe Streaming channel: {channel_name}')

        return self.streaming_channels[channel_key]

    def disconnect(self) -> None:
        """Close Snowflake connection and streaming channels"""
        # Close all streaming channels
        if self.streaming_channels:
            self.logger.info(f'Closing {len(self.streaming_channels)} streaming channels...')
            for channel_key, channel in self.streaming_channels.items():
                try:
                    channel.close()
                    self.logger.debug(f'Closed channel: {channel.name}')
                except Exception as e:
                    self.logger.warning(f'Error closing channel: {e}')

            self.streaming_channels.clear()

        # Close streaming client
        if self.streaming_client:
            try:
                self.streaming_client.close()
                self.streaming_client = None
                self.logger.debug('Closed Snowpipe Streaming client')
            except Exception as e:
                self.logger.warning(f'Error closing streaming client: {e}')

        # Close cursor and connection
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        self._is_connected = False
        self.logger.info('Disconnected from Snowflake')

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

        if create_table and table_name.upper() not in self._created_tables:
            self._create_table_from_schema(batch.schema, table_name)
            self._created_tables.add(table_name.upper())

        # Route to appropriate loading method
        if self.loading_method == 'snowpipe_streaming':
            rows_loaded = self._load_via_streaming(batch, table_name, **kwargs)
        elif self.use_stage:
            rows_loaded = self._load_via_stage(batch, table_name)
        else:
            rows_loaded = self._load_via_insert(batch, table_name)

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
        """Load data via Snowflake internal stage using COPY INTO"""

        csv_buffer = io.BytesIO()

        write_options = pa_csv.WriteOptions(include_header=False, delimiter='|', quoting_style='needed')

        pa_csv.write_csv(batch, csv_buffer, write_options=write_options)

        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        stage_path = f'@{self.stage_name}/temp_{table_name}_{int(time.time() * 1000)}.csv'

        self.cursor.execute(f"PUT 'file://-' {stage_path} OVERWRITE = TRUE", file_stream=io.BytesIO(csv_content))

        column_names = [f'"{field.name}"' for field in batch.schema]

        copy_sql = f"""
        COPY INTO {table_name} ({', '.join(column_names)})
        FROM {stage_path}
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
        """

        result = self.cursor.execute(copy_sql).fetchone()
        rows_loaded = result['rows_loaded'] if result else batch.num_rows

        return rows_loaded

    def _load_via_insert(self, batch: pa.RecordBatch, table_name: str) -> int:
        """Load data via INSERT statements using Arrow's native iteration"""

        column_names = [field.name for field in batch.schema]
        quoted_column_names = [f'"{field.name}"' for field in batch.schema]

        placeholders = ', '.join(['?'] * len(quoted_column_names))
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(quoted_column_names)})
        VALUES ({placeholders})
        """

        rows = []
        data_dict = batch.to_pydict()

        # Transpose to row-wise format
        for i in range(batch.num_rows):
            row = []
            for col_name in column_names:
                value = data_dict[col_name][i]

                # Convert Arrow nulls to None
                if value is None or (hasattr(value, 'is_valid') and not value.is_valid):
                    row.append(None)
                else:
                    row.append(value)
            rows.append(row)

        self.cursor.executemany(insert_sql, rows)

        return len(rows)

    def _arrow_batch_to_snowflake_rows(self, batch: pa.RecordBatch) -> List[Dict[str, Any]]:
        """
        Convert PyArrow RecordBatch to list of row dictionaries for Snowpipe Streaming.

        Snowpipe Streaming expects row-oriented data as:
        [
            {'col1': val1, 'col2': val2, ...},
            {'col1': val1, 'col2': val2, ...},
        ]
        """
        rows = []
        data_dict = batch.to_pydict()
        num_rows = batch.num_rows

        for i in range(num_rows):
            row = {}
            for col_name in data_dict.keys():
                value = data_dict[col_name][i]

                # Handle Arrow nulls
                if value is None or (hasattr(value, 'is_valid') and not value.is_valid):
                    row[col_name] = None
                else:
                    # Type-specific conversions
                    row[col_name] = value  # Most types work as-is

            rows.append(row)

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

    def _load_via_streaming(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """
        Load data via Snowpipe Streaming API with automatic retry on transient failures.

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
        # Initialize streaming client if needed (lazy initialization per table)
        if not self.streaming_client:
            self._init_streaming_client(table_name)

        # Get channel (create if needed)
        channel_suffix = kwargs.get('channel_suffix', 'default')
        channel = self._get_or_create_channel(table_name, channel_suffix)

        # Convert Arrow batch to row-oriented format
        rows = self._arrow_batch_to_snowflake_rows(batch)

        # Retry logic with exponential backoff
        max_retries = self.config.streaming_max_retries
        rows_loaded = 0

        for attempt in range(max_retries + 1):
            try:
                # Insert rows one by one using append_row
                for row in rows:
                    channel.append_row(row)
                    rows_loaded += 1

                self.logger.debug(f'Inserted {len(rows)} rows to Snowpipe Streaming channel')

                return rows_loaded

            except Exception as e:
                # Check if we should retry
                if attempt < max_retries and self._is_transient_error(e):
                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    self.logger.warning(
                        f'Snowpipe Streaming error (attempt {attempt + 1}/{max_retries + 1}), '
                        f'retrying in {wait_time}s: {e}'
                    )
                    time.sleep(wait_time)
                    rows_loaded = 0  # Reset counter for retry
                else:
                    # Final attempt failed or non-transient error
                    self.logger.error(f'Snowpipe Streaming insertion failed after {attempt + 1} attempts: {e}')
                    raise

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
            # Handle complex types
            if pa.types.is_timestamp(field.type):
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
            'loading_method': 'stage' if self.use_stage else 'insert',
            'warehouse': self.config.warehouse,
            'database': self.config.database,
            'schema': self.config.schema,
        }

    def _get_loader_table_metadata(
        self, table: pa.Table, duration: float, batch_count: int, **kwargs
    ) -> Dict[str, Any]:
        """Get Snowflake-specific metadata for table operation"""
        return {
            'loading_method': 'stage' if self.use_stage else 'insert',
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
                            self.logger.debug(f'Closed streaming channel: {channel.name}')
                        except Exception as e:
                            self.logger.warning(f'Error closing channel {channel.name}: {e}')
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
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = '_META_BLOCK_RANGES'
                """,
                (self.config.schema, table_name.upper()),
            )

            result = self.cursor.fetchone()
            if not result or result['COUNT'] == 0:
                self.logger.warning(
                    f"Table '{table_name}' doesn't have '_meta_block_ranges' column, skipping reorg handling"
                )
                return

            # Build DELETE statement with conditions for each invalidation range
            # Snowflake's PARSE_JSON and ARRAY_SIZE functions help work with JSON data
            delete_conditions = []

            for range_obj in invalidation_ranges:
                network = range_obj.network
                reorg_start = range_obj.start

                # Create condition for this network's reorg
                # Delete rows where any range in the JSON array for this network has end >= reorg_start
                condition = f"""
                EXISTS (
                    SELECT 1
                    FROM TABLE(FLATTEN(input => PARSE_JSON("_META_BLOCK_RANGES"))) f
                    WHERE f.value:network::STRING = '{network}'
                    AND f.value:end::NUMBER >= {reorg_start}
                )
                """
                delete_conditions.append(condition)

            # Combine conditions with OR
            if delete_conditions:
                where_clause = ' OR '.join(f'({cond})' for cond in delete_conditions)

                # Execute deletion
                delete_sql = f'DELETE FROM {table_name} WHERE {where_clause}'

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
