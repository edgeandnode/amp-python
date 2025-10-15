# Snowflake Loader Configuration

Complete guide for configuring the Snowflake data loader in amp-python.

## Overview

The Snowflake loader provides high-performance data loading using Snowflake's COPY INTO command with internal stages. It supports multiple authentication methods and automatic schema creation.

## Basic Configuration

### Required Fields

```python
from amp.client import Client

client = Client("grpc://your-amp-server:80")

client.configure_connection(
    name='my_snowflake',
    loader='snowflake',
    config={
        'account': 'myorg-myaccount',     # Snowflake account identifier
        'user': 'myuser',                  # Snowflake username
        'password': 'mypassword',          # Password (not needed for key-based auth)
        'warehouse': 'COMPUTE_WH',         # Warehouse for query execution
        'database': 'MY_DATABASE',         # Target database
    }
)
```

### Optional Fields

```python
config = {
    # Required
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',

    # Optional - commonly used
    'schema': 'PUBLIC',                    # Default: 'PUBLIC'
    'role': 'DATA_LOADER',                 # Snowflake role to use

    # Loading behavior
    'use_stage': True,                     # Use internal stage (default: True)
    'stage_name': 'AMP_STAGE',            # Stage name (default: 'AMP_STAGE')
    'compression': 'gzip',                 # Compression type (default: 'gzip')
}
```

## Authentication Methods

### 1. Username/Password (Default)

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'password': 'mypassword',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',
}
```

### 2. Key Pair Authentication

More secure than passwords, recommended for production:

```python
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load private key from file
with open("rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,  # or b'passphrase' if encrypted
        backend=default_backend()
    )

config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',
    'private_key': private_key,
    # 'private_key_passphrase': 'passphrase',  # If key is encrypted
}
```

### 3. OAuth Authentication

For SSO environments:

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',
    'authenticator': 'oauth',
    'token': 'your_oauth_token',
}
```

### 4. External Browser (SSO)

Opens browser for authentication:

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',
    'authenticator': 'externalbrowser',
}
```

### 5. Okta Authentication

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',
    'authenticator': 'okta',
    'okta_account_name': 'mycompany',  # Will use https://mycompany.okta.com
}
```

## Advanced Connection Parameters

For advanced Snowflake connector options, use the `connection_params` dictionary:

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'password': 'mypassword',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',

    'connection_params': {
        # Timeout settings (in seconds)
        'login_timeout': 60,              # Default: 60
        'network_timeout': 300,           # Default: 300 (5 minutes)
        'socket_timeout': 300,            # Default: 300 (5 minutes)

        # Timezone
        'timezone': 'UTC',                # Set session timezone

        # Security
        'ocsp_response_cache_filename': '/path/to/cache',  # OCSP response cache
        'insecure_mode': False,           # Disable SSL verification (not recommended)

        # Connection behavior
        'validate_default_parameters': True,  # Validate connection params (default: True)
        'paramstyle': 'qmark',            # Parameter style: 'qmark', 'numeric', 'format'
        'autocommit': False,              # Auto-commit mode (default: False)
        'client_session_keep_alive': True,  # Keep session alive
        'client_prefetch_threads': 4,     # Prefetch threads for result fetching

        # Application identification
        'application': 'amp-python',      # Application name in Snowflake query history
        'session_parameters': {           # Session-level parameters
            'QUERY_TAG': 'amp-load',
            'TIMEZONE': 'UTC',
        },
    }
}
```

## Loading Configuration

### Stage-Based Loading (Recommended)

Uses Snowflake's internal stages for efficient bulk loading:

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'password': 'mypassword',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',

    # Stage configuration
    'use_stage': True,                    # Default: True
    'stage_name': 'AMP_STAGE',           # Default: 'AMP_STAGE'
    'compression': 'gzip',                # Options: 'gzip', 'none' (default: 'gzip')
}
```

**Performance**: Stage-based loading uses COPY INTO which is much faster than INSERT for large datasets.

### INSERT-Based Loading

For smaller datasets or when stages aren't available:

```python
config = {
    'account': 'myorg-myaccount',
    'user': 'myuser',
    'password': 'mypassword',
    'warehouse': 'COMPUTE_WH',
    'database': 'MY_DATABASE',

    'use_stage': False,  # Use INSERT statements instead
}
```

**When to use**: Development/testing environments or when loading < 1000 rows per batch.

## Usage Examples

### Basic Loading

```python
from amp.client import Client

client = Client("grpc://your-amp-server:80")

# Configure connection
client.configure_connection(
    name='my_snowflake',
    loader='snowflake',
    config={
        'account': 'myorg-myaccount',
        'user': 'myuser',
        'password': 'mypassword',
        'warehouse': 'COMPUTE_WH',
        'database': 'MY_DATABASE',
        'schema': 'RAW_DATA',
    }
)

# Load data
results = client.sql('SELECT * FROM blocks LIMIT 10000').load(
    connection='my_snowflake',
    destination='blocks',
    create_table=True,
)

# Process results
for result in results:
    print(f"Loaded {result.rows_loaded:,} rows in {result.duration:.2f}s")
```

### Streaming Mode

```python
# Stream data continuously
results = client.sql('SELECT * FROM blocks').load(
    connection='my_snowflake',
    destination='blocks',
    stream=True,
    with_reorg_detection=True,
)

for result in results:
    if result.is_reorg:
        print(f"Reorg detected: {len(result.invalidation_ranges)} ranges")
    else:
        print(f"Loaded {result.rows_loaded:,} rows")
```

### Production Configuration with Key Pair Auth

```python
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load private key
key_path = os.path.expanduser("~/.ssh/snowflake_rsa_key.p8")
with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=os.environ.get("SNOWFLAKE_KEY_PASSPHRASE", "").encode(),
        backend=default_backend()
    )

client.configure_connection(
    name='production_snowflake',
    loader='snowflake',
    config={
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': 'PRODUCTION',
        'role': 'DATA_LOADER_ROLE',
        'private_key': private_key,

        'connection_params': {
            'application': 'amp-python-production',
            'session_parameters': {
                'QUERY_TAG': 'amp-blockchain-load',
                'TIMEZONE': 'UTC',
            },
            'client_session_keep_alive': True,
        },
    }
)
```

## Type Mapping

Arrow types are automatically mapped to Snowflake types:

| Arrow Type | Snowflake Type |
|------------|----------------|
| int8, int16 | SMALLINT |
| int32 | INTEGER |
| int64, uint32, uint64 | BIGINT |
| float32 | FLOAT |
| float64 | DOUBLE |
| string, large_string | VARCHAR |
| binary, large_binary | BINARY |
| bool | BOOLEAN |
| date32, date64 | DATE |
| time32, time64 | TIME |
| timestamp (no tz) | TIMESTAMP_NTZ |
| timestamp (with tz) | TIMESTAMP_TZ |
| decimal | NUMBER(precision, scale) |
| list, struct, map | VARIANT/OBJECT |

## Performance Tuning

### Warehouse Sizing

Choose warehouse size based on data volume:

```python
config = {
    'warehouse': 'LARGE_WH',  # Options: X-SMALL, SMALL, MEDIUM, LARGE, X-LARGE, etc.
    # ...
}
```

**Guidelines**:
- X-SMALL/SMALL: < 10M rows/hour
- MEDIUM: 10M-100M rows/hour
- LARGE: 100M-500M rows/hour
- X-LARGE+: > 500M rows/hour

### Batch Size

```python
results = client.sql(query).load(
    connection='my_snowflake',
    destination='blocks',
    batch_size=50000,  # Default: 10000
)
```

**Guidelines**:
- 10,000 rows: Good for most use cases
- 50,000 rows: Better for high-throughput scenarios
- 100,000+ rows: Best for bulk historical loads

### Compression

```python
config = {
    'compression': 'gzip',  # Options: 'gzip', 'none'
    # ...
}
```

- `gzip`: Reduces network transfer, recommended for most cases
- `none`: Faster CPU usage, use for local/high-bandwidth connections

## Troubleshooting

### Authentication Errors

**Issue**: `Authentication failed`

**Solutions**:
1. Verify account identifier format: `orgname-accountname` (not `accountname.snowflakecomputing.com`)
2. Check user permissions in Snowflake
3. Verify warehouse is started: `ALTER WAREHOUSE my_wh RESUME;`

### Permission Errors

**Issue**: `Insufficient privileges`

**Solution**: Grant necessary permissions:
```sql
-- Grant database and schema permissions
GRANT USAGE ON DATABASE my_database TO ROLE data_loader;
GRANT CREATE TABLE ON SCHEMA my_database.public TO ROLE data_loader;
GRANT INSERT ON ALL TABLES IN SCHEMA my_database.public TO ROLE data_loader;

-- Grant warehouse permission
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE data_loader;

-- Grant stage permission
GRANT CREATE STAGE ON SCHEMA my_database.public TO ROLE data_loader;
```

### Stage Errors

**Issue**: `Stage not found or insufficient permissions`

**Solutions**:
1. Verify stage exists: `SHOW STAGES IN SCHEMA my_database.public;`
2. Grant stage permissions: `GRANT USAGE ON STAGE amp_stage TO ROLE data_loader;`
3. Or disable stage loading: `'use_stage': False`

### Timeout Errors

**Issue**: `Connection timeout` or `Socket timeout`

**Solution**: Increase timeouts via `connection_params`:
```python
config = {
    # ...
    'connection_params': {
        'login_timeout': 120,      # Increase from 60s
        'network_timeout': 600,    # Increase from 300s
        'socket_timeout': 600,     # Increase from 300s
    }
}
```

## Blockchain Reorganization Handling

The Snowflake loader automatically handles blockchain reorganizations when using streaming mode:

```python
results = client.sql('SELECT * FROM blocks').load(
    connection='my_snowflake',
    destination='blocks',
    stream=True,
    with_reorg_detection=True,  # Enable reorg detection
)

for result in results:
    if result.is_reorg:
        # Reorg was detected and handled automatically
        print(f"Reorg: {len(result.invalidation_ranges)} ranges invalidated")
        # Affected rows were automatically deleted from Snowflake
```

**Requirements**:
- Table must have `_meta_block_ranges` column (automatically added)
- DELETE permission required on target table

**How it works**:
1. Reorg detected in streaming data
2. Snowflake loader uses JSON functions to identify affected rows
3. DELETE query removes invalidated data
4. New correct data is loaded

## Environment Variables

For security, use environment variables for credentials:

```bash
# .env file
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=myuser
SNOWFLAKE_PASSWORD=mypassword
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MY_DATABASE
```

```python
import os
from dotenv import load_dotenv

load_dotenv()

client.configure_connection(
    name='my_snowflake',
    loader='snowflake',
    config={
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
    }
)
```

## See Also

- [Snowflake Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Snowflake COPY INTO Documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html)
- [Implementing Data Loaders](./implementing_data_loaders.md) - Guide for creating custom loaders