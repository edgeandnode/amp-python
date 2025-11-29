# Python Amp Client

[![Unit tests status](https://github.com/edgeandnode/amp-python/actions/workflows/unit-tests.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/unit-tests.yml)
[![Integration tests status](https://github.com/edgeandnode/amp-python/actions/workflows/integration-tests.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/integration-tests.yml)
[![Formatting status](https://github.com/edgeandnode/amp-python/actions/workflows/ruff.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/ruff.yml)


## Overview

Python client for Amp - a database for blockchain data.

**Features:**
- **Query Client**: Issue Flight SQL queries to Amp servers
- **Admin Client**: Manage datasets, deployments, and jobs programmatically
- **Registry Client**: Discover, search, and publish datasets to the Registry
- **Dataset Inspection**: Explore dataset schemas with `inspect()` and `describe()` methods
- **Data Loaders**: Zero-copy loading into PostgreSQL, Redis, Snowflake, Delta Lake, Iceberg, and more
- **Parallel Streaming**: High-throughput parallel data ingestion with automatic resume
- **Manifest Generation**: Fluent API for creating and deploying datasets from SQL queries
- **Auto-Refreshing Auth**: Seamless authentication with automatic token refresh

## Dependencies
1. Rust
   `brew install rust`

## Installation
 
1. Ensure you have [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed locally.
2. Install dependencies
    ```bash
    uv build 
   ```
3. Activate a virtual environment

   Python 3.13 is the highest version supported `brew install python@3.13`
    ```bash
    uv venv --python 3.13
   ```

## Quick Start

### Querying Data

```python
from amp import Client

# Connect to Amp server
client = Client(url="grpc://localhost:8815")

# Execute query and convert to pandas
df = client.sql("SELECT * FROM eth.blocks LIMIT 10").to_arrow().to_pandas()
print(df)
```

### Admin Operations

```python
from amp import Client

# Connect with admin capabilities
client = Client(
    query_url="grpc://localhost:8815",
    admin_url="http://localhost:8080",
    auth_token="your-token"
)

# Register and deploy a dataset
job = (
    client.sql("SELECT block_num, hash FROM eth.blocks")
    .with_dependency('eth', '_/eth_firehose@1.0.0')
    .register_as('_', 'my_dataset', '1.0.0', 'blocks', 'mainnet')
    .deploy(parallelism=4, end_block='latest', wait=True)
)

print(f"Deployment completed: {job.status}")
```

### Loading Data

```python
# Load query results into PostgreSQL
result = client.sql("SELECT * FROM eth.blocks").load(
    connection='my_pg_connection',
    destination='eth_blocks'
)
print(f"Loaded {result.rows_loaded} rows")
```

### Authentication

The client supports three authentication methods (in priority order):

```python
from amp import Client

# 1. Explicit token (highest priority)
client = Client(
    url="grpc://localhost:8815",
    auth_token="your-token"
)

# 2. Environment variable
# export AMP_AUTH_TOKEN="your-token"
client = Client(url="grpc://localhost:8815")

# 3. Shared auth file (auto-refresh, recommended)
# Uses ~/.amp/cache/amp_cli_auth (shared with TypeScript CLI)
client = Client(
    url="grpc://localhost:8815",
    auth=True  # Automatically refreshes expired tokens
)
```

### Registry - Discovering Datasets

```python
from amp import Client

# Connect with registry support
client = Client(
    query_url="grpc://localhost:8815",
    registry_url="https://api.registry.amp.staging.thegraph.com",
    auth=True
)

# Search for datasets
results = client.registry.datasets.search('ethereum blocks')
for dataset in results.datasets[:5]:
    print(f"{dataset.namespace}/{dataset.name} - {dataset.description}")

# Get dataset details
dataset = client.registry.datasets.get('edgeandnode', 'ethereum_mainnet')
print(f"Latest version: {dataset.latest_version}")

# Inspect dataset schema
client.registry.datasets.inspect('edgeandnode', 'ethereum_mainnet')
```

### Dataset Inspection

Explore dataset schemas before querying:

```python
from amp.registry import RegistryClient

client = RegistryClient()

# Pretty-print dataset structure (interactive)
client.datasets.inspect('edgeandnode', 'ethereum_mainnet')
# Output:
# Dataset: edgeandnode/ethereum_mainnet@latest
#
# blocks (21 columns)
#   block_num          UInt64                    NOT NULL
#   timestamp          Timestamp(Nanosecond)     NOT NULL
#   hash               FixedSizeBinary(32)       NOT NULL
#   ...

# Get structured schema data (programmatic)
schema = client.datasets.describe('edgeandnode', 'ethereum_mainnet')

# Find tables with specific columns
for table_name, columns in schema.items():
    col_names = [col['name'] for col in columns]
    if 'block_num' in col_names:
        print(f"Table '{table_name}' has block_num column")

# Find all address columns (20-byte binary)
for table_name, columns in schema.items():
    addresses = [col['name'] for col in columns if col['type'] == 'FixedSizeBinary(20)']
    if addresses:
        print(f"{table_name}: {', '.join(addresses)}")
```

## Usage

### Marimo

Start up a marimo workspace editor
```bash
uv run marimo edit
```

The Marimo app will open a new browser tab where you can create a new notebook, view helpful resources, and
browse existing notebooks in the workspace.

### Apps

You can execute python apps and scripts using `uv run <path>` which will give them access to the dependencies
and the `amp` package. For example, you can run the `execute_query` app with the following command.
```bash
uv run apps/execute_query.py
```

## Documentation

### Getting Started
- **[Admin Client Guide](docs/admin_client_guide.md)** - Complete guide for dataset management and deployment
- **[Registry Guide](docs/registry-guide.md)** - Discover and search datasets in the Registry
- **[Dataset Inspection](docs/inspecting_datasets.md)** - Explore dataset schemas with `inspect()` and `describe()`
- **[Admin API Reference](docs/api/client_api.md)** - Full API documentation for admin operations

### Features
- **[Parallel Streaming Usage Guide](docs/parallel_streaming_usage.md)** - User guide for high-throughput parallel data loading
- **[Parallel Streaming Design](docs/parallel_streaming.md)** - Technical design documentation for parallel streaming architecture
- **[Reorganization Handling](docs/reorg_handling.md)** - Guide for handling blockchain reorganizations
- **[Implementing Data Loaders](docs/implementing_data_loaders.md)** - Guide for creating custom data loaders

# Self-hosted Amp server

In order to operate a local Amp server you will need to have the files 
that [`dump`](../dump) produces available locally, and run the [server](../server) 
You can then use it in your python scripts, apps or notebooks.

# Testing

The project is set up to use the [`pytest`](https://docs.pytest.org/en/stable/) testing framework. 
It follows [standard python test discovery rules](https://docs.pytest.org/en/stable/explanation/goodpractices.html#test-discovery). 

## Quick Test Commands

Run all tests
```bash
uv run pytest
```

Run only unit tests (fast, no external dependencies)
```bash
make test-unit
```

Run integration tests with automatic container setup
```bash
make test-integration
```

Run all tests with coverage
```bash
make test-all
```

## Integration Testing

Integration tests can run in two modes:

### 1. Automatic Container Mode (Default)
The integration tests will automatically spin up PostgreSQL and Redis containers using testcontainers. This is the default mode and requires Docker to be installed and running.

```bash
# Run integration tests with automatic containers
uv run pytest tests/integration/ -m integration
```

**Note**: The configuration automatically disables Ryuk (testcontainers cleanup container) to avoid Docker connectivity issues. If you need Ryuk enabled, set `TESTCONTAINERS_RYUK_DISABLED=false`.

### 2. Manual Setup Mode
If you prefer to use your own database instances, you can disable testcontainers:

```bash
# Disable testcontainers and use manual configuration
export USE_TESTCONTAINERS=false

# Configure your database connections
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=test_amp
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=yourpassword

export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=yourpassword  # Optional

# Run tests
uv run pytest tests/integration/ -m integration
```

For manual setup, you can use the provided Makefile commands:
```bash
# Start test databases manually
make test-setup

# Run tests
make test-integration

# Clean up databases
make test-cleanup
```

## Loader-Specific Tests

Run tests for specific loaders:
```bash
make test-postgresql   # PostgreSQL tests
make test-redis       # Redis tests
make test-deltalake   # Delta Lake tests
make test-iceberg     # Iceberg tests
make test-lmdb        # LMDB tests
```

## Feature-Specific Tests

Run tests for specific features:
```bash
make test-parallel-streaming   # Parallel streaming integration tests (requires Amp server)
```

**Note**: Parallel streaming tests require an Amp server. Configure using environment variables in `.test.env`:
- `AMP_SERVER_URL` - Amp server URL (e.g., `grpc://your-server:80`)
- `AMP_TEST_TABLE` - Source table name (e.g., `eth_firehose.blocks`)
- `AMP_TEST_BLOCK_COLUMN` - Block column name (default: `block_num`)
- `AMP_TEST_MAX_BLOCK` - Max block for testing (default: `1000`)

# Linting and formatting

Ruff is configured to be used for linting and formatting of this project. 

Run formatter
```bash
uv run ruff format
```

Run linter 
```bash
uv run ruff check .
```

Run linter and apply auto-fixes
```bash 
uv run ruff check . --fix
```
