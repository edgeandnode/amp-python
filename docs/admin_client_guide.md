# Amp Admin Client Guide

The Amp Admin Client provides Python bindings for the Amp Admin API, enabling you to register datasets, deploy jobs, and manage your Amp infrastructure programmatically.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Client Configuration](#client-configuration)
- [Dataset Operations](#dataset-operations)
- [Job Management](#job-management)
- [Schema Validation](#schema-validation)
- [Manifest Generation](#manifest-generation)
- [Deployment Workflows](#deployment-workflows)
- [Error Handling](#error-handling)

## Installation

The admin client is included in the `amp` package:

```bash
pip install amp
```

Or with `uv`:

```bash
uv add amp
```

## Quick Start

### Basic Client Setup

```python
from amp import Client

# Initialize client with both query and admin capabilities
client = Client(
    query_url="grpc://localhost:8815",  # Flight SQL endpoint
    admin_url="http://localhost:8080",   # Admin API endpoint
    auth_token="your-auth-token"         # Optional authentication
)
```

### Register a Dataset

```python
# Define your dataset manifest
manifest = {
    'kind': 'manifest',
    'dependencies': {
        'eth': '_/eth_firehose@1.0.0'
    },
    'tables': {
        'blocks': {
            'input': {'sql': 'SELECT * FROM eth.blocks'},
            'schema': {'arrow': {'fields': [...]}},
            'network': 'mainnet'
        }
    },
    'functions': {}
}

# Register the dataset
client.datasets.register(
    namespace='_',
    name='my_dataset',
    version='1.0.0',
    manifest=manifest
)
```

### Deploy and Monitor

```python
# Deploy the dataset
deploy_response = client.datasets.deploy(
    namespace='_',
    name='my_dataset',
    version='1.0.0',
    parallelism=4,
    end_block='latest'
)

# Wait for completion
job = client.jobs.wait_for_completion(
    deploy_response.job_id,
    poll_interval=5.0,
    timeout=3600.0
)

print(f"Job completed with status: {job.status}")
```

## Core Concepts

### Manifests

A manifest is a JSON document that defines a dataset's structure, dependencies, tables, and functions. Manifests include:

- **dependencies**: References to other datasets this dataset depends on
- **tables**: SQL transformations and output schemas
- **functions**: Custom Python/SQL functions (optional)
- **network**: Blockchain network identifier

### Datasets and Versions

Datasets are versioned using semantic versioning (e.g., `1.0.0`). Each version has:

- A unique manifest
- Immutable registration
- Independent deployment history

### Jobs

Jobs represent long-running operations like dataset deployments. Jobs have states:

- **Pending**: Queued for execution
- **Running**: Currently executing
- **Completed**: Successfully finished
- **Failed**: Encountered an error
- **Cancelled**: Stopped by user

## Client Configuration

### Unified Client

The `Client` class provides both query and admin functionality:

```python
from amp import Client

# Full configuration
client = Client(
    query_url="grpc://localhost:8815",
    admin_url="http://localhost:8080",
    auth_token="your-token"
)

# Query operations (Flight SQL)
df = client.sql("SELECT * FROM eth.blocks LIMIT 10").to_pandas()

# Admin operations (HTTP API)
datasets = client.datasets.list_all()
```

### Admin-Only Client

If you only need admin functionality:

```python
from amp.admin import AdminClient

admin = AdminClient(
    base_url="http://localhost:8080",
    auth_token="your-token"
)

# Access admin operations
admin.datasets.list_all()
admin.jobs.get(123)
```

### Backward Compatibility

The legacy `url` parameter still works for Flight SQL:

```python
# This still works
client = Client(url="grpc://localhost:8815")
client.sql("SELECT * FROM eth.blocks")
```

### Environment Variables

You can configure the client using environment variables:

```bash
export AMP_QUERY_URL="grpc://localhost:8815"
export AMP_ADMIN_URL="http://localhost:8080"
export AMP_AUTH_TOKEN="your-token"
```

```python
import os
from amp import Client

client = Client(
    query_url=os.getenv('AMP_QUERY_URL'),
    admin_url=os.getenv('AMP_ADMIN_URL'),
    auth_token=os.getenv('AMP_AUTH_TOKEN')
)
```

## Dataset Operations

### Registering Datasets

```python
# Simple registration
client.datasets.register(
    namespace='_',
    name='eth_blocks',
    version='1.0.0',
    manifest=manifest
)

# Registration without explicit version (server assigns)
client.datasets.register(
    namespace='_',
    name='eth_blocks',
    manifest=manifest
)
```

### Listing Datasets

```python
# List all datasets
response = client.datasets.list_all()

for dataset in response.datasets:
    print(f"{dataset.namespace}/{dataset.name}@{dataset.latest_version}")
    print(f"  Available versions: {dataset.versions}")
```

### Getting Dataset Versions

```python
# Get all versions of a dataset
versions_response = client.datasets.get_versions('_', 'eth_blocks')

print(f"Latest: {versions_response.special_tags.latest}")
print(f"Dev: {versions_response.special_tags.dev}")

for version_info in versions_response.versions:
    print(f"  {version_info.version} - {version_info.manifest_hash}")
```

### Getting Version Details

```python
# Get specific version info
version = client.datasets.get_version('_', 'eth_blocks', '1.0.0')
print(f"Manifest hash: {version.manifest_hash}")
print(f"Created: {version.created_at}")
```

### Getting Manifests

```python
# Retrieve the manifest for a version
manifest = client.datasets.get_manifest('_', 'eth_blocks', '1.0.0')

print(f"Tables: {list(manifest['tables'].keys())}")
print(f"Dependencies: {manifest['dependencies']}")
```

### Deploying Datasets

```python
# Deploy with options
deploy_response = client.datasets.deploy(
    namespace='_',
    name='eth_blocks',
    version='1.0.0',
    parallelism=8,           # Number of parallel workers
    end_block='latest'       # Stop at latest block (vs continuous)
)

print(f"Started job: {deploy_response.job_id}")
```

### Deleting Datasets

```python
# Delete all versions of a dataset
client.datasets.delete('_', 'old_dataset')
```

## Job Management

### Getting Job Status

```python
# Get job by ID
job = client.jobs.get(123)

print(f"Status: {job.status}")
print(f"Node: {job.node_id}")
print(f"Descriptor: {job.descriptor}")
```

### Listing Jobs

```python
# List jobs with pagination
response = client.jobs.list(limit=50)

for job in response.jobs:
    print(f"Job {job.id}: {job.status}")

# Continue pagination if needed
if response.next_cursor:
    next_page = client.jobs.list(
        limit=50,
        last_job_id=response.next_cursor
    )
```

### Waiting for Completion

```python
# Block until job completes or times out
try:
    final_job = client.jobs.wait_for_completion(
        job_id=123,
        poll_interval=5.0,    # Check every 5 seconds
        timeout=3600.0        # Give up after 1 hour
    )

    if final_job.status == 'Completed':
        print("Job succeeded!")
    elif final_job.status == 'Failed':
        print("Job failed!")

except TimeoutError as e:
    print(f"Job did not complete in time: {e}")
```

### Stopping Jobs

```python
# Stop a running job
client.jobs.stop(123)
```

### Deleting Jobs

```python
# Delete a single job
client.jobs.delete(123)

# Delete multiple jobs
client.jobs.delete_many([123, 124, 125])
```

## Schema Validation

The schema client validates SQL queries and returns their output schemas without execution:

```python
# Validate a query and get its schema
schema_response = client.schema.get_output_schema(
    sql_query='SELECT block_num, hash, timestamp FROM eth.blocks WHERE block_num > 1000000',
    dependencies={'eth': '_/eth_firehose@1.0.0'}
)

# Inspect the Arrow schema
print(schema_response.schema)
```

This is particularly useful for:

- Validating queries before registration
- Understanding output structure
- Generating correct Arrow schemas for manifests

## Manifest Generation

The QueryBuilder provides a fluent API for generating manifests from SQL queries:

### Basic Manifest Generation

```python
# Build a query
query = client.sql("SELECT block_num, hash FROM eth.blocks")

# Add dependencies
query = query.with_dependency('eth', '_/eth_firehose@1.0.0')

# Generate manifest
manifest = query.to_manifest(
    table_name='blocks',
    network='mainnet'
)

print(manifest)
# {
#     'kind': 'manifest',
#     'dependencies': {'eth': '_/eth_firehose@1.0.0'},
#     'tables': {
#         'blocks': {
#             'input': {'sql': 'SELECT block_num, hash FROM eth.blocks'},
#             'schema': {'arrow': {...}},  # Auto-fetched
#             'network': 'mainnet'
#         }
#     },
#     'functions': {}
# }
```

### One-Line Registration and Deployment

The most powerful pattern combines query building, manifest generation, registration, and deployment:

```python
# Build, register, and deploy in one chain
job = (
    client.sql("SELECT block_num, hash FROM eth.blocks")
    .with_dependency('eth', '_/eth_firehose@1.0.0')
    .register_as(
        namespace='_',
        name='eth_blocks_simple',
        version='1.0.0',
        table_name='blocks',
        network='mainnet'
    )
    .deploy(
        end_block='latest',
        parallelism=4,
        wait=True  # Block until completion
    )
)

print(f"Deployment completed: {job.status}")
```

### Multiple Dependencies

```python
manifest = (
    client.sql("""
        SELECT
            t.token_address,
            t.amount,
            m.name,
            m.symbol
        FROM erc20_transfers t
        JOIN token_metadata m ON t.token_address = m.address
    """)
    .with_dependency('erc20_transfers', '_/erc20_transfers@1.0.0')
    .with_dependency('token_metadata', '_/token_metadata@1.0.0')
    .to_manifest('enriched_transfers', 'mainnet')
)
```

## Deployment Workflows

### Development Workflow

```python
# 1. Develop query locally
query = client.sql("""
    SELECT
        block_num,
        COUNT(*) as tx_count
    FROM eth.transactions
    GROUP BY block_num
""")

# Test the query
df = query.to_pandas()
print(df.head())

# 2. Register as dataset
query = query.with_dependency('eth', '_/eth_firehose@1.0.0')

client.datasets.register(
    namespace='_',
    name='tx_counts',
    version='0.1.0',
    manifest=query.to_manifest('tx_counts', 'mainnet')
)

# 3. Deploy to limited range for testing
deploy_resp = client.datasets.deploy(
    namespace='_',
    name='tx_counts',
    version='0.1.0',
    end_block='10000',  # Test on first 10k blocks
    parallelism=2
)

# 4. Monitor
job = client.jobs.wait_for_completion(deploy_resp.job_id, timeout=600)

if job.status == 'Completed':
    print("Test deployment successful!")

    # 5. Deploy full version
    prod_deploy = client.datasets.deploy(
        namespace='_',
        name='tx_counts',
        version='0.1.0',
        end_block='latest',
        parallelism=8
    )
```

### Production Workflow

```python
# Register production version
context = (
    client.sql("SELECT * FROM processed_data")
    .with_dependency('raw', '_/raw_data@2.0.0')
    .register_as('_', 'processed_data', '2.0.0', 'data', 'mainnet')
)

# Deploy without waiting
deploy_resp = context.deploy(
    end_block='latest',
    parallelism=16,
    wait=False
)

print(f"Started production deployment: {deploy_resp.job_id}")

# Monitor separately (e.g., in a monitoring service)
def monitor_job(job_id):
    while True:
        job = client.jobs.get(job_id)

        if job.status in ['Completed', 'Failed', 'Cancelled']:
            return job

        print(f"Job {job_id} status: {job.status}")
        time.sleep(30)

final_job = monitor_job(deploy_resp.job_id)
```

### Continuous Deployment

```python
# Deploy continuous processing (no end_block)
deploy_resp = client.datasets.deploy(
    namespace='_',
    name='realtime_data',
    version='1.0.0',
    parallelism=4
    # end_block=None means continuous
)

# Job will run indefinitely, processing new blocks as they arrive
print(f"Continuous deployment started: {deploy_resp.job_id}")

# Stop later when needed
client.jobs.stop(deploy_resp.job_id)
```

## Error Handling

The admin client provides typed exceptions for different error scenarios:

### Error Types

```python
from amp.admin.errors import (
    AdminAPIError,           # Base exception
    DatasetNotFoundError,
    InvalidManifestError,
    JobNotFoundError,
    DependencyValidationError,
    InternalServerError,
)
```

### Handling Errors

```python
try:
    client.datasets.register('_', 'my_dataset', '1.0.0', manifest)

except InvalidManifestError as e:
    print(f"Manifest validation failed: {e.message}")
    print(f"Error code: {e.error_code}")

except DependencyValidationError as e:
    print(f"Dependency issue: {e.message}")

except AdminAPIError as e:
    print(f"API error: {e.error_code} - {e.message}")
    print(f"HTTP status: {e.status_code}")
```

### Robust Deployment

```python
def robust_deploy(client, namespace, name, version, **deploy_options):
    """Deploy with comprehensive error handling."""
    try:
        # Check if dataset exists
        try:
            version_info = client.datasets.get_version(namespace, name, version)
            print(f"Found existing version: {version_info.manifest_hash}")
        except DatasetNotFoundError:
            raise ValueError(f"Dataset {namespace}/{name}@{version} not registered")

        # Deploy
        deploy_resp = client.datasets.deploy(
            namespace, name, version, **deploy_options
        )

        # Wait for completion
        job = client.jobs.wait_for_completion(
            deploy_resp.job_id,
            poll_interval=5.0,
            timeout=3600.0
        )

        if job.status == 'Completed':
            print(f"Deployment successful: job {job.id}")
            return job
        else:
            raise RuntimeError(f"Job failed with status: {job.status}")

    except TimeoutError:
        print("Deployment timeout - job may still be running")
        raise

    except AdminAPIError as e:
        print(f"API error during deployment: {e.message}")
        raise

# Usage
job = robust_deploy(
    client,
    namespace='_',
    name='my_dataset',
    version='1.0.0',
    parallelism=4,
    end_block='latest'
)
```

## Best Practices

### 1. Use Context Managers

```python
with Client(query_url=..., admin_url=..., auth_token=...) as client:
    # Client will automatically close connections
    client.datasets.register(...)
```

### 2. Validate Schemas Early

```python
# Validate before registration
schema = client.schema.get_output_schema(sql_query, dependencies={...})
print(f"Query will produce {len(schema.schema['fields'])} columns")
```

### 3. Version Your Datasets

```python
# Use semantic versioning
# - Major: Breaking schema changes
# - Minor: Backward-compatible additions
# - Patch: Bug fixes

client.datasets.register('_', 'my_dataset', '1.0.0', manifest_v1)
client.datasets.register('_', 'my_dataset', '1.1.0', manifest_v1_1)  # Added columns
client.datasets.register('_', 'my_dataset', '2.0.0', manifest_v2)    # Breaking change
```

### 4. Monitor Long-Running Jobs

```python
# Don't block main thread for long deployments
deploy_resp = client.datasets.deploy(..., wait=False)

# Monitor asynchronously
import threading

def monitor():
    job = client.jobs.wait_for_completion(deploy_resp.job_id)
    print(f"Job finished: {job.status}")

thread = threading.Thread(target=monitor)
thread.start()
```

### 5. Handle Dependencies Correctly

```python
# Always specify full dependency references
query = (
    client.sql("SELECT * FROM base.data")
    .with_dependency('base', '_/base_dataset@1.0.0')  # Include version!
)

# Not: .with_dependency('base', 'base_dataset')  # ❌ Missing namespace/version
```

## Next Steps

- See [API Reference](api/client_api.md) for complete API documentation
- Check [examples/admin/](../examples/admin/) for more code samples
- Review the [Admin API OpenAPI spec](../specs/admin.spec.json) for endpoint details
