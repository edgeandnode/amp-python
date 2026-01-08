# Client API Reference

Complete API reference for the Amp Admin Client.

## Table of Contents

- [Client Classes](#client-classes)
  - [Client](#client)
  - [AdminClient](#adminclient)
  - [DatasetsClient](#datasetsclient)
  - [JobsClient](#jobsclient)
  - [SchemaClient](#schemaclient)
- [Data Models](#data-models)
- [Error Classes](#error-classes)
- [Helper Classes](#helper-classes)

---

## Client Classes

### Client

Main client providing both Flight SQL query operations and admin operations.

**Module:** `amp.client`

#### Constructor

```python
Client(
    url: Optional[str] = None,
    query_url: Optional[str] = None,
    admin_url: Optional[str] = None,
    auth_token: Optional[str] = None
)
```

**Parameters:**

- `url` (str, optional): Legacy parameter for Flight SQL URL. If provided and `query_url` is not, this value is used for `query_url`.
- `query_url` (str, optional): Query endpoint URL via Flight SQL (e.g., `"grpc://localhost:8815"`).
- `admin_url` (str, optional): Admin API HTTP endpoint URL (e.g., `"http://localhost:8080"`).
- `auth_token` (str, optional): Authentication token for Admin API requests.

**Raises:**

- `ValueError`: When accessing admin properties without configuring `admin_url`.

**Example:**

```python
from amp import Client

# Full configuration
client = Client(
    query_url="grpc://localhost:8815",
    admin_url="http://localhost:8080",
    auth_token="my-token"
)

# Query-only (backward compatible)
client = Client(url="grpc://localhost:8815")
```

#### Properties

##### `datasets`

Access the DatasetsClient for dataset operations.

**Returns:** `DatasetsClient`

**Raises:** `ValueError` if `admin_url` was not configured.

##### `jobs`

Access the JobsClient for job operations.

**Returns:** `JobsClient`

**Raises:** `ValueError` if `admin_url` was not configured.

##### `schema`

Access the SchemaClient for schema operations.

**Returns:** `SchemaClient`

**Raises:** `ValueError` if `admin_url` was not configured.

#### Methods

##### `sql(sql: str) -> QueryBuilder`

Create a QueryBuilder for the given SQL query.

**Parameters:**

- `sql` (str): SQL query string.

**Returns:** `QueryBuilder` instance.

**Example:**

```python
qb = client.sql("SELECT * FROM eth.blocks LIMIT 10")
df = qb.to_pandas()
```

---

### AdminClient

Low-level HTTP client for the Admin API. Typically you'll use the unified `Client` class instead.

**Module:** `amp.admin.client`

#### Constructor

```python
AdminClient(
    base_url: str,
    auth_token: Optional[str] = None
)
```

**Parameters:**

- `base_url` (str): Base URL for the Admin API (e.g., `"http://localhost:8080"`).
- `auth_token` (str, optional): Authentication token. If provided, adds `Authorization: Bearer <token>` header.

**Example:**

```python
from amp.admin import AdminClient

admin = AdminClient(
    base_url="http://localhost:8080",
    auth_token="my-token"
)
```

#### Properties

##### `datasets`

Access the DatasetsClient.

**Returns:** `DatasetsClient`

##### `jobs`

Access the JobsClient.

**Returns:** `JobsClient`

##### `schema`

Access the SchemaClient.

**Returns:** `SchemaClient`

#### Methods

##### `close()`

Close the HTTP client connection.

**Example:**

```python
admin.close()
```

#### Context Manager

AdminClient can be used as a context manager:

```python
with AdminClient("http://localhost:8080") as admin:
    admin.datasets.list_all()
# Connection automatically closed
```

---

### DatasetsClient

Client for dataset registration, deployment, and management operations.

**Module:** `amp.admin.datasets`

#### Methods

##### `register()`

Register a new dataset or dataset version.

```python
register(
    namespace: str,
    name: str,
    version: Optional[str],
    manifest: dict
) -> None
```

**Parameters:**

- `namespace` (str): Dataset namespace (e.g., `"_"`).
- `name` (str): Dataset name.
- `version` (str, optional): Semantic version string (e.g., `"1.0.0"`). If not provided, server assigns version.
- `manifest` (dict): Dataset manifest dictionary.

**Raises:**

- `InvalidManifestError`: If manifest validation fails.
- `DependencyValidationError`: If referenced dependencies don't exist.

**Example:**

```python
manifest = {
    'kind': 'manifest',
    'dependencies': {'eth': '_/eth_firehose@1.0.0'},
    'tables': {'blocks': {...}},
    'functions': {}
}

client.datasets.register('_', 'my_dataset', '1.0.0', manifest)
```

##### `deploy()`

Deploy a registered dataset version.

```python
deploy(
    namespace: str,
    name: str,
    version: str,
    parallelism: Optional[int] = None,
    end_block: Optional[str] = None
) -> models.DeployResponse
```

**Parameters:**

- `namespace` (str): Dataset namespace.
- `name` (str): Dataset name.
- `version` (str): Version to deploy.
- `parallelism` (int, optional): Number of parallel workers.
- `end_block` (str, optional): Block to stop at (e.g., `"latest"`, `"1000000"`). If not provided, runs continuously.

**Returns:** `DeployResponse` with `job_id` field.

**Raises:**

- `DatasetNotFoundError`: If dataset/version doesn't exist.

**Example:**

```python
response = client.datasets.deploy(
    '_', 'my_dataset', '1.0.0',
    parallelism=4,
    end_block='latest'
)
print(f"Job ID: {response.job_id}")
```

##### `list_all()`

List all registered datasets.

```python
list_all() -> models.ListDatasetsResponse
```

**Returns:** `ListDatasetsResponse` containing list of `DatasetSummary` objects.

**Example:**

```python
response = client.datasets.list_all()

for dataset in response.datasets:
    print(f"{dataset.namespace}/{dataset.name}@{dataset.latest_version}")
```

##### `get_versions()`

Get all versions of a dataset.

```python
get_versions(
    namespace: str,
    name: str
) -> models.VersionsResponse
```

**Parameters:**

- `namespace` (str): Dataset namespace.
- `name` (str): Dataset name.

**Returns:** `VersionsResponse` with `versions` list and `special_tags` dict.

**Raises:**

- `DatasetNotFoundError`: If dataset doesn't exist.

**Example:**

```python
response = client.datasets.get_versions('_', 'eth_blocks')

print(f"Latest: {response.special_tags.latest}")
for version in response.versions:
    print(f"  {version.version}")
```

##### `get_version()`

Get details of a specific dataset version.

```python
get_version(
    namespace: str,
    name: str,
    version: str
) -> models.VersionInfo
```

**Parameters:**

- `namespace` (str): Dataset namespace.
- `name` (str): Dataset name.
- `version` (str): Version string.

**Returns:** `VersionInfo` with version metadata.

**Raises:**

- `DatasetNotFoundError`: If dataset or version doesn't exist.

**Example:**

```python
info = client.datasets.get_version('_', 'eth_blocks', '1.0.0')
print(f"Manifest hash: {info.manifest_hash}")
print(f"Created: {info.created_at}")
```

##### `get_manifest()`

Retrieve the manifest for a specific dataset version.

```python
get_manifest(
    namespace: str,
    name: str,
    version: str
) -> dict
```

**Parameters:**

- `namespace` (str): Dataset namespace.
- `name` (str): Dataset name.
- `version` (str): Version string.

**Returns:** Manifest dictionary.

**Raises:**

- `DatasetNotFoundError`: If dataset or version doesn't exist.

**Example:**

```python
manifest = client.datasets.get_manifest('_', 'eth_blocks', '1.0.0')
print(f"Tables: {list(manifest['tables'].keys())}")
```

##### `delete()`

Delete a dataset and all its versions.

```python
delete(
    namespace: str,
    name: str
) -> None
```

**Parameters:**

- `namespace` (str): Dataset namespace.
- `name` (str): Dataset name.

**Raises:**

- `DatasetNotFoundError`: If dataset doesn't exist.

**Example:**

```python
client.datasets.delete('_', 'old_dataset')
```

---

### JobsClient

Client for job monitoring and management.

**Module:** `amp.admin.jobs`

#### Methods

##### `get()`

Get details of a specific job.

```python
get(job_id: int) -> models.JobInfo
```

**Parameters:**

- `job_id` (int): Job ID.

**Returns:** `JobInfo` with job details.

**Raises:**

- `JobNotFoundError`: If job doesn't exist.

**Example:**

```python
job = client.jobs.get(123)
print(f"Status: {job.status}")
print(f"Node: {job.node_id}")
```

##### `list()`

List jobs with pagination.

```python
list(
    limit: int = 100,
    last_job_id: Optional[int] = None
) -> models.ListJobsResponse
```

**Parameters:**

- `limit` (int, optional): Maximum number of jobs to return. Default: 100.
- `last_job_id` (int, optional): Cursor for pagination. Returns jobs after this ID.

**Returns:** `ListJobsResponse` with `jobs` list and optional `next_cursor`.

**Example:**

```python
# First page
response = client.jobs.list(limit=50)
for job in response.jobs:
    print(f"Job {job.id}: {job.status}")

# Next page
if response.next_cursor:
    next_page = client.jobs.list(limit=50, last_job_id=response.next_cursor)
```

##### `wait_for_completion()`

Poll job status until completion or timeout.

```python
wait_for_completion(
    job_id: int,
    poll_interval: float = 5.0,
    timeout: float = 3600.0
) -> models.JobInfo
```

**Parameters:**

- `job_id` (int): Job ID to monitor.
- `poll_interval` (float, optional): Seconds between status checks. Default: 5.0.
- `timeout` (float, optional): Maximum seconds to wait. Default: 3600.0 (1 hour).

**Returns:** `JobInfo` with final job status.

**Raises:**

- `TimeoutError`: If job doesn't complete within timeout.
- `JobNotFoundError`: If job doesn't exist.

**Example:**

```python
try:
    job = client.jobs.wait_for_completion(
        job_id=123,
        poll_interval=5.0,
        timeout=1800.0  # 30 minutes
    )

    if job.status == 'Completed':
        print("Success!")
    else:
        print(f"Job ended with status: {job.status}")

except TimeoutError:
    print("Job timed out")
```

##### `stop()`

Stop a running job.

```python
stop(job_id: int) -> None
```

**Parameters:**

- `job_id` (int): Job ID to stop.

**Raises:**

- `JobNotFoundError`: If job doesn't exist.

**Example:**

```python
client.jobs.stop(123)
```

##### `delete()`

Delete a single job.

```python
delete(job_id: int) -> None
```

**Parameters:**

- `job_id` (int): Job ID to delete.

**Raises:**

- `JobNotFoundError`: If job doesn't exist.

**Example:**

```python
client.jobs.delete(123)
```

##### `delete_many()`

Delete multiple jobs.

```python
delete_many(job_ids: list[int]) -> None
```

**Parameters:**

- `job_ids` (list[int]): List of job IDs to delete.

**Example:**

```python
client.jobs.delete_many([123, 124, 125])
```

---

### SchemaClient

Client for SQL query validation and schema inference.

**Module:** `amp.admin.schema`

#### Methods

##### `get_output_schema()`

Validate SQL query and get its output Arrow schema without executing it.

```python
get_output_schema(
    tables: Optional[dict[str, str]] = None,
    dependencies: Optional[dict[str, str]] = None,
    functions: Optional[dict[str, Any]] = None
) -> models.SchemaResponse
```

**Parameters:**

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `tables` | `dict[str, str]` | `None` | Optional map of table names to SQL queries |
| `dependencies` | `dict[str, str]` | `None` | Optional map of alias -> dataset reference |
| `functions` | `dict[str, Any]` | `None` | Optional map of function definitions |

**Returns:** `SchemaResponse` containing schemas for all requested tables.

**Raises:**

- `GetOutputSchemaError`: If schema analysis fails.
- `DependencyValidationError`: If query references invalid dependencies.

**Example:**

```python
response = client.schema.get_output_schema(
    tables={'my_table': 'SELECT block_num FROM eth.blocks'},
    dependencies={'eth': '_/eth_firehose@1.0.0'}
)

print(response.schemas['my_table'].schema)  # Arrow schema dict
```

---

## Data Models

All data models are Pydantic v2 models defined in `amp.admin.models`.

### Core Models

#### `DatasetSummary`

Summary information about a dataset.

**Fields:**

- `namespace` (str): Dataset namespace
- `name` (str): Dataset name
- `latest_version` (str): Latest version string
- `versions` (list[str]): All available versions

#### `VersionInfo`

Detailed information about a dataset version.

**Fields:**

- `version` (str): Version string
- `manifest_hash` (str): Hash of the manifest
- `created_at` (str): ISO timestamp
- `updated_at` (str): ISO timestamp

#### `VersionsResponse`

Response containing all versions of a dataset.

**Fields:**

- `namespace` (str): Dataset namespace
- `name` (str): Dataset name
- `versions` (list[VersionInfo]): List of version details
- `special_tags` (SpecialTags): Special version tags

#### `SpecialTags`

Special version tags for a dataset.

**Fields:**

- `latest` (str): Latest stable version
- `dev` (str, optional): Development version

#### `JobInfo`

Information about a job.

**Fields:**

- `id` (int): Job ID
- `status` (str): Job status (`"Pending"`, `"Running"`, `"Completed"`, `"Failed"`, `"Cancelled"`)
- `descriptor` (dict): Job configuration
- `node_id` (str, optional): Worker node ID

#### `ListJobsResponse`

Response from listing jobs.

**Fields:**

- `jobs` (list[JobInfo]): List of jobs
- `next_cursor` (int, optional): Cursor for next page

#### `ListDatasetsResponse`

Response from listing datasets.

**Fields:**

- `datasets` (list[DatasetSummary]): List of dataset summaries

#### `DeployResponse`

Response from deploying a dataset.

**Fields:**

- `job_id` (int): ID of the created job

#### `SchemaResponse`

Response containing schemas for one or more tables.

**Fields:**

- `schemas` (dict[str, TableSchemaWithNetworks]): Map of table names to their schemas

#### `TableSchemaWithNetworks`

Response containing Arrow schema for a query and associated networks.

**Fields:**

- `schema` (dict): Arrow schema dictionary
- `networks` (list[str]): List of referenced networks

### Request Models

#### `RegisterRequest`

Request to register a dataset.

**Fields:**

- `namespace` (str): Dataset namespace
- `name` (str): Dataset name
- `version` (str, optional): Version string
- `manifest` (dict): Dataset manifest

#### `SchemaRequest`

Request for schema analysis with dependencies, tables, and functions.

**Fields:**

- `dependencies` (dict[str, str], optional): External dataset dependencies
- `tables` (dict[str, str], optional): Table definitions
- `functions` (dict[str, Any], optional): User-defined functions

---

## Error Classes

All error classes are defined in `amp.admin.errors`.

### Base Error

#### `AdminAPIError`

Base exception for all Admin API errors.

**Attributes:**

- `error_code` (str): Error code from API
- `message` (str): Human-readable error message
- `status_code` (int): HTTP status code

**Example:**

```python
try:
    client.datasets.register(...)
except AdminAPIError as e:
    print(f"Error: {e.error_code} - {e.message}")
    print(f"HTTP Status: {e.status_code}")
```

### Specific Errors

All specific errors inherit from `AdminAPIError`:

- `DatasetNotFoundError`: Dataset or version not found (404)
- `InvalidManifestError`: Manifest validation failed (400)
- `JobNotFoundError`: Job not found (404)
- `DependencyValidationError`: Invalid dependency reference (400)
- `GetOutputSchemaError`: Schema analysis failed (400)
- `InvalidDependencyError`: Malformed dependency specification (400)
- `InternalServerError`: Server error (500)
- `BadGatewayError`: Gateway error (502)
- `ServiceUnavailableError`: Service unavailable (503)
- `GatewayTimeoutError`: Gateway timeout (504)

**Usage:**

```python
from amp.admin.errors import DatasetNotFoundError, InvalidManifestError

try:
    client.datasets.get_version('_', 'nonexistent', '1.0.0')
except DatasetNotFoundError:
    print("Dataset not found")

try:
    client.datasets.register('_', 'bad', '1.0.0', {})
except InvalidManifestError as e:
    print(f"Manifest invalid: {e.message}")
```

---

## Helper Classes

### QueryBuilder

Fluent API for building SQL queries and generating manifests.

**Module:** `amp.client`

#### Methods

##### `with_dependency()`

Add a dependency to the query.

```python
with_dependency(alias: str, reference: str) -> QueryBuilder
```

**Parameters:**

- `alias` (str): Dependency alias used in SQL (e.g., `"eth"`)
- `reference` (str): Full dependency reference (e.g., `"_/eth_firehose@1.0.0"`)

**Returns:** Self for chaining.

**Example:**

```python
qb = (
    client.sql("SELECT * FROM eth.blocks")
    .with_dependency('eth', '_/eth_firehose@1.0.0')
)
```

##### `to_manifest()`

Generate a dataset manifest from the query.

```python
to_manifest(table_name: str, network: str = 'mainnet') -> dict
```

**Parameters:**

- `table_name` (str): Name for the output table
- `network` (str, optional): Network identifier. Default: `"mainnet"`.

**Returns:** Manifest dictionary.

**Example:**

```python
manifest = (
    client.sql("SELECT * FROM eth.blocks")
    .with_dependency('eth', '_/eth_firehose@1.0.0')
    .to_manifest('blocks', 'mainnet')
)
```

##### `register_as()`

Register the query as a dataset and return a deployment context.

```python
register_as(
    namespace: str,
    name: str,
    version: str,
    table_name: str,
    network: str = 'mainnet'
) -> DeploymentContext
```

**Parameters:**

- `namespace` (str): Dataset namespace
- `name` (str): Dataset name
- `version` (str): Version string
- `table_name` (str): Output table name
- `network` (str, optional): Network identifier. Default: `"mainnet"`.

**Returns:** `DeploymentContext` for chaining deployment.

**Example:**

```python
job = (
    client.sql("SELECT * FROM eth.blocks")
    .with_dependency('eth', '_/eth_firehose@1.0.0')
    .register_as('_', 'my_dataset', '1.0.0', 'blocks')
    .deploy(parallelism=4, wait=True)
)
```

### DeploymentContext

Context for deploying a registered dataset with fluent API.

**Module:** `amp.admin.deployment`

#### Methods

##### `deploy()`

Deploy the registered dataset.

```python
deploy(
    end_block: Optional[str] = None,
    parallelism: Optional[int] = None,
    wait: bool = False,
    poll_interval: float = 5.0,
    timeout: float = 3600.0
) -> models.JobInfo
```

**Parameters:**

- `end_block` (str, optional): Block to stop at. If None, runs continuously.
- `parallelism` (int, optional): Number of parallel workers.
- `wait` (bool, optional): If True, blocks until job completes. Default: False.
- `poll_interval` (float, optional): Seconds between polls if waiting. Default: 5.0.
- `timeout` (float, optional): Maximum wait time if waiting. Default: 3600.0.

**Returns:** `JobInfo` - if `wait=False`, returns initial job info; if `wait=True`, returns final job info.

**Raises:**

- `TimeoutError`: If waiting and job doesn't complete within timeout.

**Example:**

```python
# Deploy and return immediately
context = client.sql(...).register_as(...)
job = context.deploy(parallelism=4)
print(f"Started job {job.id}")

# Deploy and wait for completion
job = context.deploy(parallelism=4, wait=True, timeout=1800)
print(f"Completed with status: {job.status}")
```

---

## Complete Example

Putting it all together:

```python
from amp import Client
from amp.admin.errors import AdminAPIError, TimeoutError

# Initialize client
client = Client(
    query_url="grpc://localhost:8815",
    admin_url="http://localhost:8080",
    auth_token="my-token"
)

try:
    # Build and test query
    query = client.sql("""
        SELECT block_num, hash, timestamp
        FROM eth.blocks
        WHERE block_num > 1000000
    """)

    # Test locally
    df = query.to_pandas()
    print(f"Query returns {len(df)} rows")

    # Validate schema
    schema = client.schema.get_output_schema(
        query.query, 
        dependencies={'eth': '_/eth_firehose@1.0.0'}
    )
    print(f"Schema: {schema.schema}")

    # Register and deploy
    job = (
        query
        .with_dependency('eth', '_/eth_firehose@1.0.0')
        .register_as('_', 'eth_blocks_filtered', '1.0.0', 'blocks', 'mainnet')
        .deploy(
            end_block='latest',
            parallelism=4,
            wait=True,
            timeout=1800.0
        )
    )

    print(f"Deployment completed: {job.status}")

except AdminAPIError as e:
    print(f"API Error: {e.error_code} - {e.message}")

except TimeoutError:
    print("Deployment timed out")

finally:
    client.close()
```
