# Registry API Guide

The Amp Registry is a public catalog for discovering and sharing dataset manifests. This guide covers how to search for datasets, fetch manifests, and publish your own datasets.

## Quick Start

### Read-Only Operations (No Authentication Required)

```python
from amp.registry import RegistryClient

# Create registry client
registry = RegistryClient()

# Search for datasets
results = registry.datasets.search('ethereum blocks', limit=10)
for dataset in results.datasets:
    print(f'[{dataset.score:.2f}] {dataset.namespace}/{dataset.name}')
    print(f'  {dataset.description}')
```

### Using with Unified Client

```python
from amp import Client

# Initialize with all three APIs
client = Client(
    query_url='grpc://localhost:1602',      # Flight SQL queries
    admin_url='http://localhost:8080',      # Admin operations
    registry_url='https://api.registry.amp.staging.thegraph.com',  # Registry (default)
    auth=True  # Use ~/.amp-cli-config for authentication
)

# Search registry
results = client.registry.datasets.search('ethereum')

# Deploy found dataset to local node
dataset = results.datasets[0]
manifest = client.registry.datasets.get_manifest(
    dataset.namespace,
    dataset.name,
    dataset.latest_version.version_tag
)

client.admin.datasets.register(
    namespace=dataset.namespace,
    name=dataset.name,
    revision=dataset.latest_version.version_tag,
    manifest=manifest
)
```

## Discovery: Finding Datasets

### List All Datasets

```python
# Get first page of datasets
response = registry.datasets.list(limit=50, page=1)

print(f'Total datasets: {response.total_count}')
print(f'Has more pages: {response.has_next_page}')

for dataset in response.datasets:
    print(f'{dataset.namespace}/{dataset.name} (v{dataset.latest_version.version_tag})')
```

### Full-Text Search

Search by keywords in dataset names, descriptions, tags, and chains:

```python
# Search with keyword
results = registry.datasets.search('ethereum blocks', limit=10)

# Results are ranked by relevance
for dataset in results.datasets:
    print(f'Score: {dataset.score:.2f}')
    print(f'Dataset: {dataset.namespace}/{dataset.name}')
    print(f'Description: {dataset.description}')
    print()
```

### AI Semantic Search

Use natural language queries for semantic matching (requires OpenAI configuration):

```python
try:
    # Natural language query
    results = registry.datasets.ai_search(
        'find NFT transfer and marketplace data',
        limit=5
    )

    for dataset in results:
        print(f'[{dataset.score:.2f}] {dataset.namespace}/{dataset.name}')
except Exception as e:
    print(f'AI search not available: {e}')
```

## Exploring Datasets

### Get Dataset Details

```python
# Get full dataset information
dataset = registry.datasets.get('edgeandnode', 'ethereum_mainnet')

print(f'Name: {dataset.name}')
print(f'Namespace: {dataset.namespace}')
print(f'Latest version: {dataset.latest_version.version_tag}')
print(f'Visibility: {dataset.visibility}')
print(f'Description: {dataset.description}')
print(f'Tags: {dataset.keywords}')
print(f'Chains: {dataset.indexing_chains}')
```

### List Versions

```python
# Get all versions of a dataset
versions = registry.datasets.list_versions('edgeandnode', 'ethereum_mainnet')

for version in versions:
    print(f'v{version.version_tag} - {version.status}')
    print(f'  Created: {version.created_at}')
    print(f'  Changelog: {version.changelog}')
```

### Get Specific Version

```python
# Get details of a specific version
version = registry.datasets.get_version(
    'edgeandnode',
    'ethereum_mainnet',
    '1.0.0'
)

print(f'Version: {version.version_tag}')
print(f'Status: {version.status}')
print(f'Dataset reference: {version.dataset_reference}')
```

### Fetch Manifest

```python
# Get manifest for latest version
dataset = registry.datasets.get('edgeandnode', 'ethereum_mainnet')
manifest = registry.datasets.get_manifest(
    dataset.namespace,
    dataset.name,
    dataset.latest_version.version_tag
)

print(f'Kind: {manifest.get("kind")}')
print(f'Tables: {list(manifest.get("tables", {}).keys())}')
print(f'Dependencies: {list(manifest.get("dependencies", {}).keys())}')
```

## Publishing Datasets

Publishing requires authentication. Set up your auth token:

```python
# Option 1: Use existing auth from ~/.amp-cli-config
from amp import Client
client = Client(auth=True)

# Option 2: Provide token directly
from amp.registry import RegistryClient
registry = RegistryClient(auth_token='your-token-here')
```

### Publish New Dataset

```python
# Prepare your manifest
manifest = {
    'kind': 'manifest',
    'network': 'mainnet',
    'dependencies': {},
    'tables': {
        'my_table': {
            'input': {
                'sql': 'SELECT * FROM source_table'
            },
            'schema': {
                'arrow': {
                    'fields': [
                        {'name': 'id', 'type': 'UInt64', 'nullable': False},
                        {'name': 'value', 'type': 'String', 'nullable': True}
                    ]
                }
            },
            'network': 'mainnet'
        }
    },
    'functions': {}
}

# Publish dataset
dataset = client.registry.datasets.publish(
    namespace='myuser',
    name='my_dataset',
    version='1.0.0',
    manifest=manifest,
    visibility='public',
    description='My custom dataset',
    tags=['ethereum', 'custom'],
    chains=['ethereum-mainnet']
)

print(f'Published: {dataset.namespace}/{dataset.name}@{dataset.latest_version.version_tag}')
```

### Publish New Version

```python
# Update manifest for new version
updated_manifest = {
    # ... your updated manifest
}

# Publish new version
version = client.registry.datasets.publish_version(
    namespace='myuser',
    name='my_dataset',
    version='1.1.0',
    manifest=updated_manifest,
    description='Added new features'
)

print(f'Published version: {version.version_tag}')
```

### Update Metadata

```python
# Update dataset description, tags, etc.
dataset = client.registry.datasets.update(
    namespace='myuser',
    name='my_dataset',
    description='Updated description',
    tags=['ethereum', 'defi', 'updated'],
    chains=['ethereum-mainnet', 'arbitrum-one']
)
```

### Change Visibility

```python
# Make dataset private
dataset = client.registry.datasets.update_visibility(
    'myuser',
    'my_dataset',
    'private'
)

# Make it public again
dataset = client.registry.datasets.update_visibility(
    'myuser',
    'my_dataset',
    'public'
)
```

### Manage Version Status

```python
# Deprecate old version
version = client.registry.datasets.update_version_status(
    'myuser',
    'my_dataset',
    '1.0.0',
    'deprecated'
)

# Available statuses: 'draft', 'published', 'deprecated', 'archived'
```

### Delete Version

```python
# Archive/delete a version
response = client.registry.datasets.delete_version(
    'myuser',
    'my_dataset',
    '0.1.0'
)

print(f'Deleted: {response.reference}')
```

## Complete Workflow: Search → Deploy

This example shows the full workflow of finding a dataset, deploying it, and creating a derived dataset:

```python
from amp import Client

# Initialize client with all APIs
client = Client(
    query_url='grpc://localhost:1602',
    admin_url='http://localhost:8080',
    auth=True
)

# 1. Search for a dataset
print('Searching for ethereum datasets...')
results = client.registry.datasets.search('ethereum blocks', limit=5)
dataset = results.datasets[0]

print(f'Found: {dataset.namespace}/{dataset.name}')

# 2. Get full dataset details
full_dataset = client.registry.datasets.get(dataset.namespace, dataset.name)
print(f'Latest version: {full_dataset.latest_version.version_tag}')

# 3. Fetch manifest
manifest = client.registry.datasets.get_manifest(
    dataset.namespace,
    dataset.name,
    full_dataset.latest_version.version_tag
)

# 4. Deploy dependency to local node
print(f'Deploying {dataset.namespace}/{dataset.name}...')
client.admin.datasets.register(
    namespace=dataset.namespace,
    name=dataset.name,
    revision=full_dataset.latest_version.version_tag,
    manifest=manifest
)

deploy_response = client.admin.datasets.deploy(
    dataset.namespace,
    dataset.name,
    full_dataset.latest_version.version_tag
)

# Wait for deployment
client.admin.jobs.wait_for_completion(deploy_response.job_id)
print('Dependency deployed!')

# 5. Create derived dataset
derived_manifest = {
    'kind': 'manifest',
    'network': 'mainnet',
    'dependencies': {
        'base': f'{dataset.namespace}/{dataset.name}@{full_dataset.latest_version.version_tag}'
    },
    'tables': {
        'sample_data': {
            'input': {
                'sql': 'SELECT * FROM base.blocks LIMIT 1000'
            },
            'schema': {
                'arrow': {
                    'fields': [
                        {'name': 'block_num', 'type': 'UInt64', 'nullable': False}
                    ]
                }
            },
            'network': 'mainnet'
        }
    },
    'functions': {}
}

# 6. Deploy derived dataset
client.admin.datasets.register(
    namespace='_',
    name='my_sample',
    revision='1.0.0',
    manifest=derived_manifest
)

deploy_response = client.admin.datasets.deploy('_', 'my_sample', '1.0.0')
client.admin.jobs.wait_for_completion(deploy_response.job_id)
print('Derived dataset deployed!')

# 7. Query the data
result = client.sql('SELECT COUNT(*) FROM _/my_sample@1.0.0.sample_data')
print(f'Rows: {result.to_pandas()}')
```

## Error Handling

```python
from amp.registry.errors import (
    RegistryError,
    DatasetNotFoundError,
    UnauthorizedError,
    ValidationError
)

try:
    dataset = registry.datasets.get('unknown', 'dataset')
except DatasetNotFoundError as e:
    print(f'Dataset not found: {e}')
    print(f'Error code: {e.error_code}')
    print(f'Request ID: {e.request_id}')
except UnauthorizedError:
    print('Authentication required or invalid token')
except ValidationError as e:
    print(f'Invalid input: {e}')
except RegistryError as e:
    print(f'Registry error: {e}')
```

## Best Practices

### Dataset Naming

- Use lowercase letters, numbers, and underscores
- Must start with a letter or underscore (not a number)
- Examples: `ethereum_mainnet`, `uniswap_v3_trades`, `nft_transfers`

### Version Tags

- Use semantic versioning: `1.0.0`, `1.1.0`, `2.0.0`
- Or commit hashes: `8e0acc0`
- Or use `latest` for development

### Dependencies

- Always deploy dependencies before derived datasets
- Use version-pinned references: `namespace/dataset@1.0.0`
- Reference dependencies in SQL queries using aliases:

```python
manifest = {
    'dependencies': {
        'eth': 'edgeandnode/ethereum_mainnet@1.0.0',
        'prices': 'edgeandnode/token_prices@2.1.0'
    },
    'tables': {
        'enriched_blocks': {
            'input': {
                'sql': '''
                    SELECT
                        b.block_num,
                        b.timestamp,
                        p.eth_price
                    FROM eth.blocks b
                    LEFT JOIN prices.hourly p ON DATE_TRUNC('hour', b.timestamp) = p.hour
                '''
            }
        }
    }
}
```

### Visibility

- Use `public` for datasets you want to share
- Use `private` for internal/proprietary datasets
- Default is `public` when publishing

## Environment Configuration

### Registry URLs

```python
# Staging (default)
registry = RegistryClient(
    base_url='https://api.registry.amp.staging.thegraph.com'
)

# Production (when available)
registry = RegistryClient(
    base_url='https://api.registry.amp.thegraph.com'
)
```

### Authentication

The Registry client uses the same authentication as the Admin API:

1. Interactive login: `~/.amp-cli-config`
2. Direct token: Pass `auth_token='your-token'`
3. Unified client: Set `auth=True` to use saved credentials
