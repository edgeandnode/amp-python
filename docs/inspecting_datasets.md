# Inspecting Dataset Schemas

The Registry client and Admin API on standard client provide convenient methods to explore dataset structures without manually parsing manifests.

## Quick Start

```python
from amp.registry import RegistryClient

client = RegistryClient() # Note: Inspection functionality is also available on the Admin api of the regular client (Client())

# Pretty-print dataset structure
client.datasets.inspect('edgeandnode', 'ethereum-mainnet')

# Get structured schema data
schema = client.datasets.describe('edgeandnode', 'ethereum-mainnet')
```

## Methods

### `inspect(namespace, name, version='latest')`

Pretty-prints the dataset structure in a human-readable format. Perfect for interactive exploration.

**Example Output:**
```
Dataset: edgeandnode/ethereum-mainnet@latest
Description: Ethereum mainnet blockchain data

📊 blocks (21 columns)
  • block_num          UInt64                    NOT NULL
  • timestamp          Timestamp(Nanosecond)     NOT NULL
  • hash               FixedSizeBinary(32)       NOT NULL
  • parent_hash        FixedSizeBinary(32)       NOT NULL
  • miner              FixedSizeBinary(20)       NOT NULL
  ...

📊 transactions (24 columns)
  • block_num          UInt64                    NOT NULL
  • tx_hash            FixedSizeBinary(32)       NOT NULL
  • from               FixedSizeBinary(20)       NOT NULL
  • to                 FixedSizeBinary(20)       NULL
  ...
```

### `describe(namespace, name, version='latest')`

Returns a structured dictionary mapping table names to column information. Use this for programmatic access.

**Returns:**
```python
{
    'blocks': [
        {'name': 'block_num', 'type': 'UInt64', 'nullable': False},
        {'name': 'timestamp', 'type': 'Timestamp(Nanosecond)', 'nullable': False},
        {'name': 'hash', 'type': 'FixedSizeBinary(32)', 'nullable': False},
        ...
    ],
    'transactions': [
        {'name': 'tx_hash', 'type': 'FixedSizeBinary(32)', 'nullable': False},
        ...
    ]
}
```

## Use Cases

### 1. Interactive Exploration

```python
# Quickly see what's available
client.datasets.inspect('namespace', 'dataset-name')
```

### 2. Finding Specific Columns

```python
schema = client.datasets.describe('namespace', 'dataset-name')

# Find tables with specific columns
for table_name, columns in schema.items():
    col_names = [col['name'] for col in columns]
    if 'address' in col_names:
        print(f"Table '{table_name}' has an address column")
```

### 3. Finding Ethereum Addresses

```python
schema = client.datasets.describe('namespace', 'dataset-name')

# Find all address columns (20-byte binary fields)
for table_name, columns in schema.items():
    address_cols = [col['name'] for col in columns if col['type'] == 'FixedSizeBinary(20)']
    if address_cols:
        print(f"{table_name}: {', '.join(address_cols)}")

# Example output:
# blocks: miner
# transactions: from, to
# logs: address
```

### 4. Finding Transaction/Block Hashes

```python
schema = client.datasets.describe('namespace', 'dataset-name')

# Find all hash columns (32-byte binary fields)
for table_name, columns in schema.items():
    hash_cols = [col['name'] for col in columns if col['type'] == 'FixedSizeBinary(32)']
    if hash_cols:
        print(f"{table_name}: {', '.join(hash_cols)}")

# Example output:
# blocks: hash, parent_hash, state_root, transactions_root
# transactions: block_hash, tx_hash
# logs: block_hash, tx_hash, topic0, topic1, topic2, topic3
```

### 5. Checking Nullable Columns

```python
schema = client.datasets.describe('namespace', 'dataset-name')

# Find columns that allow NULL values (important for data quality)
for table_name, columns in schema.items():
    nullable_cols = [col['name'] for col in columns if col['nullable']]
    print(f"{table_name}: {len(nullable_cols)}/{len(columns)} nullable columns")
    print(f"  Nullable: {', '.join(nullable_cols[:5])}")

# Example output:
# transactions: 5/24 nullable columns
#   Nullable: to, gas_price, value, max_fee_per_gas, max_priority_fee_per_gas
```

### 6. Building Dynamic Queries

```python
from amp import Client

registry_client = RegistryClient()
client = Client(
    query_url='grpc://localhost:1602',
    admin_url='http://localhost:8080',
    auth=True
)

# Discover available tables
schema = registry_client.datasets.describe('namespace', 'dataset-name')
print(f"Available tables: {list(schema.keys())}")

# Build query based on available columns
if 'blocks' in schema:
    block_cols = [col['name'] for col in schema['blocks']]
    if 'block_num' in block_cols and 'timestamp' in block_cols:
        # Safe to query these columns
        result = client.sql("SELECT block_num, timestamp FROM blocks LIMIT 10")
```

## Supported Arrow Types

The `describe()` and `inspect()` methods handle these Arrow types:

- **Primitives**: `UInt64`, `Int32`, `Boolean`, `Binary`
- **Timestamps**: `Timestamp(Nanosecond)`, `Timestamp(Microsecond)`, etc.
- **Fixed-size Binary**: `FixedSizeBinary(20)` (addresses), `FixedSizeBinary(32)` (hashes)
- **Decimals**: `Decimal128(38,0)` (large integers), `Decimal128(18,6)` (fixed-point)

## Complete Example

```python
from amp.registry import RegistryClient
from amp import Client

# Step 1: Discover datasets
registry = RegistryClient()
results = registry.datasets.search('ethereum blocks')

print("Available datasets:")
for ds in results.datasets[:5]:
    print(f"  • {ds.namespace}/{ds.name}")

# Step 2: Inspect a dataset
print("\nInspecting dataset structure:")
registry.datasets.inspect('graphops', 'ethereum-mainnet')

# Step 3: Get schema programmatically
schema = registry.datasets.describe('graphops', 'ethereum-mainnet')

# Step 4: Query based on discovered schema
client = Client(query_url='grpc://your-server:1602', auth=True)

# Find tables with block_num column
tables_with_blocks = [
    table for table, cols in schema.items()
    if any(col['name'] == 'block_num' for col in cols)
]

for table in tables_with_blocks:
    print(f"\nQuerying {table}...")
    results = client.sql(f"SELECT * FROM {table} LIMIT 5").to_arrow()
    print(f"  Rows: {len(results)}")
```

## Tips

1. **Use `inspect()` interactively**: Great for Jupyter notebooks or REPL exploration
2. **Use `describe()` in scripts**: When you need programmatic access to schema info
3. **Check nullability**: The `nullable` field tells you if a column can have NULL values
4. **Version pinning**: Always specify a version in production (`version='1.2.3'`) instead of using `'latest'`
