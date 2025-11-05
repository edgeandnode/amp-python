# Label Manager Guide

The Label Manager enables enriching streaming blockchain data with reference datasets (labels) stored in CSV files. This is useful for adding human-readable information like token symbols, decimals, or NFT collection names to raw blockchain data.

## Overview

The Label Manager:
- Loads CSV files containing reference data (e.g., token metadata)
- Automatically converts hex addresses to binary format for efficient joining
- Stores labels in memory as PyArrow tables for zero-copy joins
- Supports multiple label datasets in a single streaming session

## Basic Usage

### Python API

```python
from amp.client import Client
from amp.loaders.types import LabelJoinConfig

# Create client and add labels
client = Client()
client.label_manager.add_label(
    name='tokens',
    csv_path='data/eth_mainnet_token_metadata.csv',
    binary_columns=['token_address']  # Auto-detected if column name contains 'address'
)

# Use labels when loading data
config = LabelJoinConfig(
    label_name='tokens',
    label_key='token_address',
    stream_key='token_address'
)

result = loader.load_table(
    data=batch,
    table_name='erc20_transfers',
    label_config=config
)
```

### Command Line (snowflake_parallel_loader.py)

```bash
python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_transfers \
  --label-csv data/eth_mainnet_token_metadata.csv \
  --label-name tokens \
  --label-key token_address \
  --stream-key token_address \
  --blocks 100000
```

## Label CSV Format

### Example: Token Metadata

```csv
token_address,symbol,decimals,name
0x6b175474e89094c44da98b954eedeac495271d0f,DAI,18,Dai Stablecoin
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6,USD Coin
0xdac17f958d2ee523a2206206994597c13d831ec7,USDT,6,Tether USD
```

### Supported Column Types

- **Address columns**: Hex strings (with or without `0x` prefix) automatically converted to binary
- **Text columns**: Symbols, names, descriptions
- **Numeric columns**: Decimals, supply, prices
- **Any valid CSV data**

## Mounting Data Files in Containers

Since label CSV files can be large (10-100MB+) and shouldn't be checked into git, you need to mount them at runtime.

### Docker: Volume Mounts

Mount a local directory containing your CSV files:

```bash
# Create local data directory with your CSV files
mkdir -p ./data
# Download or copy your label files here
cp /path/to/eth_mainnet_token_metadata.csv ./data/

# Run container with volume mount
docker run \
  -v $(pwd)/data:/app/data:ro \
  -e SNOWFLAKE_ACCOUNT=xxx \
  -e SNOWFLAKE_USER=xxx \
  -e SNOWFLAKE_PRIVATE_KEY="$(cat private_key.pem)" \
  ghcr.io/your-org/amp-python:latest \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_transfers \
  --label-csv /app/data/eth_mainnet_token_metadata.csv \
  --label-name tokens \
  --label-key token_address \
  --stream-key token_address
```

**Key points:**
- Mount as read-only (`:ro`) for security
- Use absolute paths inside container (`/app/data/...`)
- The `/app/data` directory exists in the image but is empty by default

### Docker Compose

```yaml
version: '3.8'
services:
  amp-loader:
    image: ghcr.io/your-org/amp-python:latest
    volumes:
      - ./data:/app/data:ro
    environment:
      - SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}
      - SNOWFLAKE_USER=${SNOWFLAKE_USER}
      - SNOWFLAKE_PRIVATE_KEY=${SNOWFLAKE_PRIVATE_KEY}
    command: >
      --query-file apps/queries/erc20_transfers.sql
      --table-name erc20_transfers
      --label-csv /app/data/eth_mainnet_token_metadata.csv
      --label-name tokens
      --label-key token_address
      --stream-key token_address
```

## Kubernetes Deployments

For Kubernetes, you have several options depending on file size and update frequency.

### Option 1: Init Container with Cloud Storage (Recommended)

Best for large files (>1MB) that don't change frequently.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: amp-loader
spec:
  template:
    spec:
      # Init container downloads data files before main container starts
      initContainers:
      - name: fetch-labels
        image: google/cloud-sdk:slim
        command:
        - /bin/sh
        - -c
        - |
          gsutil cp gs://your-bucket/eth_mainnet_token_metadata.csv /data/
          echo "Downloaded label files successfully"
        volumeMounts:
        - name: data-volume
          mountPath: /data

      # Main application container
      containers:
      - name: loader
        image: ghcr.io/your-org/amp-python:latest
        args:
        - --query-file
        - apps/queries/erc20_transfers.sql
        - --table-name
        - erc20_transfers
        - --label-csv
        - /app/data/eth_mainnet_token_metadata.csv
        - --label-name
        - tokens
        - --label-key
        - token_address
        - --stream-key
        - token_address
        volumeMounts:
        - name: data-volume
          mountPath: /app/data
          readOnly: true
        env:
        - name: SNOWFLAKE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: amp-secrets
              key: snowflake-account
        # ... other env vars

      # Shared volume between init container and main container
      volumes:
      - name: data-volume
        emptyDir: {}
```

**For AWS S3:**
```yaml
initContainers:
- name: fetch-labels
  image: amazon/aws-cli
  command:
  - /bin/sh
  - -c
  - |
    aws s3 cp s3://your-bucket/eth_mainnet_token_metadata.csv /data/
  env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: secret-access-key
```

### Option 2: ConfigMap (Small Files Only)

Only suitable for files < 1MB (Kubernetes ConfigMap size limit).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: label-data
data:
  tokens.csv: |
    token_address,symbol,decimals,name
    0x6b175474e89094c44da98b954eedeac495271d0f,DAI,18,Dai Stablecoin
    0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6,USD Coin
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: amp-loader
spec:
  template:
    spec:
      containers:
      - name: loader
        image: ghcr.io/your-org/amp-python:latest
        args:
        - --label-csv
        - /app/data/tokens.csv
        volumeMounts:
        - name: label-data
          mountPath: /app/data
          readOnly: true
      volumes:
      - name: label-data
        configMap:
          name: label-data
          items:
          - key: tokens.csv
            path: tokens.csv
```

### Option 3: PersistentVolume (Shared Data)

Use when multiple pods need access to the same large label files.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: amp-label-data
spec:
  accessModes:
  - ReadOnlyMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: amp-loader
spec:
  template:
    spec:
      containers:
      - name: loader
        image: ghcr.io/your-org/amp-python:latest
        volumeMounts:
        - name: label-data
          mountPath: /app/data
          readOnly: true
      volumes:
      - name: label-data
        persistentVolumeClaim:
          claimName: amp-label-data
```

**Note:** You'll need to populate the PV with your CSV files manually or via a separate job.

## Performance Considerations

### Memory Usage

Labels are loaded entirely into memory as PyArrow tables:
- Small CSV (1k rows): ~100 KB memory
- Medium CSV (100k rows): ~10 MB memory
- Large CSV (1M+ rows): ~100 MB+ memory

Monitor memory usage with large label datasets and adjust container resource limits accordingly.

### Binary Conversion

The Label Manager automatically converts hex address columns to fixed-size binary:
- **Before**: `0x6b175474e89094c44da98b954eedeac495271d0f` (42 chars)
- **After**: 20 bytes of binary data
- **Savings**: ~50% memory reduction + faster joins

### Join Performance

Joining is done using PyArrow's native join operations:
- **Zero-copy**: No data serialization/deserialization
- **Columnar**: Efficient memory access patterns
- **Throughput**: Can join 10k+ rows/second

## Best Practices

### 1. Label File Organization

```
data/
├── eth_mainnet_token_metadata.csv    # Token symbols, decimals
├── nft_collections.csv               # NFT collection names
└── contract_labels.csv               # Known contract labels
```

### 2. Binary Column Detection

Columns with "address" in the name are auto-detected for binary conversion:
```python
# These columns are automatically converted to binary
token_address
from_address
to_address
contract_address
```

Manually specify columns if needed:
```python
client.label_manager.add_label(
    'labels',
    'data/custom.csv',
    binary_columns=['my_custom_hex_column']
)
```

### 3. Error Handling

```python
try:
    client.label_manager.add_label('tokens', 'data/tokens.csv')
except FileNotFoundError:
    print("Warning: Label file not found, proceeding without labels")
    # Continue without labels - they're optional
```

### 4. Label Reuse

Register labels once, use across multiple tables:
```python
# Register once
client.label_manager.add_label('tokens', 'data/tokens.csv')

# Use in multiple load operations
loader.load_table(data1, 'erc20_transfers', label_config)
loader.load_table(data2, 'erc20_swaps', label_config)
```

### 5. Development vs Production

**Development:**
```bash
# Local files
--label-csv ./local_data/tokens.csv
```

**Production:**
```yaml
# Download from cloud storage in init container
initContainers:
- name: fetch-labels
  command: ['gsutil', 'cp', 'gs://bucket/tokens.csv', '/data/']
```

## Troubleshooting

### "Label file not found"
- Check file path is absolute inside container: `/app/data/file.csv`
- Verify volume mount is configured correctly
- Check init container logs if using cloud storage download

### "Binary column not found"
- Verify CSV column names match exactly
- Check column name contains "address" for auto-detection
- Manually specify `binary_columns` parameter

### High memory usage
- Large CSVs consume memory proportional to their size
- Consider filtering CSV to only needed columns
- Increase container memory limits if needed

### Slow joins
- Ensure binary conversion is working (check logs for "converted to fixed_size_binary")
- Verify join keys are the same type (both binary or both string)
- Check for null values in join columns

## Examples

See the complete examples in:
- `apps/snowflake_parallel_loader.py` - Command-line tool with label support
- `apps/examples/erc20_example.md` - Full ERC-20 transfer enrichment example
- `apps/examples/run_erc20_example.sh` - Shell script example

## API Reference

### LabelManager.add_label()

```python
def add_label(
    name: str,
    csv_path: str,
    binary_columns: Optional[List[str]] = None
) -> None:
    """
    Load and register a CSV label dataset.

    Args:
        name: Unique identifier for this label dataset
        csv_path: Path to CSV file (absolute or relative)
        binary_columns: List of hex column names to convert to binary.
                       If None, auto-detects columns with 'address' in name.

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV parsing fails or name already exists
    """
```

### LabelJoinConfig

```python
@dataclass
class LabelJoinConfig:
    """Configuration for joining labels with streaming data."""

    label_name: str      # Name of registered label dataset
    label_key: str       # Column name in label CSV to join on
    stream_key: str      # Column name in streaming data to join on
```

## Related Documentation

- [Snowflake Loader Guide](../apps/SNOWFLAKE_LOADER_GUIDE.md)
- [Query Examples](../apps/queries/README.md)
- [Kubernetes Deployment](../k8s/deployment.yaml)
