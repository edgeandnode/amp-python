# Kafka Streaming Loader - Usage Guide

Stream blockchain data to Kafka topics in real-time.

## Quick Start

```bash
uv run python apps/kafka_streaming_loader.py \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers localhost:9092 \
  --topic erc20_transfers \
  --query-file apps/queries/erc20_transfers_activity.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet
```

## Basic Usage

### Minimal Example (Staging Gateway)

```bash
uv run python apps/kafka_streaming_loader.py \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers localhost:9092 \
  --topic my_topic \
  --query-file my_query.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet
```

### Local Development (Anvil)

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic anvil_logs \
  --query-file apps/queries/anvil_logs.sql \
  --raw-dataset anvil \
  --start-block 0
```

## Configuration Options

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--topic NAME` | Kafka topic name |
| `--query-file PATH` | Path to SQL query file |
| `--raw-dataset NAME` | Dataset name (e.g., `edgeandnode/ethereum_mainnet`, `anvil`) |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--amp-server URL` | `grpc://127.0.0.1:1602` | AMP server URL (use `grpc+tls://gateway.amp.staging.thegraph.com:443` for staging) |
| `--kafka-brokers` | `localhost:9092` | Kafka broker addresses |
| `--network NAME` | `anvil` | Network identifier (e.g., `ethereum-mainnet`, `anvil`) |
| `--start-block N` | Latest block | Start streaming from this block |
| `--label-csv PATH` | - | CSV file for data enrichment |

## Message Format

### Data Messages

Each row is sent as JSON with `_type: 'data'`:

```json
{
  "_type": "data",
  "block_num": 19000000,
  "tx_hash": "0x123...",
  "address": "0xabc...",
  "data": "0x..."
}
```

### Reorg Messages

On blockchain reorganizations, reorg events are sent:

```json
{
  "_type": "reorg",
  "network": "ethereum",
  "start_block": 19000100,
  "end_block": 19000110
}
```

Consumers should invalidate data in the specified block range.

## Examples

### Stream ERC20 Transfers

Stream ERC20 transfer events with the activity schema:

```bash
uv run python apps/kafka_streaming_loader.py \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers localhost:9092 \
  --topic erc20_transfers \
  --query-file apps/queries/erc20_transfers_activity.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet
```

#### Token Metadata Enrichment (Optional)

To enrich transfer events with token metadata (symbol, name, decimals), add a CSV file:

1. **Obtain the token metadata CSV**:
   - Download from your token metadata source
   - Or export from a database with token information
   - Required columns: `token_address`, `symbol`, `name`, `decimals`

2. **Place the CSV file** in the `data/` directory:
   ```bash
   mkdir -p data
   # Copy your CSV file
   cp /path/to/your/tokens.csv data/eth_mainnet_token_metadata.csv
   ```

3. **Run with label enrichment**:
   ```bash
   uv run python apps/kafka_streaming_loader.py \
     --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
     --kafka-brokers localhost:9092 \
     --topic erc20_transfers \
     --query-file apps/queries/erc20_transfers_activity.sql \
     --raw-dataset 'edgeandnode/ethereum_mainnet' \
     --network ethereum-mainnet \
     --label-csv data/eth_mainnet_token_metadata.csv
   ```

**CSV Format Example**:
```csv
token_address,symbol,name,decimals
0xe0f066cb646256d33cae9a32c7b144ccbd248fdd,gg unluck,gg unluck,18
0xabb2a7bec4604491e85a959177cc0e95f60c6bd5,RTX,Remittix,3
```

Without the CSV file, `token_symbol`, `token_name`, and `token_decimals` will be `null` in the output.

### Stream from Latest Blocks

```bash
uv run python apps/kafka_streaming_loader.py \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers localhost:9092 \
  --topic eth_live_logs \
  --query-file apps/queries/all_logs.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet
```

### Local Development (Anvil)

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic anvil_logs \
  --query-file apps/queries/anvil_logs.sql \
  --raw-dataset anvil \
  --start-block 0
```

## Consuming Messages

Use the consumer script to view messages:

```bash
# Basic usage
uv run python apps/kafka_consumer.py anvil_logs

# Custom broker
uv run python apps/kafka_consumer.py anvil_logs localhost:9092

# Custom consumer group
uv run python apps/kafka_consumer.py anvil_logs localhost:9092 my-group
```

## Docker Usage

```bash
# Build image
docker build -f Dockerfile.kafka -t amp-kafka-loader .

# Run loader
docker run --rm \
  --network host \
  amp-kafka-loader \
  --kafka-brokers localhost:9092 \
  --topic my_topic \
  --query-file apps/queries/my_query.sql \
  --raw-dataset anvil \
  --start-block 0
```

## Getting Help

```bash
# View all options
uv run python apps/kafka_streaming_loader.py --help

# View this guide
cat apps/kafka_streaming_loader_guide.md
```
