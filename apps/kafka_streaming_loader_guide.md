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
| `--start-block N` | Resume from state | Block number or `latest` to start from |
| `--reorg-topic NAME` | Same as `--topic` | Separate topic for reorg messages |
| `--label-csv PATH` | - | CSV file for data enrichment |
| `--state-dir PATH` | `.amp_state` | Directory for LMDB state storage |
| `--auth` | - | Enable auth using `~/.amp/cache` or `AMP_AUTH_TOKEN` env var |
| `--auth-token TOKEN` | - | Explicit auth token |

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
  "network": "ethereum-mainnet",
  "start_block": 19000100,
  "end_block": 19000110,
  "last_valid_hash": "0xabc123..."
}
```

Consumers should invalidate data in the specified block range. Use `--reorg-topic` to send these to a separate topic (useful for Snowflake Kafka connector which requires strict schema per topic).

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

### Stream from Latest Block

```bash
uv run python apps/kafka_streaming_loader.py \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers localhost:9092 \
  --topic eth_live_logs \
  --query-file apps/queries/all_logs.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet \
  --start-block latest \
  --auth
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

### Build the loader image

```bash
docker build -f Dockerfile.kafka -t amp-kafka .
```

### Quick demo: local Kafka with Docker

This section runs a single-broker Kafka in Docker for quick testing. For production, point `--kafka-brokers` at your real Kafka cluster and skip this section.

Start a single-broker Kafka using `confluentinc/cp-kafka`:

```bash
docker network create kafka-net

docker run -d --name kafka --network kafka-net -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  confluentinc/cp-kafka:latest
```

This configures two listeners:
- `kafka:29092` — for containers on the `kafka-net` network (use this from the loader)
- `localhost:9092` — for host access (use this from `uv run` or the consumer script)

### Run the loader

```bash
docker run -d \
  --name amp-kafka-loader \
  --network kafka-net \
  -e AMP_AUTH_TOKEN \
  -v $(pwd)/apps/queries:/data/queries \
  -v $(pwd)/.amp_state:/data/state \
  amp-kafka \
  --amp-server 'grpc+tls://gateway.amp.staging.thegraph.com:443' \
  --kafka-brokers kafka:29092 \
  --topic erc20_transfers \
  --query-file /data/queries/erc20_transfers_activity.sql \
  --raw-dataset 'edgeandnode/ethereum_mainnet' \
  --network ethereum-mainnet \
  --state-dir /data/state \
  --start-block latest \
  --auth

# Check logs
docker logs -f amp-kafka-loader
```

### Consume messages from host

```bash
uv run python apps/kafka_consumer.py erc20_transfers localhost:9092
```

## Getting Help

```bash
# View all options
uv run python apps/kafka_streaming_loader.py --help

# View this guide
cat apps/kafka_streaming_loader_guide.md
```
