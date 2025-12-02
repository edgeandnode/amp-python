# Kafka Streaming Loader - Usage Guide

Stream blockchain data to Kafka topics in real-time.

## Quick Start

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic anvil_logs \
  --query-file apps/queries/anvil_logs.sql \
  --raw-dataset anvil \
  --start-block 0
```

## Basic Usage

### Minimal Example

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic my_topic \
  --query-file my_query.sql \
  --raw-dataset eth_firehose
```

### With Common Options

```bash
uv run python apps/kafka_streaming_loader.py \
  --kafka-brokers localhost:9092 \
  --topic erc20_transfers \
  --query-file apps/queries/erc20_transfers.sql \
  --raw-dataset eth_firehose \
  --start-block 19000000
```

## Configuration Options

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--topic NAME` | Kafka topic name |
| `--query-file PATH` | Path to SQL query file |
| `--raw-dataset NAME` | Dataset name (e.g., `eth_firehose`, `anvil`) |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--kafka-brokers` | `localhost:9092` | Kafka broker addresses |
| `--start-block N` | Latest block | Start streaming from this block |
| `--network NAME` | `anvil` | Network identifier |
| `--label-csv PATH` | - | CSV file for data enrichment |
| `--amp-server URL` | `grpc://127.0.0.1:1602` | AMP server URL |

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

### Stream Anvil Logs

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic anvil_logs \
  --query-file apps/queries/anvil_logs.sql \
  --raw-dataset anvil \
  --start-block 0
```

### Stream ERC20 Transfers

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic erc20_transfers \
  --query-file apps/queries/erc20_transfers.sql \
  --raw-dataset eth_firehose \
  --start-block 19000000 \
  --label-csv data/eth_mainnet_token_metadata.csv
```

### Stream from Latest Blocks

```bash
uv run python apps/kafka_streaming_loader.py \
  --topic eth_live_logs \
  --query-file apps/queries/all_logs.sql \
  --raw-dataset eth_firehose
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
