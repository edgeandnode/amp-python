# Snowflake Parallel Loader - Usage Guide

Complete guide for using `snowflake_parallel_loader.py` to load blockchain data into Snowflake.

## Table of Contents

- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Basic Usage](#basic-usage)
- [Common Use Cases](#common-use-cases)
- [Configuration Options](#configuration-options)
- [Complete Examples](#complete-examples)
- [Troubleshooting](#troubleshooting)

## Quick Start

```bash
# 1. Set Snowflake credentials
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_PRIVATE_KEY="$(cat path/to/rsa_key.p8)"

# 2. Load data with custom query
uv run python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name my_table \
  --blocks 10000
```

## Prerequisites

### Required Environment Variables

Set these in your shell or `.env` file:

```bash
# Snowflake connection (all required)
export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=YOUR_DB

# Authentication - use ONE of these methods:
export SNOWFLAKE_PRIVATE_KEY="$(cat ~/.ssh/snowflake_rsa_key.p8)"
# OR
export SNOWFLAKE_PASSWORD=your_password

# AMP server (optional, has default)
export AMP_SERVER_URL=grpc://your-server:80
```

### Required Files

1. **SQL Query File** - Your custom query (see `apps/queries/` for examples)
2. **Label CSV** (optional) - For data enrichment

## Basic Usage

### Minimal Example

Load data with just a query and table name:

```bash
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_events \
  --blocks 50000
```

This will:
- Load the most recent 50,000 blocks
- Use Snowpipe Streaming (default)
- Enable state management (job resumption)
- Enable reorg history preservation
- Use 4 parallel workers (default)

### With All Common Options

```bash
uv run python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_transfers \
  --blocks 100000 \
  --workers 8 \
  --label-csv data/token_metadata.csv \
  --label-name tokens \
  --label-key token_address \
  --stream-key token_address
```

## Common Use Cases

### 1. Load ERC20 Transfers with Token Metadata

See the [ERC20 Example](#erc20-transfers-with-labels) below for complete walkthrough.

### 2. Load Raw Logs (No Labels)

```bash
# Create a simple logs query
cat > /tmp/raw_logs.sql << 'EOF'
select
    block_num,
    block_hash,
    timestamp,
    tx_hash,
    log_index,
    address,
    topic0,
    data
from eth_firehose.logs
EOF

# Load it
uv run python apps/snowflake_parallel_loader.py \
  --query-file /tmp/raw_logs.sql \
  --table-name raw_logs \
  --min-block 19000000 \
  --max-block 19100000
```

### 3. Custom Event Decoding

```bash
# Create Uniswap V2 Swap query
cat > /tmp/uniswap_swaps.sql << 'EOF'
select
    l.block_num,
    l.timestamp,
    l.address as pool_address,
    evm_decode(
        l.topic1, l.topic2, l.topic3, l.data,
        'Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)'
    )['sender'] as sender,
    evm_decode(
        l.topic1, l.topic2, l.topic3, l.data,
        'Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)'
    )['amount0In'] as amount0_in
from eth_firehose.logs l
where l.topic0 = evm_topic('Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)')
EOF

# Load it
uv run python apps/snowflake_parallel_loader.py \
  --query-file /tmp/uniswap_swaps.sql \
  --table-name uniswap_v2_swaps \
  --blocks 50000 \
  --workers 12
```

### 4. Resume an Interrupted Job

If a job gets interrupted, just run the same command again. State management automatically resumes from where it left off:

```bash
# Initial run (gets interrupted)
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --blocks 1000000

# Press Ctrl+C to interrupt...

# Resume - runs the exact same command
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --blocks 1000000
# Will skip already-processed batches and continue!
```

### 5. Use Stage Loading (Instead of Snowpipe Streaming)

```bash
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --blocks 50000 \
  --loading-method stage
```

## Configuration Options

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--query-file PATH` | Path to SQL query file |
| `--table-name NAME` | Destination Snowflake table |

### Block Range (pick one strategy)

**Strategy 1: Auto-detect recent blocks**
```bash
--blocks 100000  # Load most recent 100k blocks
```

**Strategy 2: Explicit range**
```bash
--min-block 19000000 --max-block 19100000
```

**Additional options:**
- `--source-table TABLE` - Table for max block detection (default: `eth_firehose.logs`)
- `--block-column COLUMN` - Partitioning column (default: `block_num`)

### Label Configuration (all optional)

To enrich data with CSV labels:

```bash
--label-csv data/labels.csv       # Path to CSV file
--label-name my_labels             # Label identifier
--label-key address                # Column in CSV to join on
--stream-key contract_address      # Column in query to join on
```

**Requirements:**
- All four arguments required together
- CSV must have header row
- Join columns must exist in both CSV and query

### Snowflake Configuration

| Argument | Default | Description |
|----------|---------|-------------|
| `--loading-method` | `snowpipe_streaming` | Method: `snowpipe_streaming`, `stage`, or `insert` |
| `--preserve-reorg-history` | `True` | Enable temporal reorg tracking |
| `--no-preserve-reorg-history` | - | Disable reorg history |
| `--disable-state` | - | Disable state management (no resumption) |
| `--connection-name` | `snowflake_{table}` | Connection identifier |
| `--pool-size N` | `workers + 2` | Connection pool size |

### Parallel Execution

| Argument | Default | Description |
|----------|---------|-------------|
| `--workers N` | `4` | Number of parallel workers |
| `--flush-interval SECONDS` | `1.0` | Snowpipe buffer flush interval |

### Server

| Argument | Default | Description |
|----------|---------|-------------|
| `--server URL` | From env or default | AMP server URL |
| `--verbose` | False | Enable verbose logging from Snowflake libraries |

## Complete Examples

### ERC20 Transfers with Labels

Full example replicating `test_erc20_labeled_parallel.py`:

```bash
# 1. Ensure you have the token metadata CSV
ls data/eth_mainnet_token_metadata.csv

# 2. Run the loader
uv run python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_labeled \
  --label-csv data/eth_mainnet_token_metadata.csv \
  --label-name token_metadata \
  --label-key token_address \
  --stream-key token_address \
  --blocks 100000 \
  --workers 4 \
  --flush-interval 1.0

# 3. Query the results in Snowflake
# SELECT token_address, symbol, name, from_address, to_address, value
# FROM erc20_labeled_current LIMIT 10;
```

### Large Historical Load with Many Workers

```bash
uv run python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_historical \
  --min-block 17000000 \
  --max-block 19000000 \
  --workers 16 \
  --loading-method stage \
  --label-csv data/eth_mainnet_token_metadata.csv \
  --label-name tokens \
  --label-key token_address \
  --stream-key token_address
```

### Development/Testing (Small Load)

```bash
# Quick test with just 1000 blocks
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name test_table \
  --blocks 1000 \
  --workers 2
```

## Query Requirements

Your SQL query file must:

1. **Include a block partitioning column** (default: `block_num`)
   ```sql
   select
       block_num,  -- Required for partitioning
       ...
   ```

2. **Be valid SQL** for the AMP server
   ```sql
   select ... from eth_firehose.logs where ...
   ```

3. **Include join columns** if using labels
   ```sql
   select
       address as token_address,  -- Used for --stream-key
       ...
   ```

See `apps/queries/README.md` for detailed query guidelines.

## Understanding the Output

### Execution Summary

```
🎉 Load Complete!
======================================================================
📊 Table name:       erc20_labeled
📦 Block range:      19,900,000 to 20,000,000
📈 Rows loaded:      1,234,567
🏷️  Label columns:    symbol, name, decimals
⏱️  Duration:         45.23s
🚀 Throughput:       27,302 rows/sec
👷 Workers:          4 configured
✅ Successful:       25/25 batches
📊 Avg rows/block:   12
======================================================================
```

### Created Database Objects

The loader creates:

1. **Main table**: `{table_name}`
   - Contains all data with metadata columns
   - Includes `_amp_batch_id` for tracking
   - Includes `_amp_is_current` and `_amp_reorg_batch_id` if reorg history enabled

2. **Current view**: `{table_name}_current`
   - Filters to `_amp_is_current = TRUE`
   - Use this for queries

3. **History view**: `{table_name}_history`
   - Shows all rows including reorged data
   - Use for temporal analysis

### Metadata Columns

| Column | Type | Purpose |
|--------|------|---------|
| `_amp_batch_id` | VARCHAR(16) | Unique batch identifier (hex) |
| `_amp_is_current` | BOOLEAN | True = current, False = superseded by reorg |
| `_amp_reorg_batch_id` | VARCHAR(16) | Batch ID that superseded this row (NULL if current) |

## Troubleshooting

### "No data found in eth_firehose.logs"

**Problem:** Block range detection query returned no results

**Solutions:**
1. Check your AMP server connection
2. Verify the source table name: `--source-table your_table`
3. Use explicit block range instead: `--min-block N --max-block N`

### "Query file not found"

**Problem:** Path to SQL file is incorrect

**Solutions:**
1. Use absolute path: `--query-file /full/path/to/query.sql`
2. Use relative path from repo root: `--query-file apps/queries/my_query.sql`
3. Check file exists: `ls -la apps/queries/`

### "Label CSV not found"

**Problem:** CSV file path is incorrect

**Solutions:**
1. Check file exists: `ls -la data/eth_mainnet_token_metadata.csv`
2. Use absolute path if needed
3. Verify CSV has header row

### "Password is empty" or Snowflake connection errors

**Problem:** Snowflake credentials not set

**Solutions:**
1. Check environment variables: `echo $SNOWFLAKE_USER`
2. Source your `.env` file: `source .test.env`
3. Use `uv run --env-file .test.env` to load env file
4. Verify private key format (PKCS#8 PEM)

### Job runs but no data loaded

**Problem:** State management found all batches already processed

**Solutions:**
1. Check if table already has data: `SELECT COUNT(*) FROM {table}_current;`
2. This is expected behavior for job resumption
3. To force reload, delete the table first or use a different table name
4. To disable state: `--disable-state` (not recommended)

### Worker/Performance Issues

**Problem:** Load is slow or workers aren't being utilized

**Solutions:**
1. Increase workers: `--workers 16`
2. Adjust partition size by changing block range
3. Use stage loading for large batches: `--loading-method stage`
4. Check Snowflake warehouse size
5. Monitor with: `--flush-interval 0.5` for faster Snowpipe commits

### Label Join Not Working

**Problem:** No data loaded when using labels

**Solutions:**
1. Verify CSV has data: `wc -l data/labels.csv`
2. Check CSV header matches `--label-key`
3. Verify query includes `--stream-key` column
4. Inner join means only matching rows are kept
5. Test without labels first to verify query works

### Need More Detailed Logs

**Problem:** Want to see verbose output from Snowflake libraries for debugging

**Solution:**
```bash
# Add --verbose flag to enable detailed logging
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --blocks 1000 \
  --verbose
```

By default, verbose logs from Snowflake connector and Snowpipe Streaming are suppressed for cleaner output. Use `--verbose` to see all library logs when troubleshooting connection or streaming issues.

## Advanced Usage

### Multiple Sequential Loads

```bash
# Load different block ranges to same table
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --min-block 19000000 \
  --max-block 19100000

uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --min-block 19100000 \
  --max-block 19200000
# State management prevents duplicates!
```

### Disable Features for Testing

```bash
# Minimal features for quick testing
uv run python apps/snowflake_parallel_loader.py \
  --query-file test_query.sql \
  --table-name test_table \
  --blocks 100 \
  --workers 2 \
  --disable-state \
  --no-preserve-reorg-history \
  --loading-method insert
```

### Custom Connection Pool

```bash
# Large pool for many workers
uv run python apps/snowflake_parallel_loader.py \
  --query-file my_query.sql \
  --table-name my_table \
  --blocks 50000 \
  --workers 20 \
  --pool-size 25
```

## Getting Help

```bash
# View all options
uv run python apps/snowflake_parallel_loader.py --help

# View query examples
cat apps/queries/README.md

# View this guide
cat apps/SNOWFLAKE_LOADER_GUIDE.md
```

## Next Steps

1. **Start with example**: Try the ERC20 example below
2. **Create your query**: Write a custom SQL query for your use case
3. **Test small**: Load a small block range first (1000 blocks)
4. **Scale up**: Increase workers and block range for production loads
5. **Monitor**: Check Snowflake for data and use the `_current` views

For ERC20 transfers specifically, see the complete walkthrough in `apps/examples/erc20_example.md`.
