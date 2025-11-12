# SQL Query Examples for Snowflake Parallel Loader

This directory contains example SQL queries that can be used with `snowflake_parallel_loader.py`.

## Query Requirements

### Required Columns

Your query **must** include:

- **`block_num`** (or specify a different column with `--block-column`)
  - Used for partitioning data across parallel workers
  - Should be an integer column representing block numbers

### Optional Columns for Label Joining

If you plan to use `--label-csv` for enrichment:

- Include a column that matches your label key (e.g., `token_address`)
- The column can be binary or string format
- The loader will auto-convert binary addresses to hex strings for matching

### Best Practices

1. **Filter early**: Apply WHERE clauses in your query to reduce data volume
2. **Select specific columns**: Avoid `SELECT *` for better performance
3. **Use event decoding**: Use `evm_decode()` and `evm_topic()` for Ethereum events
4. **Include metadata**: Include useful columns like `block_hash`, `timestamp`, `tx_hash`

## Example Queries

### ERC20 Transfers (with labels)

See `erc20_transfers.sql` for a complete example that:
- Decodes Transfer events from raw logs
- Filters for standard ERC20 transfers (topic3 IS NULL)
- Includes `token_address` for label joining
- Can be enriched with token metadata (symbol, name, decimals)

Usage:
```bash
python apps/snowflake_parallel_loader.py \
  --query-file apps/queries/erc20_transfers.sql \
  --table-name erc20_transfers \
  --label-csv data/eth_mainnet_token_metadata.csv \
  --label-name tokens \
  --label-key token_address \
  --stream-key token_address \
  --blocks 50000
```

### Simple Log Query (without labels)

```sql
-- Basic logs query - no decoding
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
where block_num >= 19000000
```

Usage:
```bash
python apps/snowflake_parallel_loader.py \
  --query-file my_logs.sql \
  --table-name raw_logs \
  --min-block 19000000 \
  --max-block 19100000
```

### Custom Event Decoding

```sql
-- Decode Uniswap V2 Swap events
select
    l.block_num,
    l.timestamp,
    l.address as pool_address,
    evm_decode(
        l.topic1, l.topic2, l.topic3, l.data,
        'Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)'
    ) as swap_data
from eth_firehose.logs l
where l.topic0 = evm_topic('Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)')
```

## Testing Your Query

Before running a full parallel load, test your query with a small block range:

```bash
# Test with just 1000 blocks
python apps/snowflake_parallel_loader.py \
  --query-file your_query.sql \
  --table-name test_table \
  --blocks 1000 \
  --workers 2
```

## Query Performance Tips

1. **Partition size**: Default partition size is optimized for `block_num` ranges
2. **Worker count**: More workers = smaller partitions. Start with 4-8 workers
3. **Block range**: Larger ranges take longer but have better per-block efficiency
4. **Event filtering**: Use `topic0` filters to reduce data scanned
5. **Label joins**: Inner joins reduce output rows to only matching records

## Troubleshooting

**Error: "No blocks found"**
- Check that your query's source table contains data
- Verify `--source-table` matches your query's FROM clause

**Error: "Column not found: block_num"**
- Your query must include a `block_num` column
- Or specify a different column with `--block-column`

**Label join not working**
- Ensure `--stream-key` column exists in your query
- Check that column types match between query and CSV
- Verify CSV file has a header row with the `--label-key` column
