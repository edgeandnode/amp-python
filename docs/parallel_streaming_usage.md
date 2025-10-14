# Parallel Streaming Guide

This guide explains how to use the parallel streaming feature in amp-python to efficiently load large historical datasets and seamlessly transition to live streaming.

## Overview

Parallel streaming allows you to:
- **Parallelize historical data loading** across multiple workers for faster backfills
- **Automatically transition** from parallel catchup to continuous streaming
- **Configure partition sizes** to optimize for your data characteristics
- **Resume from specific block heights** when restarting

## Quick Start

### Basic Parallel Historical Load

Load a specific block range using multiple workers:

```python
from amp.client import Client
from amp.streaming.parallel import ParallelConfig

# Connect to Amp server
client = Client("grpc://your-amp-server:80")

# Configure PostgreSQL connection
client.configure_connection(
    name='my_postgres',
    loader='postgresql',
    config={
        'host': 'localhost',
        'database': 'blockchain_data',
        'user': 'postgres',
        'password': 'password'
    }
)

# Configure parallel execution
parallel_config = ParallelConfig(
    num_workers=4,                    # Use 4 parallel workers
    table_name='eth_firehose.blocks', # Source table in Amp
    min_block=0,                      # Start from block 0
    max_block=1000000,                # Load up to block 1M
    block_column='block_num'          # Column to partition on
)

# Execute parallel streaming query
query = "SELECT * FROM eth_firehose.blocks"
results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,                      # Enable streaming mode
    parallel_config=parallel_config   # Enable parallel execution
)

# Process results from all partitions
total_rows = 0
for result in results:
    if result.success:
        partition_id = result.metadata.get('partition_id', 'N/A')
        print(f"Partition {partition_id}: {result.rows_loaded:,} rows loaded")
        total_rows += result.rows_loaded
    else:
        print(f"Error: {result.error}")

print(f"\nTotal: {total_rows:,} rows loaded")
```

### Hybrid Mode: Parallel Catchup + Live Streaming

Automatically detect the current block height, catch up in parallel, then transition to continuous streaming:

```python
# Configure hybrid mode (max_block=None enables auto-detection)
parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=None,  # Auto-detect current max and transition to streaming
    block_column='block_num'
)

# Execute hybrid streaming
results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,
    parallel_config=parallel_config
)

# Process both parallel and continuous results
parallel_complete = False
for result in results:
    if 'partition_id' in result.metadata:
        # Parallel catchup phase
        print(f"Catchup partition {result.metadata['partition_id']}: {result.rows_loaded:,} rows")
    else:
        # Continuous streaming phase
        if not parallel_complete:
            print("\n🎯 Parallel catchup complete! Now streaming live data...")
            parallel_complete = True
        print(f"Live batch: {result.rows_loaded:,} rows")
```

## Configuration Options

### ParallelConfig Parameters

```python
@dataclass
class ParallelConfig:
    # Required
    num_workers: int            # Number of parallel workers (recommend 2-8)
    table_name: str             # Table name in Amp server
    block_column: str = 'block_num'  # Column to partition on

    # Optional - Block range
    min_block: Optional[int] = None   # Start block (default: 0)
    max_block: Optional[int] = None   # End block (None = auto-detect for hybrid mode)

    # Optional - Partitioning
    partition_size: Optional[int] = None  # Blocks per partition (auto-calculated if not set)

    # Optional - Performance tuning
    batch_size: int = 10000     # Rows per batch within each partition
```

### Choosing num_workers

The optimal number of workers depends on:
- **Network bandwidth**: More workers = more concurrent connections
- **Database write capacity**: Target database must handle parallel writes
- **Data characteristics**: Sparse data may benefit from fewer, larger partitions

**Recommendations**:
- Small datasets (<1M blocks): 2-4 workers
- Medium datasets (1M-10M blocks): 4-8 workers
- Large datasets (>10M blocks): 8+ workers (monitor database load)

### Partition Sizing

By default, partition size is calculated as: `(max_block - min_block) / num_workers`

You can override this for finer control:

```python
# Load 10M blocks with 100k blocks per partition
parallel_config = ParallelConfig(
    num_workers=8,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=10_000_000,
    partition_size=100_000,  # Custom partition size
    block_column='block_num'
)
```

This creates 100 partitions (10M / 100k), processed by 8 workers concurrently.

## Usage Patterns

### Pattern 1: One-time Historical Backfill

Load a specific historical range and exit when complete:

```python
parallel_config = ParallelConfig(
    num_workers=8,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=20_000_000,  # Specific end block
    block_column='block_num'
)

results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,
    parallel_config=parallel_config
)

# Consume all results
for result in results:
    pass  # Results are automatically loaded to database

print("Historical backfill complete!")
```

### Pattern 2: Resume from Checkpoint

Resume a previously interrupted load:

```python
# Assume we previously loaded up to block 5M
checkpoint_block = 5_000_000

parallel_config = ParallelConfig(
    num_workers=8,
    table_name='eth_firehose.blocks',
    min_block=checkpoint_block,  # Resume from checkpoint
    max_block=20_000_000,
    block_column='block_num'
)
```

### Pattern 3: Continuous Operation

Start near current block height and stream indefinitely:

```python
# Only load recent history, then stream live
parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.blocks',
    min_block=20_000_000,  # Start from recent block
    max_block=None,         # Auto-detect and transition to live streaming
    block_column='block_num'
)

results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,
    parallel_config=parallel_config
)

# Run indefinitely (Ctrl+C to stop)
try:
    for result in results:
        if result.success:
            print(f"Loaded {result.rows_loaded:,} rows")
except KeyboardInterrupt:
    print("\nStopped by user")
```

**Note on Reorg Buffer**: When transitioning from parallel catchup to continuous streaming, the system automatically starts continuous streaming from `detected_max_block - reorg_buffer` (default: 200 blocks). This overlap ensures that any reorgs that occurred during the parallel catchup phase are detected and handled properly. With reorg detection enabled, duplicate blocks are automatically handled correctly. The `reorg_buffer` can be customized via `ParallelConfig(reorg_buffer=N)`.

## Limitations

Currently, parallel streaming has the following limitations:

1. **Block-based partitioning only**: Only supports partitioning by block number columns (`block_num` or `_block_num`). Tables without block numbers cannot use parallel execution.

2. **Schema detection requires data**: Pre-flight schema detection requires at least 1 row in the source table. Empty tables will skip pre-flight creation and let workers handle it.

3. **Static partitioning**: Partitions are created upfront based on the block range. The system does not support dynamic repartitioning during execution.

4. **Thread-level parallelism**: Uses Python threads (ThreadPoolExecutor), not processes. For CPU-bound transformations, performance may be limited by the GIL.

5. **Single table queries**: The partitioning strategy works best with queries against a single table. Complex joins or unions may require careful query structuring.

6. **Reorg buffer configuration**: The `reorg_buffer` parameter (default: 200 blocks) is configurable but applies uniformly. Per-chain customization requires separate `ParallelConfig` instances.

## Performance Characteristics

### Speedup

Expected speedup with parallel loading:

| Workers | Speedup | Notes |
|---------|---------|-------|
| 1       | 1x      | Baseline (sequential) |
| 2       | 1.8-1.9x| Good for modest datasets |
| 4       | 3.5-3.8x| Optimal for most use cases |
| 8       | 6-7x    | Best for large datasets |
| 16+     | 8-12x   | Diminishing returns, increased overhead |

Actual speedup depends on:
- Network latency between client, Amp server, and target database
- Target database write throughput
- Data density per block

### Memory Usage

Memory consumption is proportional to:
- `num_workers * batch_size * row_size`

**Example**: With 8 workers, 10k batch size, and 1KB rows:
- `8 * 10,000 * 1KB ≈ 80MB` peak memory

Memory is released after each batch is written to the target database.

### When to Use Parallel vs Sequential

**Use Parallel Streaming When**:
- Loading historical data (>100k blocks)
- Initial backfills or catchups
- Target database can handle concurrent writes
- Network bandwidth is not the bottleneck

**Use Sequential Streaming When**:
- Near real-time (within last ~100 blocks)
- Target system has write concurrency limits
- Data is very sparse (few records per block)
- Memory constrained environments

## Advanced Configuration

### Reorg Buffer in Hybrid Mode

When using hybrid mode (`max_block=None`), the system automatically handles the transition from parallel catchup to continuous streaming with a built-in reorg buffer:

```
Timeline Example:
─────────────────────────────────────────────────────────────────────
T0: System detects current max block = 10,000,000
T1: Parallel workers start loading blocks 0 → 10,000,000
T2: Parallel workers complete (takes ~5 minutes)
    (Meanwhile, new blocks 10,000,001-10,000,050 have arrived)
T3: Continuous streaming starts from block 9,999,800 (10M - 200)
    ↓
    Blocks 9,999,800 → 10,000,000 loaded TWICE (parallel + streaming)
    └─ Reorg detection handles any inconsistencies
    └─ Database upsert/merge handles duplicates

Result: Zero data gaps, all reorgs caught ✓
─────────────────────────────────────────────────────────────────────
```

**Why 200 blocks (default)?**
- Ethereum average reorg depth: 1-5 blocks
- 200 blocks = ~40 minutes of history
- Provides safety margin for deep reorgs that occurred during catchup
- Small performance cost (200 blocks re-loaded) vs high data integrity value

**Customizing the Buffer:**
The reorg buffer is fully configurable via `ParallelConfig`:
```python
parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=None,  # Hybrid mode
    reorg_buffer=500,  # Increase for networks with deeper reorgs (e.g., testnets)
    block_column='block_num'
)
```

### Custom Partition Filters

For advanced use cases, you can combine parallel loading with additional WHERE clause filters:

```python
# Only load specific event types in parallel
query = """
SELECT * FROM eth_firehose.logs
WHERE topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
"""

parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.logs',
    min_block=0,
    max_block=20_000_000,
    block_column='block_num'
)

# The parallel executor will add block range filters to your WHERE clause
results = client.sql(query).load(
    connection='my_postgres',
    destination='transfer_events',
    stream=True,
    parallel_config=parallel_config
)
```

### Multiple Destination Loaders

Each worker streams data to the same destination table, so ensure your loader configuration supports concurrent writes:

**PostgreSQL**: Use connection pooling (automatically configured)
```python
config = {
    'host': 'localhost',
    'database': 'blockchain_data',
    'max_connections': 20,  # Ensure enough connections for all workers
    'batch_size': 10000
}
```

**Redis**: Supports concurrent writes by default
```python
config = {
    'host': 'localhost',
    'port': 6379
}
```

**DeltaLake**: Use appropriate table isolation level
```python
config = {
    'table_path': './data/blocks',
    'partition_by': ['block_num'],
    'optimize_after_write': False  # Optimize once after all workers complete
}
```

## Monitoring and Observability

### Logging

Enable INFO level logging to monitor progress:

```python
import logging
logging.basicConfig(level=logging.INFO)

# You'll see output like:
# INFO: Worker 0 processing blocks 0 to 250000
# INFO: Worker 1 processing blocks 250000 to 500000
# INFO: Partition 0 completed: 1,234,567 rows in 45.2s
```

### Result Metadata

Each LoadResult includes metadata about the partition:

```python
for result in results:
    if 'partition_id' in result.metadata:
        print(f"Partition: {result.metadata['partition_id']}")
        print(f"Block range: {result.metadata.get('block_range', 'N/A')}")
        print(f"Duration: {result.duration:.2f}s")
        print(f"Throughput: {result.ops_per_second:.0f} rows/s")
```

## Error Handling

### Partition Failures

If a worker encounters an error, it will:
1. Return a LoadResult with `success=False` and `error` message
2. Not retry automatically (to avoid infinite loops)
3. Allow other partitions to continue

```python
failed_partitions = []
for result in results:
    if not result.success:
        failed_partitions.append(result.metadata.get('partition_id'))
        print(f"Partition {result.metadata['partition_id']} failed: {result.error}")

if failed_partitions:
    print(f"\nFailed partitions: {failed_partitions}")
    # Implement retry logic as needed
```

### Connection Errors

If the Amp server connection fails:
- All workers will fail with a connection error
- The iterator will yield error results and terminate

### Graceful Shutdown

Press Ctrl+C to stop streaming:

```python
try:
    for result in results:
        # Process results...
        pass
except KeyboardInterrupt:
    print("\nShutdown requested, waiting for workers to finish current batches...")
    # Workers will complete current partitions and exit gracefully
```

## Troubleshooting

### Workers Hang or Don't Complete

**Issue**: Workers appear stuck after loading first batch
**Cause**: Query has `SETTINGS stream = true` which makes workers wait for new data
**Solution**: Don't include `SETTINGS stream = true` in the query when using parallel_config - the parallel executor handles this automatically

### Out of Memory Errors

**Issue**: Process crashes with OOM
**Cause**: Too many workers or batch size too large
**Solution**: Reduce `num_workers` or `batch_size`:

```python
parallel_config = ParallelConfig(
    num_workers=4,  # Reduced from 8
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=1_000_000,
    block_column='block_num'
)

# Also reduce batch_size in load() call
results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,
    parallel_config=parallel_config,
    batch_size=5000  # Reduced from 10000
)
```

### Database Connection Pool Exhausted

**Issue**: `OperationalError: connection pool exhausted`
**Cause**: Not enough database connections for number of workers
**Solution**: Increase `max_connections` in loader config:

```python
config = {
    'host': 'localhost',
    'database': 'blockchain_data',
    'user': 'postgres',
    'password': 'password',
    'max_connections': num_workers * 3  # At least 3x num_workers
}
```

### Uneven Partition Load Distribution

**Issue**: Some workers finish much faster than others
**Cause**: Data is not evenly distributed across block ranges
**Solution**: Use smaller partition sizes to create more partitions:

```python
parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=1_000_000,
    partition_size=50_000,  # Creates 20 partitions instead of 4
    block_column='block_num'
)
```

This allows workers to dynamically pick up new partitions as they finish.

## See Also

- [Streaming Guide](./streaming.md) - Sequential streaming documentation
- [Loader Configuration](./loaders.md) - Target database configuration
- [Performance Benchmarks](../performance_benchmarks.json) - Performance test results
