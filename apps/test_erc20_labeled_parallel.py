#!/usr/bin/env python3
"""
Real-world test: Load ERC20 transfers into Snowflake with token labels using parallel streaming.

This test demonstrates:
- CSV label joining: Enriches ERC20 transfer data with token metadata (symbol, name, decimals)
- Persistent job state: Snowflake-backed state management that survives process restarts
- Job resumption: Automatically resumes from last processed batch if interrupted
- Compact batch IDs: Each row gets _amp_batch_id for fast reorg invalidation
- Reorg history preservation: Temporal tracking with SCD Type 2 pattern (UPDATE instead of DELETE)

Features:
- Uses consistent table name ('erc20_labeled') instead of timestamp-based names
- State stored in Snowflake amp_stream_state table (not in-memory)
- Can safely interrupt and restart - will continue from where it left off
- No duplicate processing across runs

Usage:
    python apps/test_erc20_labeled_parallel.py [--blocks BLOCKS] [--workers WORKERS]

Example:
    python apps/test_erc20_labeled_parallel.py --blocks 100000 --workers 4

    # If interrupted, just run again - will resume automatically:
    python apps/test_erc20_labeled_parallel.py --blocks 100000 --workers 4
"""

import argparse
import os
import time
from pathlib import Path

from amp.client import Client
from amp.loaders.types import LabelJoinConfig
from amp.streaming.parallel import ParallelConfig


def get_recent_block_range(client: Client, num_blocks: int = 100_000):
    """Query amp server to get recent block range."""
    print(f'\n🔍 Detecting recent block range ({num_blocks:,} blocks)...')

    query = 'SELECT MAX(block_num) as max_block FROM eth_firehose.logs'
    result = client.get_sql(query, read_all=True)

    if result.num_rows == 0:
        raise RuntimeError('No data found in eth_firehose.logs')

    max_block = result.column('max_block')[0].as_py()
    if max_block is None:
        raise RuntimeError('No blocks found in eth_firehose.logs')

    min_block = max(0, max_block - num_blocks)

    print(f'✅ Block range: {min_block:,} to {max_block:,} ({max_block - min_block:,} blocks)')
    return min_block, max_block


def load_erc20_transfers_with_labels(num_blocks: int = 100_000, num_workers: int = 4, flush_interval: float = 1.0):
    """Load ERC20 transfers with token labels using Snowpipe Streaming and parallel streaming."""

    # Initialize client
    server_url = os.getenv('AMP_SERVER_URL', 'grpc://34.27.238.174:80')
    client = Client(server_url)
    print(f'📡 Connected to amp server: {server_url}')

    # Configure token metadata labels
    project_root = Path(__file__).parent.parent
    token_csv_path = project_root / 'data' / 'eth_mainnet_token_metadata.csv'

    if not token_csv_path.exists():
        raise FileNotFoundError(
            f'Token metadata CSV not found at {token_csv_path}. Please ensure the file exists in the data directory.'
        )

    print(f'\n🏷️  Configuring token metadata labels from: {token_csv_path}')
    client.configure_label('token_metadata', str(token_csv_path))
    print(f'✅ Loaded token labels: {len(client.label_manager.get_label("token_metadata"))} tokens')

    # Get recent block range
    min_block, max_block = get_recent_block_range(client, num_blocks)

    # Use consistent table name for job persistence (not timestamp-based)
    table_name = 'erc20_labeled'
    print(f'\n📊 Target table: {table_name}')
    print('🌊 Using Snowpipe Streaming with label joining')
    print('💾 State Management: ENABLED (Snowflake-backed persistent state)')
    print('🕐 Reorg History: ENABLED (temporal tracking with _current and _history views)')
    print('♻️  Job Resumption: ENABLED (automatically resumes if interrupted)')

    # ERC20 Transfer event signature
    transfer_sig = 'Transfer(address indexed from, address indexed to, uint256 value)'

    # ERC20 transfer query - decode from raw logs and include token address
    # The address is binary, but our join logic will auto-convert to match CSV hex strings
    erc20_query = f"""
        select
            pc.block_num,
            pc.block_hash,
            pc.timestamp,
            pc.tx_hash,
            pc.tx_index,
            pc.log_index,
            pc.address as token_address,
            pc.dec['from'] as from_address,
            pc.dec['to'] as to_address,
            pc.dec['value'] as value
        from (
            select
                l.block_num,
                l.block_hash,
                l.tx_hash,
                l.tx_index,
                l.log_index,
                l.timestamp,
                l.address,
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{transfer_sig}') as dec
            from eth_firehose.logs l
            where
                l.topic0 = evm_topic('{transfer_sig}') and
                l.topic3 IS NULL) pc
    """

    # Configure Snowflake connection with Snowpipe Streaming
    snowflake_config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'private_key': os.getenv('SNOWFLAKE_PRIVATE_KEY'),
        'loading_method': 'snowpipe_streaming',  # Use Snowpipe Streaming
        'pool_size': num_workers + 2,  # Set pool size to match workers + buffer
        'streaming_buffer_flush_interval': int(flush_interval),  # Buffer flush interval in seconds
        'preserve_reorg_history': True,  # Enable reorg history preservation (SCD Type 2)
        # Enable unified state management for idempotency and resumability
        'state': {
            'enabled': True,  # Enable state tracking
            'storage': 'snowflake',  # Use Snowflake-backed persistent state (survives restarts)
            'store_batch_id': True,  # Store compact batch IDs in data table
        },
    }

    client.configure_connection(name='snowflake_snowpipe_labeled', loader='snowflake', config=snowflake_config)

    # Configure parallel execution
    parallel_config = ParallelConfig(
        num_workers=num_workers,
        table_name='eth_firehose.logs',
        min_block=min_block,
        max_block=max_block,
        block_column='block_num',
    )

    print(f'\n🚀 Starting parallel Snowpipe Streaming load with {num_workers} workers...')
    print('🏷️  Joining with token labels on token_address column')
    print('   Only transfers from tokens in the metadata CSV will be loaded (inner join)\n')

    start_time = time.time()

    # Configure label joining with the new structured API
    label_config = LabelJoinConfig(
        label_name='token_metadata',
        label_key_column='token_address',  # Key in CSV
        stream_key_column='token_address',  # Key in streaming data
    )

    # Load data in parallel with label joining
    results = list(
        client.sql(erc20_query).load(
            connection='snowflake_snowpipe_labeled',
            destination=table_name,
            stream=True,
            parallel_config=parallel_config,
            label_config=label_config,
        )
    )

    duration = time.time() - start_time

    # Calculate statistics
    total_rows = sum(r.rows_loaded for r in results if r.success)
    failures = [r for r in results if not r.success]
    rows_per_sec = total_rows / duration if duration > 0 else 0
    failed_count = len(failures)

    # Print results
    print(f'\n{"=" * 70}')
    if failures:
        print(f'⚠️  ERC20 Labeled Load Complete (with {failed_count} failures)')
    else:
        print('🎉 ERC20 Labeled Load Complete!')
    print(f'{"=" * 70}')
    print(f'📊 Table name:       {table_name}')
    print(f'📦 Block range:      {min_block:,} to {max_block:,}')
    print(f'📈 Rows loaded:      {total_rows:,}')
    print('🏷️  Label columns:    symbol, name, decimals (from CSV)')
    print(f'⏱️  Duration:         {duration:.2f}s')
    print(f'🚀 Throughput:       {rows_per_sec:,.0f} rows/sec')
    print(f'👷 Workers:          {num_workers} configured')
    print(f'✅ Successful:       {len(results) - failed_count}/{len(results)} batches')
    if failed_count > 0:
        print(f'❌ Failed batches:   {failed_count}')
        print('\nFirst 3 errors:')
        for f in failures[:3]:
            print(f'   - {f.error}')
    if total_rows > 0:
        print(f'📊 Avg rows/block:   {total_rows / (max_block - min_block):.0f}')
    print(f'{"=" * 70}')

    print(f'\n✅ Table "{table_name}" is ready in Snowflake with token labels!')
    print('\n📊 Created views:')
    print(f'   • {table_name}_current - Active data only (for queries)')
    print(f'   • {table_name}_history - All data including reorged rows')
    print('\n💡 Sample queries:')
    print('   -- View current transfers with token info (recommended)')
    print('   SELECT token_address, symbol, name, decimals, from_address, to_address, value')
    print(f'   FROM {table_name}_current LIMIT 10;')
    print('\n   -- Top tokens by transfer count (current data only)')
    print('   SELECT symbol, name, COUNT(*) as transfer_count')
    print(f'   FROM {table_name}_current')
    print('   GROUP BY symbol, name')
    print('   ORDER BY transfer_count DESC')
    print('   LIMIT 10;')
    print('\n   -- View batch IDs (for identifying data batches)')
    print('   SELECT DISTINCT _amp_batch_id, COUNT(*) as row_count')
    print(f'   FROM {table_name}_current')
    print('   GROUP BY _amp_batch_id')
    print('   ORDER BY row_count DESC LIMIT 10;')
    print('\n   -- View reorg history (invalidated rows)')
    print('   SELECT _amp_reorg_id, _amp_reorg_block, _amp_valid_from, _amp_valid_to, COUNT(*) as affected_rows')
    print(f'   FROM {table_name}_history')
    print('   WHERE _amp_is_current = FALSE')
    print('   GROUP BY _amp_reorg_id, _amp_reorg_block, _amp_valid_from, _amp_valid_to')
    print('   ORDER BY _amp_valid_to DESC;')
    print('\n💡 Note: Snowpipe Streaming data may take a few moments to be queryable')
    print('💡 Note: Only transfers for tokens in the metadata CSV are included (inner join)')
    print('💡 Note: Persistent state in Snowflake prevents duplicate batches across runs')
    print('💡 Note: Job automatically resumes from last processed batch if interrupted')
    print('💡 Note: Reorged data is preserved with temporal tracking (not deleted)')
    print(f'💡 Note: Use {table_name}_current for queries, {table_name}_history for full history')

    return table_name, total_rows, duration


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load ERC20 transfers with token labels into Snowflake using Snowpipe Streaming'
    )
    parser.add_argument(
        '--blocks', type=int, default=100_000, help='Number of recent blocks to load (default: 100,000)'
    )
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument(
        '--flush-interval',
        type=float,
        default=1.0,
        help='Snowpipe Streaming buffer flush interval in seconds (default: 1.0)',
    )

    args = parser.parse_args()

    try:
        load_erc20_transfers_with_labels(
            num_blocks=args.blocks, num_workers=args.workers, flush_interval=args.flush_interval
        )
    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted by user')
    except Exception as e:
        print(f'\n\n❌ Error: {e}')
        import traceback

        traceback.print_exc()
        raise
