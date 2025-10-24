#!/usr/bin/env python3
"""
Real-world test: Load ERC20 transfers into Snowflake using parallel streaming.

Usage:
    python app/test_erc20_parallel_load.py [--blocks BLOCKS] [--workers WORKERS]

Example:
    python app/test_erc20_parallel_load.py --blocks 100000 --workers 8
"""

import argparse
import os
import time
from datetime import datetime

from amp.client import Client
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


def load_erc20_transfers(num_blocks: int = 100_000, num_workers: int = 8):
    """Load ERC20 transfers using parallel streaming."""

    # Initialize client
    server_url = os.getenv('AMP_SERVER_URL', 'grpc://34.27.238.174:80')
    client = Client(server_url)
    print(f'📡 Connected to amp server: {server_url}')

    # Get recent block range
    min_block, max_block = get_recent_block_range(client, num_blocks)

    # Generate unique table name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    table_name = f'erc20_transfers_{timestamp}'
    print(f'\n📊 Target table: {table_name}')

    # ERC20 Transfer event signature
    transfer_sig = 'Transfer(address indexed from, address indexed to, uint256 value)'

    # ERC20 transfer query with corrected syntax
    erc20_query = f"""
        select 
            pc.block_num,
            pc.block_hash,
            pc.timestamp,
            pc.tx_hash,
            pc.tx_index,
            pc.log_index,
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
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{transfer_sig}') as dec
            from eth_firehose.logs l
            where 
                l.topic0 = evm_topic('{transfer_sig}') and
                l.topic3 IS NULL) pc 
    """

    # Configure Snowflake connection
    snowflake_config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'private_key': os.getenv('SNOWFLAKE_PRIVATE_KEY'),
        'loading_method': 'stage',  # Use fast bulk loading via COPY INTO
    }

    client.configure_connection(name='snowflake_erc20', loader='snowflake', config=snowflake_config)

    # Configure parallel execution
    parallel_config = ParallelConfig(
        num_workers=num_workers,
        table_name='eth_firehose.logs',
        min_block=min_block,
        max_block=max_block,
        block_column='block_num',
    )

    print(f'\n🚀 Starting parallel load with {num_workers} workers...\n')

    start_time = time.time()

    # Load data in parallel (will stop after processing the block range)
    results = list(
        client.sql(erc20_query).load(
            connection='snowflake_erc20', destination=table_name, stream=True, parallel_config=parallel_config
        )
    )

    duration = time.time() - start_time

    # Calculate statistics
    total_rows = sum(r.rows_loaded for r in results if r.success)
    rows_per_sec = total_rows / duration if duration > 0 else 0
    partitions = [r for r in results if 'partition_id' in r.metadata]
    successful_workers = len(partitions)
    failed_workers = num_workers - successful_workers

    # Print results
    print(f'\n{"=" * 70}')
    print('🎉 ERC20 Parallel Load Complete!')
    print(f'{"=" * 70}')
    print(f'📊 Table name:       {table_name}')
    print(f'📦 Block range:      {min_block:,} to {max_block:,}')
    print(f'📈 Rows loaded:      {total_rows:,}')
    print(f'⏱️  Duration:         {duration:.2f}s')
    print(f'🚀 Throughput:       {rows_per_sec:,.0f} rows/sec')
    print(f'👷 Workers:          {successful_workers}/{num_workers} succeeded')
    if failed_workers > 0:
        print(f'⚠️  Failed workers:   {failed_workers}')
    print(f'📊 Avg rows/block:   {total_rows / (max_block - min_block):.0f}')
    print(f'{"=" * 70}')

    print(f'\n✅ Table "{table_name}" is ready in Snowflake for testing!')
    print(f'   Query it with: SELECT * FROM {table_name} LIMIT 10;')

    return table_name, total_rows, duration


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load ERC20 transfers into Snowflake using parallel streaming')
    parser.add_argument(
        '--blocks', type=int, default=100_000, help='Number of recent blocks to load (default: 100,000)'
    )
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers (default: 8)')

    args = parser.parse_args()

    try:
        load_erc20_transfers(num_blocks=args.blocks, num_workers=args.workers)
    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted by user')
    except Exception as e:
        print(f'\n\n❌ Error: {e}')
        raise
