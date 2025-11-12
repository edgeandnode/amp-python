#!/usr/bin/env python3
"""
Generalized Snowflake parallel streaming loader.

Load data from any SQL query into Snowflake using parallel streaming with
optional label joining, persistent state management, and reorg history tracking.

Features:
- Custom SQL queries via file
- Parallel execution with automatic partitioning
- Optional CSV label joining
- Snowpipe Streaming or stage loading
- Persistent state management (job resumption)
- Reorg history preservation with temporal tracking
- Automatic block range detection or explicit ranges

Usage:
    # Basic usage with custom query
    python apps/snowflake_parallel_loader.py \\
        --query-file my_query.sql \\
        --table-name my_table \\
        --blocks 50000

    # With labels
    python apps/snowflake_parallel_loader.py \\
        --query-file erc20_transfers.sql \\
        --table-name erc20_transfers \\
        --label-csv data/tokens.csv \\
        --label-name tokens \\
        --label-key token_address \\
        --stream-key token_address \\
        --blocks 100000

    # Explicit block range with stage loading
    python apps/snowflake_parallel_loader.py \\
        --query-file logs_query.sql \\
        --table-name raw_logs \\
        --min-block 19000000 \\
        --max-block 19100000 \\
        --loading-method stage
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from amp.client import Client
from amp.loaders.types import LabelJoinConfig
from amp.streaming.parallel import ParallelConfig


def configure_logging(verbose: bool = False):
    """Configure logging to suppress verbose Snowflake/Snowpipe output.

    Args:
        verbose: If True, enable verbose logging from Snowflake libraries.
                 If False (default), suppress verbose output.
    """
    # Configure root logger first
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    if not verbose:
        # Suppress verbose logs from Snowflake libraries
        logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
        logging.getLogger('snowflake.snowpark').setLevel(logging.WARNING)
        logging.getLogger('snowpipe.streaming').setLevel(logging.WARNING)
        logging.getLogger('snowflake.connector.network').setLevel(logging.ERROR)
        logging.getLogger('snowflake.connector.cursor').setLevel(logging.WARNING)
        logging.getLogger('snowflake.connector.connection').setLevel(logging.WARNING)

        # Suppress urllib3 connection pool logs
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    else:
        # Enable verbose logging for debugging
        logging.getLogger('snowflake.connector').setLevel(logging.DEBUG)
        logging.getLogger('snowflake.snowpark').setLevel(logging.DEBUG)
        logging.getLogger('snowpipe.streaming').setLevel(logging.DEBUG)

    # Keep amp logs at INFO level
    logging.getLogger('amp').setLevel(logging.INFO)


def load_query_file(query_file_path: str) -> str:
    """Load SQL query from file."""
    path = Path(query_file_path)
    if not path.exists():
        raise FileNotFoundError(f'Query file not found: {query_file_path}')

    query = path.read_text().strip()
    if not query:
        raise ValueError(f'Query file is empty: {query_file_path}')

    print(f'📄 Loaded query from: {query_file_path}')
    return query


def setup_labels(client: Client, args) -> None:
    """Configure labels if label CSV is provided."""
    if not args.label_csv:
        return

    # Validate label arguments
    if not args.label_name:
        raise ValueError('--label-name is required when using --label-csv')
    if not args.label_key:
        raise ValueError('--label-key is required when using --label-csv')
    if not args.stream_key:
        raise ValueError('--stream-key is required when using --label-csv')

    label_path = Path(args.label_csv)
    if not label_path.exists():
        raise FileNotFoundError(f'Label CSV not found: {args.label_csv}')

    print(f'\n🏷️  Configuring labels from: {args.label_csv}')
    client.configure_label(args.label_name, str(label_path))
    label_count = len(client.label_manager.get_label(args.label_name))
    print(f'✅ Loaded {label_count} label records')


def get_recent_block_range(client: Client, source_table: str, block_column: str, num_blocks: int):
    """Query server to auto-detect recent block range."""
    print(f'\n🔍 Detecting recent block range ({num_blocks:,} blocks)...')
    print(f'   Source: {source_table}.{block_column}')

    query = f'SELECT MAX({block_column}) as max_block FROM {source_table}'
    result = client.get_sql(query, read_all=True)

    if result.num_rows == 0:
        raise RuntimeError(f'No data found in {source_table}')

    max_block = result.column('max_block')[0].as_py()
    if max_block is None:
        raise RuntimeError(f'No blocks found in {source_table}')

    min_block = max(0, max_block - num_blocks)

    print(f'✅ Block range: {min_block:,} to {max_block:,} ({max_block - min_block:,} blocks)')
    return min_block, max_block


def parse_block_range(args, client: Client):
    """Parse or detect block range from arguments."""
    # Explicit range provided
    if args.min_block is not None and args.max_block is not None:
        print(f'\n📊 Using explicit block range: {args.min_block:,} to {args.max_block:,}')
        return args.min_block, args.max_block

    # Auto-detect range
    if args.blocks:
        return get_recent_block_range(client, args.source_table, args.block_column, args.blocks)

    raise ValueError('Must provide either --blocks or both --min-block and --max-block')


def build_snowflake_config(args):
    """Build Snowflake connection configuration from arguments."""
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'private_key': os.getenv('SNOWFLAKE_PRIVATE_KEY'),
        'loading_method': args.loading_method,
        'pool_size': args.pool_size or (args.workers + 2),
        'preserve_reorg_history': args.preserve_reorg_history,
    }

    # Add streaming-specific config
    if args.loading_method == 'snowpipe_streaming':
        config['streaming_buffer_flush_interval'] = int(args.flush_interval)

    # Add state management config
    if not args.disable_state:
        config['state'] = {
            'enabled': True,
            'storage': 'snowflake',
            'store_batch_id': True,
        }

    return config


def build_parallel_config(args, min_block: int, max_block: int, query: str):
    """Build parallel execution configuration."""
    return ParallelConfig(
        num_workers=args.workers,
        table_name=args.source_table,
        min_block=min_block,
        max_block=max_block,
        block_column=args.block_column,
    )


def build_label_config(args):
    """Build label join configuration if labels are configured."""
    if not args.label_csv:
        return None

    return LabelJoinConfig(
        label_name=args.label_name,
        label_key_column=args.label_key,
        stream_key_column=args.stream_key,
    )


def print_configuration(args, min_block: int, max_block: int, has_labels: bool):
    """Print configuration summary."""
    print(f'\n📊 Target table: {args.table_name}')
    print(f'🌊 Loading method: {args.loading_method}')
    print(f'💾 State Management: {"DISABLED" if args.disable_state else "ENABLED (Snowflake-backed)"}')
    print(f'🕐 Reorg History: {"ENABLED" if args.preserve_reorg_history else "DISABLED"}')
    if not args.disable_state:
        print('♻️  Job Resumption: ENABLED (automatically resumes if interrupted)')
    if has_labels:
        print(f'🏷️  Label Joining: ENABLED ({args.label_name})')


def print_results(
    results,
    table_name: str,
    min_block: int,
    max_block: int,
    duration: float,
    num_workers: int,
    has_labels: bool,
    label_columns: str = '',
):
    """Print execution results and sample queries."""
    # Calculate statistics
    total_rows = sum(r.rows_loaded for r in results if r.success)
    failures = [r for r in results if not r.success]
    rows_per_sec = total_rows / duration if duration > 0 else 0
    failed_count = len(failures)

    # Print results summary
    print(f'\n{"=" * 70}')
    if failures:
        print(f'⚠️  Load Complete (with {failed_count} failures)')
    else:
        print('🎉 Load Complete!')
    print(f'{"=" * 70}')
    print(f'📊 Table name:       {table_name}')
    print(f'📦 Block range:      {min_block:,} to {max_block:,}')
    print(f'📈 Rows loaded:      {total_rows:,}')
    if has_labels:
        print(f'🏷️  Label columns:    {label_columns}')
    print(f'⏱️  Duration:         {duration:.2f}s')
    print(f'🚀 Throughput:       {rows_per_sec:,.0f} rows/sec')
    print(f'👷 Workers:          {num_workers} configured')
    print(f'✅ Successful:       {len(results) - failed_count}/{len(results)} batches')

    if failed_count > 0:
        print(f'❌ Failed batches:   {failed_count}')
        print('\nFirst 3 errors:')
        for f in failures[:3]:
            print(f'   - {f.error}')

    if total_rows > 0 and max_block > min_block:
        print(f'📊 Avg rows/block:   {total_rows / (max_block - min_block):.0f}')
    print(f'{"=" * 70}')

    if not has_labels:
        print('   • No labels were configured - data loaded without enrichment')


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Load data into Snowflake using parallel streaming with custom SQL queries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Required arguments
    required = parser.add_argument_group('required arguments')
    required.add_argument('--query-file', required=True, help='Path to SQL query file to execute')
    required.add_argument('--table-name', required=True, help='Destination Snowflake table name')

    # Block range arguments (mutually exclusive groups)
    block_range = parser.add_argument_group('block range')
    block_range.add_argument('--blocks', type=int, help='Number of recent blocks to load (auto-detect range)')
    block_range.add_argument('--min-block', type=int, help='Explicit start block (requires --max-block)')
    block_range.add_argument('--max-block', type=int, help='Explicit end block (requires --min-block)')
    block_range.add_argument(
        '--source-table',
        default='eth_firehose.logs',
        help='Table for block range detection (default: eth_firehose.logs)',
    )
    block_range.add_argument(
        '--block-column', default='block_num', help='Column name for block partitioning (default: block_num)'
    )

    # Label configuration (all optional)
    labels = parser.add_argument_group('label configuration (optional)')
    labels.add_argument('--label-csv', help='Path to CSV file with label data')
    labels.add_argument('--label-name', help='Label identifier (required if --label-csv provided)')
    labels.add_argument('--label-key', help='CSV column for joining (required if --label-csv provided)')
    labels.add_argument('--stream-key', help='Stream column for joining (required if --label-csv provided)')

    # Snowflake configuration
    snowflake = parser.add_argument_group('snowflake configuration')
    snowflake.add_argument(
        '--connection-name', help='Snowflake connection name (default: auto-generated from table name)'
    )
    snowflake.add_argument(
        '--loading-method',
        choices=['snowpipe_streaming', 'stage', 'insert'],
        default='snowpipe_streaming',
        help='Snowflake loading method (default: snowpipe_streaming)',
    )
    snowflake.add_argument(
        '--preserve-reorg-history',
        action='store_true',
        default=True,
        help='Enable reorg history preservation (default: enabled)',
    )
    snowflake.add_argument(
        '--no-preserve-reorg-history',
        action='store_false',
        dest='preserve_reorg_history',
        help='Disable reorg history preservation',
    )
    snowflake.add_argument('--disable-state', action='store_true', help='Disable state management (job resumption)')
    snowflake.add_argument('--pool-size', type=int, help='Connection pool size (default: workers + 2)')

    # Parallel execution configuration
    parallel = parser.add_argument_group('parallel execution')
    parallel.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parallel.add_argument(
        '--flush-interval',
        type=float,
        default=1.0,
        help='Snowpipe Streaming buffer flush interval in seconds (default: 1.0)',
    )

    # Server configuration
    parser.add_argument(
        '--server',
        default=os.getenv('AMP_SERVER_URL', 'grpc://34.27.238.174:80'),
        help='AMP server URL (default: from AMP_SERVER_URL env or grpc://34.27.238.174:80)',
    )

    # Logging configuration
    parser.add_argument(
        '--verbose', action='store_true', help='Enable verbose logging from Snowflake libraries (default: suppressed)'
    )

    args = parser.parse_args()

    # Configure logging to suppress verbose Snowflake output (unless --verbose is set)
    configure_logging(verbose=args.verbose)

    # Validate block range arguments
    if args.min_block is not None and args.max_block is None:
        parser.error('--max-block is required when using --min-block')
    if args.max_block is not None and args.min_block is None:
        parser.error('--min-block is required when using --max-block')
    if args.min_block is None and args.max_block is None and args.blocks is None:
        parser.error('Must provide either --blocks or both --min-block and --max-block')

    try:
        client = Client(args.server)
        print(f'📡 Connected to AMP server: {args.server}')

        query = load_query_file(args.query_file)
        setup_labels(client, args)
        has_labels = bool(args.label_csv)
        min_block, max_block = parse_block_range(args, client)
        print_configuration(args, min_block, max_block, has_labels)
        snowflake_config = build_snowflake_config(args)
        connection_name = args.connection_name or f'snowflake_{args.table_name}'
        client.configure_connection(name=connection_name, loader='snowflake', config=snowflake_config)
        parallel_config = build_parallel_config(args, min_block, max_block, query)
        label_config = build_label_config(args)

        print(f'\n🚀 Starting parallel {args.loading_method} load with {args.workers} workers...')
        if has_labels:
            print(f'🏷️  Joining with labels on {args.stream_key} column')
        print()

        start_time = time.time()

        # Execute parallel load
        results = list(
            client.sql(query).load(
                connection=connection_name,
                destination=args.table_name,
                stream=True,
                parallel_config=parallel_config,
                label_config=label_config,
            )
        )

        duration = time.time() - start_time

        # Print results
        label_columns = f'{args.label_key} joined columns' if has_labels else ''
        print_results(results, args.table_name, min_block, max_block, duration, args.workers, has_labels, label_columns)

        return args.table_name, sum(r.rows_loaded for r in results if r.success), duration

    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted by user')
        sys.exit(1)
    except Exception as e:
        print(f'\n\n❌ Error: {e}')
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
