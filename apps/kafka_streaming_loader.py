#!/usr/bin/env python3
"""Stream data to Kafka with resume watermark support."""

import argparse
import logging
import os
from pathlib import Path

from amp.client import Client
from amp.loaders.types import LabelJoinConfig
from amp.streaming import BlockRange, ResumeWatermark


def get_block_hash(client: Client, raw_dataset: str, block_num: int) -> str:
    """Get block hash from dataset.blocks table."""
    query = f'SELECT hash FROM "{raw_dataset}".blocks WHERE block_num = {block_num} LIMIT 1'
    result = client.get_sql(query, read_all=True)
    hash_val = result.to_pydict()['hash'][0]
    return '0x' + hash_val.hex() if isinstance(hash_val, bytes) else hash_val


def get_latest_block(client: Client, raw_dataset: str) -> int:
    """Get latest block number from dataset.blocks table."""
    query = f'SELECT block_num FROM "{raw_dataset}".blocks ORDER BY block_num DESC LIMIT 1'
    result = client.get_sql(query, read_all=True)
    return result.to_pydict()['block_num'][0]


def create_watermark(client: Client, raw_dataset: str, network: str, start_block: int) -> ResumeWatermark:
    """Create a resume watermark for the given start block."""
    watermark_block = start_block - 1
    watermark_hash = get_block_hash(client, raw_dataset, watermark_block)
    return ResumeWatermark(
        ranges=[BlockRange(network=network, start=watermark_block, end=watermark_block, hash=watermark_hash)]
    )


def main(
    amp_server: str,
    kafka_brokers: str,
    topic: str,
    query_file: str,
    raw_dataset: str,
    network: str,
    start_block: int = None,
    label_csv: str = None,
    state_dir: str = '.amp_state',
):
    client = Client(amp_server)
    print(f'Connected to {amp_server}')

    if label_csv and Path(label_csv).exists():
        client.configure_label('tokens', label_csv)
        print(f'Loaded {len(client.label_manager.get_label("tokens"))} labels from {label_csv}')
        label_config = LabelJoinConfig(
            label_name='tokens', label_key_column='token_address', stream_key_column='token_address'
        )
    else:
        label_config = None

    client.configure_connection(
        'kafka',
        'kafka',
        {
            'bootstrap_servers': kafka_brokers,
            'client_id': 'amp-kafka-loader',
            'state': {'enabled': True, 'storage': 'lmdb', 'data_dir': state_dir},
        },
    )

    with open(query_file) as f:
        query = f.read()

    if start_block is None:
        start_block = get_latest_block(client, raw_dataset)

    resume_watermark = create_watermark(client, raw_dataset, network, start_block) if start_block > 0 else None

    print(f'Starting query from block {start_block}')
    print(f'Streaming to Kafka: {kafka_brokers} -> {topic}\n')

    batch_count = 0
    for result in client.sql(query).load(
        'kafka', topic, stream=True, label_config=label_config, resume_watermark=resume_watermark
    ):
        if result.success:
            batch_count += 1
            if batch_count == 1 and result.metadata:
                print(f'First batch: {result.metadata.get("block_ranges")}\n')
            print(f'Batch {batch_count}: {result.rows_loaded} rows in {result.duration:.2f}s')
        else:
            print(f'Error: {result.error}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stream data to Kafka with resume watermark')
    parser.add_argument('--amp-server', default=os.getenv('AMP_SERVER_URL', 'grpc://127.0.0.1:1602'))
    parser.add_argument('--kafka-brokers', default='localhost:9092')
    parser.add_argument('--topic', required=True)
    parser.add_argument('--query-file', required=True)
    parser.add_argument(
        '--raw-dataset', required=True, help='Dataset name for the raw dataset of the chain (e.g., anvil, eth_firehose)'
    )
    parser.add_argument('--network', default='anvil')
    parser.add_argument('--start-block', type=int, help='Start from specific block (default: latest - 10)')
    parser.add_argument('--label-csv', help='Optional CSV for label joining')
    parser.add_argument('--state-dir', default='.amp_state', help='Directory for LMDB state storage')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    args = parser.parse_args()

    if args.log_level:
        logging.basicConfig(
            level=getattr(logging, args.log_level), format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        )

    try:
        main(
            amp_server=args.amp_server,
            kafka_brokers=args.kafka_brokers,
            topic=args.topic,
            query_file=args.query_file,
            raw_dataset=args.raw_dataset,
            network=args.network,
            start_block=args.start_block,
            label_csv=args.label_csv,
            state_dir=args.state_dir,
        )
    except KeyboardInterrupt:
        print('\n\nStopped by user')
    except Exception as e:
        print(f'\nError: {e}')
        raise
