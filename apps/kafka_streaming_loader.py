#!/usr/bin/env python3
"""Stream data to Kafka with resume watermark support."""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from amp.client import Client
from amp.loaders.types import LabelJoinConfig
from amp.streaming import BlockRange, ResumeWatermark

logger = logging.getLogger('amp.kafka_streaming_loader')

RETRYABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def retry_with_backoff(func, max_retries=5, initial_delay=1.0, max_delay=60.0, backoff_factor=2.0):
    """Execute function with exponential backoff retry on transient errors."""
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except RETRYABLE_ERRORS as e:
            last_exception = e
            if attempt == max_retries:
                logger.error(f'Max retries ({max_retries}) exceeded: {e}')
                raise
            logger.warning(f'Attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s...')
            time.sleep(delay)
            delay = min(delay * backoff_factor, max_delay)

    raise last_exception


def get_block_hash(client: Client, raw_dataset: str, block_num: int) -> str:
    """Get block hash from dataset.blocks table."""
    query = f'SELECT hash FROM "{raw_dataset}".blocks WHERE block_num = {block_num} LIMIT 1'
    result = client.get_sql(query, read_all=True)
    hash_val = result.to_pydict()['hash'][0]
    return '0x' + hash_val.hex() if isinstance(hash_val, bytes) else hash_val


def get_latest_block(client: Client, raw_dataset: str) -> int:
    """Get latest block number from dataset.blocks table."""
    query = f'SELECT block_num FROM "{raw_dataset}".blocks ORDER BY block_num DESC LIMIT 1'
    logger.debug(f'Fetching latest block from {raw_dataset}')
    logger.debug(f'Query: {query}')
    result = client.get_sql(query, read_all=True)
    block_num = result.to_pydict()['block_num'][0]
    logger.info(f'Latest block in {raw_dataset}: {block_num}')
    return block_num


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
    auth: bool = False,
    auth_token: str = None,
    max_retries: int = 5,
    retry_delay: float = 1.0,
    kafka_config: dict = None,
):
    def connect():
        return Client(amp_server, auth=auth, auth_token=auth_token)

    client = retry_with_backoff(connect, max_retries=max_retries, initial_delay=retry_delay)
    logger.info(f'Connected to {amp_server}')

    if label_csv and Path(label_csv).exists():
        client.configure_label('tokens', label_csv)
        logger.info(f'Loaded {len(client.label_manager.get_label("tokens"))} labels from {label_csv}')
        label_config = LabelJoinConfig(
            label_name='tokens', label_key_column='token_address', stream_key_column='token_address'
        )
    else:
        label_config = None

    connection_config = {
        'bootstrap_servers': kafka_brokers,
        'client_id': 'amp-kafka-loader',
        'state': {'enabled': True, 'storage': 'lmdb', 'data_dir': state_dir},
    }
    if kafka_config:
        connection_config.update(kafka_config)
    client.configure_connection('kafka', 'kafka', connection_config)

    with open(query_file) as f:
        query = f.read()

    if start_block is not None:
        resume_watermark = create_watermark(client, raw_dataset, network, start_block) if start_block > 0 else None
        logger.info(f'Starting query from block {start_block}')
    else:
        resume_watermark = None
        logger.info('Resuming from LMDB state (or starting from latest if no state)')
    logger.info(f'Streaming to Kafka: {kafka_brokers} -> {topic}')

    batch_count = 0

    def stream_batches():
        nonlocal batch_count
        for result in client.sql(query).load(
            'kafka', topic, stream=True, label_config=label_config, resume_watermark=resume_watermark
        ):
            if result.success:
                batch_count += 1
                if batch_count == 1 and result.metadata:
                    logger.info(f'First batch: {result.metadata.get("block_ranges")}')
                logger.info(f'Batch {batch_count}: {result.rows_loaded} rows in {result.duration:.2f}s')
            else:
                logger.error(f'Batch error: {result.error}')

    retry_with_backoff(stream_batches, max_retries=max_retries, initial_delay=retry_delay)


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
    parser.add_argument('--auth', action='store_true', help='Enable auth using ~/.amp/cache or AMP_AUTH_TOKEN env var')
    parser.add_argument('--auth-token', help='Explicit auth token (works independently, does not require --auth)')
    parser.add_argument('--max-retries', type=int, default=5, help='Max retries for connection failures (default: 5)')
    parser.add_argument('--retry-delay', type=float, default=1.0, help='Initial retry delay in seconds (default: 1.0)')
    parser.add_argument(
        '--kafka-config',
        type=str,
        help='Extra Kafka producer config as JSON. Uses kafka-python naming (underscores). '
        'Example: \'{"compression_type": "lz4", "linger_ms": 5}\'. '
        'See: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html',
    )
    parser.add_argument(
        '--kafka-config-file',
        type=Path,
        help='Path to JSON file with extra Kafka producer config',
    )
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    log_level = getattr(logging, args.log_level) if args.log_level else logging.INFO
    logging.getLogger('amp').setLevel(log_level)

    kafka_config = {}
    if args.kafka_config_file:
        kafka_config = json.loads(args.kafka_config_file.read_text())
        logger.info(f'Loaded Kafka config from {args.kafka_config_file}')
    if args.kafka_config:
        kafka_config.update(json.loads(args.kafka_config))

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
            auth=args.auth,
            auth_token=args.auth_token,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            kafka_config=kafka_config or None,
        )
    except KeyboardInterrupt:
        logger.info('Stopped by user')
    except Exception as e:
        logger.error(f'Fatal error: {e}')
        raise
