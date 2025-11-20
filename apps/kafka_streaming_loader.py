#!/usr/bin/env python3
"""
Kafka streaming loader with label joining.
Continuously loads ERC20 transfers to Kafka with token metadata.
"""

import argparse
import json
import os
import time
from pathlib import Path

from amp.client import Client
from amp.loaders.types import LabelJoinConfig
from kafka import KafkaConsumer


def consume_messages(kafka_brokers: str, topic: str, max_messages: int = 10):
    """Consume and print messages from Kafka topic for testing."""
    print(f'\n{"=" * 60}')
    print('Consuming messages from Kafka')
    print(f'{"=" * 60}\n')
    print(f'Topic: {topic}')
    print(f'Brokers: {kafka_brokers}')
    print(f'Max messages: {max_messages}\n')

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000,
        group_id='kafka-streaming-loader-consumer',
        enable_auto_commit=True,
    )

    def format_address(addr):
        """Convert binary address to hex string."""
        if addr is None:
            return None
        if isinstance(addr, str):
            return addr
        if isinstance(addr, bytes):
            return '0x' + addr.hex()
        return addr

    msg_count = 0
    for message in consumer:
        msg_count += 1
        data = message.value
        print(f'Message {msg_count}:')
        print(f'  block_num: {data.get("block_num")}')
        print(f'  token_address: {format_address(data.get("token_address"))}')
        print(f'  symbol: {data.get("symbol")}')
        print(f'  name: {data.get("name")}')
        print(f'  decimals: {data.get("decimals")}')
        print(f'  value: {data.get("value")}')
        print(f'  from_address: {format_address(data.get("from_address"))}')
        print(f'  to_address: {format_address(data.get("to_address"))}')
        print()

        if msg_count >= max_messages:
            break

    consumer.close()
    print(f'Consumed {msg_count} messages from Kafka topic "{topic}"')


def main(
    kafka_brokers: str,
    topic: str,
    label_csv: str,
    amp_server: str,
    query_file: str,
    consume_mode: bool = False,
    consume_max: int = 10,
):
    if consume_mode:
        consume_messages(kafka_brokers, topic, consume_max)
        return

    print(f'Connecting to Amp server: {amp_server}')
    client = Client(amp_server)

    label_path = Path(label_csv)
    if not label_path.exists():
        raise FileNotFoundError(f'Label CSV not found: {label_csv}')

    client.configure_label('tokens', str(label_path))
    print(f'Loaded {len(client.label_manager.get_label("tokens"))} tokens from {label_csv}')

    kafka_config = {
        'bootstrap_servers': kafka_brokers,
        'client_id': 'amp-kafka-loader',
    }
    client.configure_connection('kafka', 'kafka', kafka_config)

    query_path = Path(query_file)
    if not query_path.exists():
        raise FileNotFoundError(f'Query file not found: {query_file}')

    with open(query_path) as f:
        query = f.read()

    label_config = LabelJoinConfig(
        label_name='tokens',
        label_key_column='token_address',
        stream_key_column='token_address',
    )

    print(f'Starting Kafka streaming loader')
    print(f'Kafka brokers: {kafka_brokers}')
    print(f'Topic: {topic}')
    print('Press Ctrl+C to stop\n')

    total_rows = 0
    batch_count = 0

    for result in client.sql(query).load(
        connection='kafka',
        destination=topic,
        stream=True,
        label_config=label_config,
    ):
        if result.success:
            total_rows += result.rows_loaded
            batch_count += 1
            print(f'Batch {batch_count}: {result.rows_loaded} rows in {result.duration:.2f}s (total: {total_rows})')
        else:
            print(f'Error: {result.error}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stream ERC20 transfers to Kafka with token labels')
    parser.add_argument(
        '--kafka-brokers',
        default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        help='Kafka bootstrap servers (default: localhost:9092 or KAFKA_BOOTSTRAP_SERVERS env var)',
    )
    parser.add_argument('--topic', default='erc20_transfers', help='Kafka topic name (default: erc20_transfers)')
    parser.add_argument(
        '--label-csv',
        default='data/eth_mainnet_token_metadata.csv',
        help='Path to token metadata CSV (default: data/eth_mainnet_token_metadata.csv)',
    )
    parser.add_argument(
        '--amp-server',
        default=os.getenv('AMP_SERVER_URL', 'grpc://34.27.238.174:80'),
        help='Amp server URL (default: grpc://34.27.238.174:80 or AMP_SERVER_URL env var)',
    )
    parser.add_argument(
        '--query-file',
        default='apps/queries/erc20_transfers.sql',
        help='Path to SQL query file (default: apps/queries/erc20_transfers.sql)',
    )
    parser.add_argument(
        '--consume',
        action='store_true',
        help='Consume mode: read and print messages from Kafka topic (for testing)',
    )
    parser.add_argument(
        '--consume-max',
        type=int,
        default=10,
        help='Maximum messages to consume in consume mode (default: 10)',
    )

    args = parser.parse_args()

    try:
        main(
            kafka_brokers=args.kafka_brokers,
            topic=args.topic,
            label_csv=args.label_csv,
            amp_server=args.amp_server,
            query_file=args.query_file,
            consume_mode=args.consume,
            consume_max=args.consume_max,
        )
    except KeyboardInterrupt:
        print('\n\nInterrupted by user')
    except Exception as e:
        print(f'\n\nError: {e}')
        import traceback

        traceback.print_exc()
        raise
