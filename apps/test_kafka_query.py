#!/usr/bin/env python3
"""
Test ERC20 query with label joining
"""

import os

from amp.client import Client
from amp.loaders.types import LabelJoinConfig

# Connect to Amp server
server_url = os.getenv('AMP_SERVER_URL', 'grpc://34.27.238.174:80')
print(f'Connecting to {server_url}...')
client = Client(server_url)
print('✅ Connected!')

# Simple ERC20 transfer query
transfer_sig = 'Transfer(address indexed from, address indexed to, uint256 value)'
query = f"""
    SELECT
        block_num,
        tx_hash,
        address as token_address,
        evm_decode(topic1, topic2, topic3, data, '{transfer_sig}')['from'] as from_address,
        evm_decode(topic1, topic2, topic3, data, '{transfer_sig}')['to'] as to_address,
        evm_decode(topic1, topic2, topic3, data, '{transfer_sig}')['value'] as value
    FROM eth_firehose.logs
    WHERE topic0 = evm_topic('{transfer_sig}')
    AND topic3 IS NULL
    LIMIT 10
"""

print('\nRunning query...')
result = client.get_sql(query, read_all=True)

print(f'Got {result.num_rows} rows')
print(f'Columns: {result.schema.names}')

print('Testing label join')


csv_path = 'data/eth_mainnet_token_metadata.csv'
client.configure_label('tokens', csv_path)
print(f'Loaded {len(client.label_manager.get_label("tokens"))} tokens from CSV')

label_config = LabelJoinConfig(
    label_name='tokens',
    label_key_column='token_address',
    stream_key_column='token_address',
)

print('Configured label join: will add symbol, name, decimals columns')


print('Loading to Kafka with labels')


kafka_config = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client_id': 'amp-erc20-loader',
}
client.configure_connection('kafka', 'kafka', kafka_config)

results = list(
    client.sql(query).load(
        connection='kafka',
        destination='erc20_transfers',
        label_config=label_config,
    )
)

total_rows = sum(r.rows_loaded for r in results if r.success)
print(f'\nLoaded {total_rows} enriched rows to Kafka topic "erc20_transfers"')

print('\n' + '=' * 60)
print('Reading back from Kafka')
print('=' * 60)

from kafka import KafkaConsumer
import json
import time

time.sleep(1)

consumer = KafkaConsumer(
    'erc20_transfers',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000,
)

print(f'\nConsuming messages from topic "erc20_transfers":\n')
msg_count = 0
for message in consumer:
    msg_count += 1
    data = message.value
    print(f'Message {msg_count}:')
    print(f'  token_address: {data.get("token_address")}')
    print(f'  symbol: {data.get("symbol")}')
    print(f'  name: {data.get("name")}')
    print(f'  decimals: {data.get("decimals")}')
    print(f'  value: {data.get("value")}')
    print(f'  from_address: {data.get("from_address")}')
    print()

consumer.close()
print(f'Read {msg_count} messages from Kafka')
