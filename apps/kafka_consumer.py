#!/usr/bin/env python3
"""Simple Kafka consumer script to print messages from a topic in real-time.

Messages are consumed from a consumer group, so subsequent runs will only show
new messages. Press Ctrl+C to exit cleanly.

Usage:
    python kafka_consumer.py [topic] [broker] [group_id]

Examples:
    python kafka_consumer.py
    python kafka_consumer.py anvil_logs
    python kafka_consumer.py anvil_logs localhost:9092
    python kafka_consumer.py anvil_logs localhost:9092 my-group
"""

import json
import sys
from datetime import datetime

from kafka import KafkaConsumer

topic = sys.argv[1] if len(sys.argv) > 1 else 'anvil_logs'
broker = sys.argv[2] if len(sys.argv) > 2 else 'localhost:9092'
group_id = sys.argv[3] if len(sys.argv) > 3 else 'kafka-consumer-cli'

print(f'Consuming from: {broker} -> topic: {topic}')
print(f'Consumer group: {group_id}')
print(f'Started at: {datetime.now().strftime("%H:%M:%S")}')
print('-' * 80)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    group_id=group_id,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

msg_count = 0
data_count = 0
reorg_count = 0

try:
    for msg in consumer:
        msg_count += 1
        msg_type = msg.value.get('_type', 'unknown')

        if msg_type == 'data':
            data_count += 1
            print(f'\nMessage #{msg_count} [DATA] - Key: {msg.key.decode() if msg.key else "None"}')
            print(f'Offset: {msg.offset} | Partition: {msg.partition}')

            for k, v in msg.value.items():
                if k != '_type':
                    print(f'{k}: {v}')

        elif msg_type == 'reorg':
            reorg_count += 1
            print(f'\nMessage #{msg_count} [REORG] - Key: {msg.key.decode() if msg.key else "None"}')
            print(f'Network: {msg.value.get("network")}')
            print(f'Blocks: {msg.value.get("start_block")} -> {msg.value.get("end_block")}')

        else:
            print(f'\nMessage #{msg_count} [UNKNOWN]')
            print(json.dumps(msg.value, indent=2))

        print(f'\nTotal: {msg_count} msgs | Data: {data_count} | Reorgs: {reorg_count}')
        print('-' * 80)

except KeyboardInterrupt:
    print('\n\nStopped')
finally:
    consumer.close()
