import json
from unittest.mock import patch

import pyarrow as pa
import pytest
from kafka import KafkaConsumer
from kafka.errors import KafkaError

try:
    from src.amp.loaders.implementations.kafka_loader import KafkaLoader
    from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaLoaderIntegration:
    def test_loader_connection(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)

        loader.connect()
        assert loader._is_connected == True
        assert loader._producer is not None

        loader.disconnect()
        assert loader._is_connected == False
        assert loader._producer is None

    def test_context_manager(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)

        with loader:
            assert loader._is_connected == True
            assert loader._producer is not None

        assert loader._is_connected == False

    def test_load_batch(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)

        batch = pa.RecordBatch.from_pydict(
            {'id': [1, 2, 3], 'name': ['alice', 'bob', 'charlie'], 'value': [100, 200, 300]}
        )

        with loader:
            result = loader.load_batch(batch, 'test_topic')

        assert result.success == True
        assert result.rows_loaded == 3

    def test_message_consumption_verification(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)
        topic_name = 'test_consumption_topic'

        batch = pa.RecordBatch.from_pydict(
            {
                'id': [1, 2, 3],
                'name': ['alice', 'bob', 'charlie'],
                'score': [100, 200, 150],
                'active': [True, False, True],
            }
        )

        with loader:
            result = loader.load_batch(batch, topic_name)

        assert result.success is True
        assert result.rows_loaded == 3

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        messages = list(consumer)
        consumer.close()

        assert len(messages) == 3

        for i, msg in enumerate(messages):
            assert msg.key == str(i + 1).encode('utf-8')
            assert msg.value['_type'] == 'data'
            assert msg.value['id'] == i + 1
            assert msg.value['name'] in ['alice', 'bob', 'charlie']
            assert msg.value['score'] in [100, 200, 150]
            assert msg.value['active'] in [True, False]

        msg1 = messages[0]
        assert msg1.value['id'] == 1
        assert msg1.value['name'] == 'alice'
        assert msg1.value['score'] == 100
        assert msg1.value['active'] is True

        msg2 = messages[1]
        assert msg2.value['id'] == 2
        assert msg2.value['name'] == 'bob'
        assert msg2.value['score'] == 200
        assert msg2.value['active'] is False

        msg3 = messages[2]
        assert msg3.value['id'] == 3
        assert msg3.value['name'] == 'charlie'
        assert msg3.value['score'] == 150
        assert msg3.value['active'] is True

    def test_handle_reorg(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)
        topic_name = 'test_reorg_topic'

        invalidation_ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc123'),
            BlockRange(network='polygon', start=500, end=600, hash='0xdef456'),
        ]

        with loader:
            loader._handle_reorg(invalidation_ranges, topic_name, 'test_connection')

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        messages = list(consumer)
        consumer.close()

        assert len(messages) == 2

        msg1 = messages[0]
        assert msg1.key == b'reorg:ethereum'
        assert msg1.value['_type'] == 'reorg'
        assert msg1.value['network'] == 'ethereum'
        assert msg1.value['start_block'] == 100
        assert msg1.value['end_block'] == 200
        assert msg1.value['last_valid_hash'] == '0xabc123'

        msg2 = messages[1]
        assert msg2.key == b'reorg:polygon'
        assert msg2.value['_type'] == 'reorg'
        assert msg2.value['network'] == 'polygon'
        assert msg2.value['start_block'] == 500
        assert msg2.value['end_block'] == 600
        assert msg2.value['last_valid_hash'] == '0xdef456'

    def test_handle_reorg_separate_topic(self, kafka_test_config):
        config_with_reorg_topic = {
            **kafka_test_config,
            'reorg_topic': 'test_reorg_events',
        }
        loader = KafkaLoader(config_with_reorg_topic)
        data_topic = 'test_data_topic_separate'

        batch = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})
        invalidation_ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc123'),
        ]

        with loader:
            loader.load_batch(batch, data_topic)
            loader._handle_reorg(invalidation_ranges, data_topic, 'test_connection')

        data_consumer = KafkaConsumer(
            data_topic,
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        data_messages = list(data_consumer)
        data_consumer.close()

        assert len(data_messages) == 2
        assert all(msg.value['_type'] == 'data' for msg in data_messages)

        reorg_consumer = KafkaConsumer(
            'test_reorg_events',
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        reorg_messages = list(reorg_consumer)
        reorg_consumer.close()

        assert len(reorg_messages) == 1
        assert reorg_messages[0].value['_type'] == 'reorg'
        assert reorg_messages[0].value['network'] == 'ethereum'
        assert reorg_messages[0].value['start_block'] == 100
        assert reorg_messages[0].value['end_block'] == 200

    def test_streaming_with_reorg(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)
        topic_name = 'test_streaming_topic'

        data1 = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})
        data2 = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})
        data3 = pa.RecordBatch.from_pydict({'id': [5, 6], 'value': [500, 600]})

        response1 = ResponseBatch.data_batch(
            data=data1,
            metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')]),
        )

        response2 = ResponseBatch.data_batch(
            data=data2,
            metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=110, end=120, hash='0xdef456')]),
        )

        reorg_response = ResponseBatch.reorg_batch(
            invalidation_ranges=[BlockRange(network='ethereum', start=110, end=200, hash='0xdef456')]
        )

        response3 = ResponseBatch.data_batch(
            data=data3,
            metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=110, end=120, hash='0xnew123')]),
        )

        stream = [response1, response2, reorg_response, response3]

        with loader:
            results = list(loader.load_stream_continuous(iter(stream), topic_name))

        assert len(results) == 4
        assert results[0].success
        assert results[0].rows_loaded == 2
        assert results[1].success
        assert results[1].rows_loaded == 2
        assert results[2].success
        assert results[2].is_reorg
        assert results[3].success
        assert results[3].rows_loaded == 2

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        messages = list(consumer)
        consumer.close()

        assert len(messages) == 7

        data_messages = [msg for msg in messages if msg.value.get('_type') == 'data']
        reorg_messages = [msg for msg in messages if msg.value.get('_type') == 'reorg']

        assert len(data_messages) == 6
        assert len(reorg_messages) == 1

        assert reorg_messages[0].key == b'reorg:ethereum'
        assert reorg_messages[0].value['network'] == 'ethereum'
        assert reorg_messages[0].value['start_block'] == 110
        assert reorg_messages[0].value['end_block'] == 200
        assert reorg_messages[0].value['last_valid_hash'] == '0xdef456'

        data_ids = [msg.value['id'] for msg in data_messages]
        assert data_ids == [1, 2, 3, 4, 5, 6]

    def test_transaction_rollback_on_error(self, kafka_test_config):
        loader = KafkaLoader(kafka_test_config)
        topic_name = 'test_rollback_topic'

        batch = pa.RecordBatch.from_pydict(
            {
                'id': [1, 2, 3, 4, 5],
                'name': ['alice', 'bob', 'charlie', 'dave', 'eve'],
                'value': [100, 200, 300, 400, 500],
            }
        )

        with loader:
            call_count = [0]

            original_send = loader._producer.send

            def failing_send(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 3:
                    raise KafkaError('Simulated Kafka send failure')
                return original_send(*args, **kwargs)

            with patch.object(loader._producer, 'send', side_effect=failing_send):
                with pytest.raises(RuntimeError, match='FATAL: Permanent error loading batch'):
                    loader.load_batch(batch, topic_name)

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_test_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        messages = list(consumer)
        consumer.close()

        assert len(messages) == 0
