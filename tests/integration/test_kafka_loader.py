import pyarrow as pa
import pytest

try:
    from src.amp.loaders.implementations.kafka_loader import KafkaLoader
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
