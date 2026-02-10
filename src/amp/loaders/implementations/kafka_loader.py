import json
from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional

import pyarrow as pa
from kafka import KafkaProducer

from ...streaming.lmdb_state import LMDBStreamStateStore
from ...streaming.types import BlockRange
from ..base import DataLoader, LoadMode


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    client_id: str = 'amp-kafka-loader'
    key_field: Optional[str] = 'id'
    reorg_topic: Optional[str] = None


KAFKA_CONFIG_FIELDS = {f.name for f in fields(KafkaConfig)}
RESERVED_CONFIG_FIELDS = {'resilience', 'state', 'checkpoint', 'idempotency'}


class KafkaLoader(DataLoader[KafkaConfig]):
    SUPPORTED_MODES = {LoadMode.APPEND}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        self._extra_producer_config = {
            k: v for k, v in config.items() if k not in KAFKA_CONFIG_FIELDS and k not in RESERVED_CONFIG_FIELDS
        }
        super().__init__(config, label_manager)
        self._producer = None

    def _get_required_config_fields(self) -> list[str]:
        return ['bootstrap_servers']

    def connect(self) -> None:
        try:
            producer_config = {
                **self._extra_producer_config,
                'bootstrap_servers': self.config.bootstrap_servers,
                'client_id': self.config.client_id,
                'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8'),
                'transactional_id': f'{self.config.client_id}-txn',
            }
            if self._extra_producer_config:
                self.logger.info(f'Extra Kafka config: {list(self._extra_producer_config.keys())}')
            self._producer = KafkaProducer(**producer_config)

            self._producer.init_transactions()

            metadata = self._producer.bootstrap_connected()
            self.logger.info(f'Connection status: {metadata}')
            self.logger.info(f'Connected to Kafka at {self.config.bootstrap_servers}')
            self.logger.info(f'Client ID: {self.config.client_id}')

            if self.state_enabled and self.state_storage == 'lmdb':
                self.state_store = LMDBStreamStateStore(
                    connection_name=self.config.client_id,
                    data_dir=self.state_data_dir,
                )
                self.logger.info(f'Initialized LMDB state store at {self.state_store.data_dir}')

            self._is_connected = True

        except Exception as e:
            if self._producer:
                self._producer.close()
                self._producer = None
            self.logger.error(f'Failed to connect to Kafka: {e}')
            raise

    def disconnect(self) -> None:
        if self._producer:
            self._producer.close()
            self._producer = None

        if isinstance(self.state_store, LMDBStreamStateStore):
            self.state_store.close()

        self._is_connected = False
        self.logger.info('Disconnected from Kafka')

    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        self.logger.info(f'Kafka topic {table_name} will be auto-created on first message send')
        pass

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        if not self._producer:
            raise RuntimeError('Producer not connected. Call connect() first.')

        data_dict = batch.to_pydict()
        num_rows = batch.num_rows

        if num_rows == 0:
            return 0

        self._producer.begin_transaction()
        try:
            for i in range(num_rows):
                row = {field: values[i] for field, values in data_dict.items()}
                row['_type'] = 'data'

                key = self._extract_message_key(row)

                self._producer.send(topic=table_name, key=key, value=row)

            self._producer.commit_transaction()
            self.logger.debug(f'Committed transaction with {num_rows} messages to topic {table_name}')

        except Exception as e:
            self._producer.abort_transaction()
            self.logger.error(f'Transaction aborted due to error: {e}')
            raise

        return num_rows

    def _extract_message_key(self, row: Dict[str, Any]) -> Optional[bytes]:
        if not self.config.key_field or self.config.key_field not in row:
            return None

        key_value = row[self.config.key_field]
        if key_value is None:
            return None

        return str(key_value).encode('utf-8')

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str, connection_name: str) -> None:
        """
        Handle blockchain reorganization by sending reorg events to Kafka.

        Reorg events are sent as special messages with _type='reorg' so consumers
        can detect and handle invalidated block ranges.

        Args:
            invalidation_ranges: List of block ranges to invalidate
            table_name: The Kafka topic name (used if reorg_topic not configured)
            connection_name: Connection identifier (required by base class interface)
        """
        if not invalidation_ranges:
            return

        if not self._producer:
            self.logger.warning('Producer not connected, skipping reorg handling')
            return

        reorg_topic = self.config.reorg_topic or table_name

        self._producer.begin_transaction()
        try:
            for invalidation_range in invalidation_ranges:
                reorg_message = {
                    '_type': 'reorg',
                    'network': invalidation_range.network,
                    'start_block': invalidation_range.start,
                    'end_block': invalidation_range.end,
                    'last_valid_hash': invalidation_range.hash,
                }

                self._producer.send(
                    topic=reorg_topic, key=f'reorg:{invalidation_range.network}'.encode('utf-8'), value=reorg_message
                )

                self.logger.info(
                    f'Sent reorg event to {reorg_topic}: '
                    f'{invalidation_range.network} blocks {invalidation_range.start}-{invalidation_range.end}'
                )

            self._producer.commit_transaction()
            self.logger.info(f'Committed {len(invalidation_ranges)} reorg events to {reorg_topic}')

        except Exception as e:
            self._producer.abort_transaction()
            self.logger.error(f'Reorg transaction aborted due to error: {e}')
            raise
