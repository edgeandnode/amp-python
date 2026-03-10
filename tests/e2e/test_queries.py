"""E2E query validation tests against real ampd + Anvil."""

import pyarrow as pa
import pytest


@pytest.mark.e2e
class TestBlocksQuery:
    def test_query_blocks(self, e2e_client):
        table = e2e_client.sql('SELECT block_num, hash FROM anvil.blocks ORDER BY block_num').to_arrow()

        assert len(table) == 11
        block_nums = table.column('block_num').to_pylist()
        assert block_nums == list(range(11))
        assert all(h is not None for h in table.column('hash').to_pylist())

    def test_blocks_schema(self, e2e_client):
        table = e2e_client.sql('SELECT * FROM anvil.blocks LIMIT 1').to_arrow()
        schema = table.schema

        assert schema.field('block_num').type == pa.uint64()
        assert schema.field('hash').type == pa.binary(32)
        assert schema.field('parent_hash').type == pa.binary(32)
        assert schema.field('miner').type == pa.binary(20)
        assert schema.field('gas_used').type == pa.uint64()
        assert hasattr(schema.field('timestamp').type, 'tz')


@pytest.mark.e2e
class TestTransactionsQuery:
    def test_query_transactions(self, e2e_client):
        table = e2e_client.sql('SELECT * FROM anvil.transactions LIMIT 5').to_arrow()
        expected_cols = {'block_num', 'tx_hash', 'from', 'to', 'value', 'gas_used'}
        assert expected_cols.issubset(set(table.column_names))


@pytest.mark.e2e
class TestLogsQuery:
    def test_query_logs(self, e2e_client):
        table = e2e_client.sql('SELECT * FROM anvil.logs LIMIT 5').to_arrow()
        expected_cols = {'block_num', 'log_index', 'address', 'data'}
        assert expected_cols.issubset(set(table.column_names))


@pytest.mark.e2e
class TestQueryBehavior:
    def test_query_with_where_clause(self, e2e_client):
        table = e2e_client.sql('SELECT block_num FROM anvil.blocks WHERE block_num > 5 ORDER BY block_num').to_arrow()

        assert len(table) == 5
        assert table.column('block_num').to_pylist() == [6, 7, 8, 9, 10]

    def test_query_with_aggregation(self, e2e_client):
        table = e2e_client.sql('SELECT COUNT(*) AS cnt FROM anvil.blocks').to_arrow()
        assert table.column('cnt').to_pylist()[0] == 11
