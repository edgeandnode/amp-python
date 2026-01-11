"""
Unit tests for LMDB-based stream state store.

Tests the LMDBStreamStateStore implementation that provides durable,
crash-recoverable state tracking using LMDB key-value database.
"""

import tempfile
from pathlib import Path

import pytest

from amp.streaming.lmdb_state import LMDBStreamStateStore
from amp.streaming.state import BatchIdentifier


@pytest.fixture
def temp_lmdb_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def lmdb_store(temp_lmdb_dir):
    store = LMDBStreamStateStore(connection_name='test_conn', data_dir=temp_lmdb_dir, sync=True)
    yield store
    store.close()


class TestLMDBStreamStateStore:
    def test_initialization(self, temp_lmdb_dir):
        store = LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir)

        assert store.connection_name == 'test'
        assert Path(temp_lmdb_dir).exists()

        store.close()

    def test_mark_and_check_processed(self, lmdb_store):
        batch_id = BatchIdentifier('ethereum', 100, 200, '0xabc')

        assert lmdb_store.is_processed('conn1', 'table1', [batch_id]) is False

        lmdb_store.mark_processed('conn1', 'table1', [batch_id])

        assert lmdb_store.is_processed('conn1', 'table1', [batch_id]) is True

    def test_is_processed_empty_list(self, lmdb_store):
        result = lmdb_store.is_processed('conn1', 'table1', [])
        assert result is True

    def test_multiple_batches_all_must_be_processed(self, lmdb_store):
        batch_id1 = BatchIdentifier('ethereum', 100, 200, '0xabc')
        batch_id2 = BatchIdentifier('ethereum', 200, 300, '0xdef')
        batch_id3 = BatchIdentifier('ethereum', 300, 400, '0x123')
        batch_id4 = BatchIdentifier('ethereum', 400, 500, '0x456')
        batch_id5 = BatchIdentifier('ethereum', 500, 600, '0x789')

        lmdb_store.mark_processed('conn1', 'table1', [batch_id1, batch_id3, batch_id5])

        assert lmdb_store.is_processed('conn1', 'table1', [batch_id1]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [batch_id1, batch_id2]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch_id1, batch_id3, batch_id5]) is True

        lmdb_store.mark_processed('conn1', 'table1', [batch_id2, batch_id4])

        assert (
            lmdb_store.is_processed('conn1', 'table1', [batch_id1, batch_id2, batch_id3, batch_id4, batch_id5]) is True
        )

    def test_separate_networks(self, lmdb_store):
        eth_batch = BatchIdentifier('ethereum', 100, 200, '0xabc')
        poly_batch = BatchIdentifier('polygon', 100, 200, '0xdef')

        lmdb_store.mark_processed('conn1', 'table1', [eth_batch])

        assert lmdb_store.is_processed('conn1', 'table1', [eth_batch]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [poly_batch]) is False

    def test_separate_connections_and_tables(self, lmdb_store):
        batch_id = BatchIdentifier('ethereum', 100, 200, '0xabc')

        lmdb_store.mark_processed('conn1', 'table1', [batch_id])

        assert lmdb_store.is_processed('conn2', 'table1', [batch_id]) is False
        assert lmdb_store.is_processed('conn1', 'table2', [batch_id]) is False

    def test_get_resume_position_empty(self, lmdb_store):
        watermark = lmdb_store.get_resume_position('conn1', 'table1')

        assert watermark is None

    def test_get_resume_position_single_network(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc', '0xparent1')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef', '0xparent2')
        batch3 = BatchIdentifier('ethereum', 300, 400, '0x123', '0xparent3')

        lmdb_store.mark_processed('conn1', 'table1', [batch1])
        lmdb_store.mark_processed('conn1', 'table1', [batch2])
        lmdb_store.mark_processed('conn1', 'table1', [batch3])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')

        assert watermark is not None
        assert len(watermark.ranges) == 1
        assert watermark.ranges[0].network == 'ethereum'
        assert watermark.ranges[0].end == 400
        assert watermark.ranges[0].hash == '0x123'
        assert watermark.ranges[0].prev_hash == '0xparent3'

    def test_get_resume_position_multiple_networks(self, lmdb_store):
        eth_batch = BatchIdentifier('ethereum', 100, 200, '0xabc', '0xeth_parent')
        poly_batch = BatchIdentifier('polygon', 500, 600, '0xdef', '0xpoly_parent')
        arb_batch = BatchIdentifier('arbitrum', 1000, 1100, '0x123', '0xarb_parent')

        lmdb_store.mark_processed('conn1', 'table1', [eth_batch])
        lmdb_store.mark_processed('conn1', 'table1', [poly_batch])
        lmdb_store.mark_processed('conn1', 'table1', [arb_batch])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')

        assert watermark is not None
        assert len(watermark.ranges) == 3

        networks = {r.network: r.end for r in watermark.ranges}
        assert networks['ethereum'] == 200
        assert networks['polygon'] == 600
        assert networks['arbitrum'] == 1100

    def test_metadata_updates_with_max_block(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc')
        batch2 = BatchIdentifier('ethereum', 300, 400, '0xdef')
        batch3 = BatchIdentifier('ethereum', 200, 250, '0x123')
        batch4 = BatchIdentifier('ethereum', 50, 75, '0x456')
        batch5 = BatchIdentifier('ethereum', 350, 380, '0x789')

        lmdb_store.mark_processed('conn1', 'table1', [batch1])
        lmdb_store.mark_processed('conn1', 'table1', [batch2])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark.ranges[0].end == 400

        lmdb_store.mark_processed('conn1', 'table1', [batch3, batch4, batch5])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark.ranges[0].end == 400

    def test_invalidate_from_block(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef')
        batch3 = BatchIdentifier('ethereum', 300, 400, '0x123')
        batch4 = BatchIdentifier('ethereum', 400, 500, '0x456')
        batch5 = BatchIdentifier('ethereum', 500, 600, '0x789')
        batch6 = BatchIdentifier('ethereum', 50, 100, '0xaaa')

        lmdb_store.mark_processed('conn1', 'table1', [batch1, batch2, batch3, batch4, batch5, batch6])

        invalidated = lmdb_store.invalidate_from_block('conn1', 'table1', 'ethereum', 250)

        assert len(invalidated) == 4
        invalidated_ids = {b.unique_id for b in invalidated}
        assert batch2.unique_id in invalidated_ids
        assert batch3.unique_id in invalidated_ids
        assert batch4.unique_id in invalidated_ids
        assert batch5.unique_id in invalidated_ids

        assert lmdb_store.is_processed('conn1', 'table1', [batch1]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [batch6]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [batch2]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch3]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch4]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch5]) is False

    def test_invalidate_updates_metadata(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc', '0xparent1')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef', '0xparent2')
        batch3 = BatchIdentifier('ethereum', 300, 400, '0x123', '0xparent3')

        lmdb_store.mark_processed('conn1', 'table1', [batch1, batch2, batch3])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark.ranges[0].end == 400

        lmdb_store.invalidate_from_block('conn1', 'table1', 'ethereum', 250)

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark.ranges[0].end == 200
        assert watermark.ranges[0].hash == '0xabc'

    def test_invalidate_all_batches_clears_metadata(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc')

        lmdb_store.mark_processed('conn1', 'table1', [batch1])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark is not None

        lmdb_store.invalidate_from_block('conn1', 'table1', 'ethereum', 50)

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark is None

    def test_invalidate_only_affects_specified_network(self, lmdb_store):
        eth_batch = BatchIdentifier('ethereum', 100, 200, '0xabc')
        poly_batch = BatchIdentifier('polygon', 100, 200, '0xdef')

        lmdb_store.mark_processed('conn1', 'table1', [eth_batch, poly_batch])

        invalidated = lmdb_store.invalidate_from_block('conn1', 'table1', 'ethereum', 150)

        assert len(invalidated) == 1
        assert invalidated[0].network == 'ethereum'

        assert lmdb_store.is_processed('conn1', 'table1', [poly_batch]) is True

    def test_cleanup_before_block(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 50, 100, '0xold1')
        batch2 = BatchIdentifier('ethereum', 100, 200, '0xold2')
        batch3 = BatchIdentifier('ethereum', 150, 220, '0xold3')
        batch4 = BatchIdentifier('ethereum', 200, 300, '0xkeep1')
        batch5 = BatchIdentifier('ethereum', 300, 400, '0xkeep2')
        batch6 = BatchIdentifier('ethereum', 400, 500, '0xkeep3')

        lmdb_store.mark_processed('conn1', 'table1', [batch1, batch2, batch3, batch4, batch5, batch6])

        lmdb_store.cleanup_before_block('conn1', 'table1', 'ethereum', 250)

        assert lmdb_store.is_processed('conn1', 'table1', [batch1]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch2]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch3]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [batch4]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [batch5]) is True
        assert lmdb_store.is_processed('conn1', 'table1', [batch6]) is True

    def test_cleanup_only_affects_specified_network(self, lmdb_store):
        eth_batch = BatchIdentifier('ethereum', 100, 200, '0xabc')
        poly_batch = BatchIdentifier('polygon', 100, 200, '0xdef')

        lmdb_store.mark_processed('conn1', 'table1', [eth_batch, poly_batch])

        lmdb_store.cleanup_before_block('conn1', 'table1', 'ethereum', 250)

        assert lmdb_store.is_processed('conn1', 'table1', [eth_batch]) is False
        assert lmdb_store.is_processed('conn1', 'table1', [poly_batch]) is True

    def test_context_manager(self, temp_lmdb_dir):
        batch_id = BatchIdentifier('ethereum', 100, 200, '0xabc')

        with LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir) as store:
            store.mark_processed('conn1', 'table1', [batch_id])
            assert store.is_processed('conn1', 'table1', [batch_id]) is True

    def test_persistence_across_close_reopen(self, temp_lmdb_dir):
        batch_id = BatchIdentifier('ethereum', 100, 200, '0xabc')

        store1 = LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir)
        store1.mark_processed('conn1', 'table1', [batch_id])
        store1.close()

        store2 = LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir)
        assert store2.is_processed('conn1', 'table1', [batch_id]) is True
        store2.close()

    def test_detect_gaps_raises_not_implemented(self, lmdb_store):
        with pytest.raises(NotImplementedError, match='Gap detection not implemented'):
            lmdb_store.get_resume_position('conn1', 'table1', detect_gaps=True)

    def test_resume_position_with_many_out_of_order_batches(self, lmdb_store):
        batches = [
            BatchIdentifier('ethereum', 100, 150, '0x1', '0xp1'),
            BatchIdentifier('ethereum', 500, 600, '0x2', '0xp2'),
            BatchIdentifier('ethereum', 200, 300, '0x3', '0xp3'),
            BatchIdentifier('ethereum', 50, 100, '0x4', '0xp4'),
            BatchIdentifier('ethereum', 300, 400, '0x5', '0xp5'),
            BatchIdentifier('ethereum', 150, 200, '0x6', '0xp6'),
            BatchIdentifier('ethereum', 400, 500, '0x7', '0xp7'),
            BatchIdentifier('ethereum', 600, 700, '0x8', '0xp8'),
            BatchIdentifier('ethereum', 700, 800, '0x9', '0xp9'),
            BatchIdentifier('ethereum', 250, 280, '0xa', '0xpa'),
        ]

        for batch in batches:
            lmdb_store.mark_processed('conn1', 'table1', [batch])

        watermark = lmdb_store.get_resume_position('conn1', 'table1')
        assert watermark.ranges[0].end == 800
        assert watermark.ranges[0].hash == '0x9'
        assert watermark.ranges[0].prev_hash == '0xp9'


class TestIntegrationScenarios:
    def test_streaming_with_resume(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef')

        lmdb_store.mark_processed('conn1', 'transfers', [batch1])
        lmdb_store.mark_processed('conn1', 'transfers', [batch2])

        watermark = lmdb_store.get_resume_position('conn1', 'transfers')
        assert watermark.ranges[0].end == 300

        batch3 = BatchIdentifier('ethereum', 300, 400, '0x123')
        batch4 = BatchIdentifier('ethereum', 400, 500, '0x456')

        assert lmdb_store.is_processed('conn1', 'transfers', [batch2]) is True

        lmdb_store.mark_processed('conn1', 'transfers', [batch3])
        lmdb_store.mark_processed('conn1', 'transfers', [batch4])

        watermark = lmdb_store.get_resume_position('conn1', 'transfers')
        assert watermark.ranges[0].end == 500

    def test_reorg_scenario(self, lmdb_store):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef')
        batch3 = BatchIdentifier('ethereum', 300, 400, '0x123')

        lmdb_store.mark_processed('conn1', 'blocks', [batch1, batch2, batch3])

        invalidated = lmdb_store.invalidate_from_block('conn1', 'blocks', 'ethereum', 250)

        assert len(invalidated) == 2

        watermark = lmdb_store.get_resume_position('conn1', 'blocks')
        assert watermark.ranges[0].end == 200

        batch2_new = BatchIdentifier('ethereum', 200, 300, '0xNEWHASH1')
        batch3_new = BatchIdentifier('ethereum', 300, 400, '0xNEWHASH2')

        lmdb_store.mark_processed('conn1', 'blocks', [batch2_new, batch3_new])

        assert lmdb_store.is_processed('conn1', 'blocks', [batch2_new]) is True
        assert lmdb_store.is_processed('conn1', 'blocks', [batch2]) is False

    def test_multi_network_streaming(self, lmdb_store):
        eth_batch1 = BatchIdentifier('ethereum', 100, 200, '0xeth1')
        eth_batch2 = BatchIdentifier('ethereum', 200, 300, '0xeth2')
        poly_batch1 = BatchIdentifier('polygon', 500, 600, '0xpoly1')
        arb_batch1 = BatchIdentifier('arbitrum', 1000, 1100, '0xarb1')

        lmdb_store.mark_processed('conn1', 'transfers', [eth_batch1, eth_batch2])
        lmdb_store.mark_processed('conn1', 'transfers', [poly_batch1])
        lmdb_store.mark_processed('conn1', 'transfers', [arb_batch1])

        watermark = lmdb_store.get_resume_position('conn1', 'transfers')

        assert len(watermark.ranges) == 3
        networks = {r.network: r.end for r in watermark.ranges}
        assert networks['ethereum'] == 300
        assert networks['polygon'] == 600
        assert networks['arbitrum'] == 1100

        invalidated = lmdb_store.invalidate_from_block('conn1', 'transfers', 'ethereum', 250)
        assert len(invalidated) == 1

        assert lmdb_store.is_processed('conn1', 'transfers', [poly_batch1]) is True
        assert lmdb_store.is_processed('conn1', 'transfers', [arb_batch1]) is True

    def test_crash_recovery_with_persistence(self, temp_lmdb_dir):
        batch1 = BatchIdentifier('ethereum', 100, 200, '0xabc', '0xparent1')
        batch2 = BatchIdentifier('ethereum', 200, 300, '0xdef', '0xparent2')

        store1 = LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir)
        store1.mark_processed('conn1', 'transfers', [batch1, batch2])
        watermark1 = store1.get_resume_position('conn1', 'transfers')
        store1.close()

        store2 = LMDBStreamStateStore(connection_name='test', data_dir=temp_lmdb_dir)
        watermark2 = store2.get_resume_position('conn1', 'transfers')

        assert watermark2 is not None
        assert watermark2.ranges[0].end == watermark1.ranges[0].end
        assert watermark2.ranges[0].hash == '0xdef'

        assert store2.is_processed('conn1', 'transfers', [batch1]) is True
        assert store2.is_processed('conn1', 'transfers', [batch2]) is True
        store2.close()
