"""E2E streaming, continuous ingestion, and reorg detection tests."""

import time

import pytest

from .helpers.process_manager import (
    evm_revert,
    evm_snapshot,
    mine_blocks,
    wait_for_block,
)

pytestmark = pytest.mark.e2e


def test_continuous_ingestion(continuous_server):
    """Verify ampd ingests new blocks in continuous mode."""
    anvil_url = continuous_server.anvil_url
    flight_port = continuous_server.ports['flight']
    client = continuous_server.client

    table = client.sql('SELECT COUNT(*) AS cnt FROM anvil.blocks').to_arrow()
    count_before = table.column('cnt').to_pylist()[0]
    assert count_before >= 11

    mine_blocks(anvil_url, 5)
    wait_for_block(flight_port, count_before + 4)

    table = client.sql('SELECT COUNT(*) AS cnt FROM anvil.blocks').to_arrow()
    count_after = table.column('cnt').to_pylist()[0]
    assert count_after >= count_before + 5


def test_streaming_metadata_parsing(continuous_server):
    """Verify real server app_metadata parses into BatchMetadata with hashes."""
    from google.protobuf.any_pb2 import Any
    from pyarrow import flight

    from amp import FlightSql_pb2
    from amp.streaming.types import BatchMetadata

    query = 'SELECT block_num, hash FROM anvil.blocks SETTINGS stream = true'

    command_query = FlightSql_pb2.CommandStatementQuery()
    command_query.query = query
    any_command = Any()
    any_command.Pack(command_query)
    cmd = any_command.SerializeToString()

    client = continuous_server.client
    flight_descriptor = flight.FlightDescriptor.for_command(cmd)
    info = client.conn.get_flight_info(flight_descriptor)
    reader = client.conn.do_get(info.endpoints[0].ticket)

    # Read chunks until we get one with data rows (skip watermark/empty batches)
    metadata = None
    for _ in range(20):
        chunk = reader.read_chunk()
        if chunk.app_metadata is not None:
            md = BatchMetadata.from_flight_data(chunk.app_metadata)
            if md.ranges:
                metadata = md
                break

    reader.cancel()

    assert metadata is not None, 'Should receive at least one batch with block ranges'
    for r in metadata.ranges:
        assert r.network is not None
        assert r.start >= 0
        assert r.end >= r.start
        assert r.hash is not None, 'Server should send block hashes'


def test_reorg_detection(reorg_server):
    """Verify ampd detects a reorg when new blocks break the hash chain."""
    anvil_url = reorg_server.anvil_url
    flight_port = reorg_server.ports['flight']
    client = reorg_server.client

    # Find current chain tip
    tip = client.sql('SELECT MAX(block_num) AS tip FROM anvil.blocks').to_arrow()
    tip_block = tip.column('tip').to_pylist()[0]

    snapshot_id = evm_snapshot(anvil_url)

    # Mine 5 blocks past the tip, wait for ingestion
    mine_blocks(anvil_url, 5)
    first_new = tip_block + 1
    last_new = tip_block + 5
    wait_for_block(flight_port, last_new)

    # Capture pre-reorg hashes
    pre_reorg = client.sql(
        f'SELECT block_num, hash FROM anvil.blocks WHERE block_num >= {first_new} ORDER BY block_num'
    ).to_arrow()
    assert len(pre_reorg) == 5
    pre_hashes = pre_reorg.column('hash').to_pylist()

    # Revert to snapshot and mine past the old tip so the worker
    # writes new blocks whose prev_hash won't match the stored hash,
    # triggering fork detection and re-materialization.
    evm_revert(anvil_url, snapshot_id)
    mine_blocks(anvil_url, 10)

    # Wait for ampd to detect reorg and re-ingest
    timeout = 30
    post_hashes = None
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        post_reorg = client.sql(
            f'SELECT block_num, hash FROM anvil.blocks '
            f'WHERE block_num >= {first_new} AND block_num <= {last_new} ORDER BY block_num'
        ).to_arrow()
        if len(post_reorg) == 5:
            post_hashes = post_reorg.column('hash').to_pylist()
            if post_hashes != pre_hashes:
                break
        time.sleep(1)
    else:
        pytest.fail(f'ampd did not detect reorg within {timeout}s')

    assert pre_reorg.column('block_num').to_pylist() == post_reorg.column('block_num').to_pylist()
    assert post_hashes != pre_hashes
