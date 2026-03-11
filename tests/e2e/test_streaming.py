"""E2E streaming and continuous ingestion tests against real ampd + Anvil."""

import pytest

from .helpers.process_manager import mine_blocks, wait_for_block


@pytest.fixture()
def continuous_server():
    """Isolated ampd + Anvil stack with continuous deploy."""
    from .conftest import _setup_amp_stack, _skip_if_missing_deps

    _skip_if_missing_deps()
    server, cleanup = _setup_amp_stack(num_blocks=10, end_block=None)
    yield server
    cleanup()


@pytest.mark.e2e
def test_continuous_ingestion(continuous_server):
    """Verify ampd ingests new blocks in continuous mode."""
    anvil_url = continuous_server.anvil_url
    flight_port = continuous_server.ports['flight']
    client = continuous_server.client

    # Verify initial 11 blocks (0-10)
    table = client.sql('SELECT COUNT(*) AS cnt FROM anvil.blocks').to_arrow()
    assert table.column('cnt').to_pylist()[0] == 11

    # Mine 5 more blocks
    mine_blocks(anvil_url, 5)
    wait_for_block(flight_port, 15)

    # Verify all 16 blocks present
    table = client.sql('SELECT COUNT(*) AS cnt FROM anvil.blocks').to_arrow()
    assert table.column('cnt').to_pylist()[0] == 16


@pytest.mark.e2e
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
