-- Snowflake Stream State Table
-- Stores server-confirmed completed batches for persistent job resumption
--
-- This table tracks which batches have been successfully processed and confirmed
-- by the server (via checkpoint watermarks). This enables jobs to resume from
-- the correct position after interruption or failure.

CREATE TABLE IF NOT EXISTS amp_stream_state (
    -- Job/Table identification
    connection_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    network VARCHAR(100) NOT NULL,

    -- Batch identification (compact 16-char hex ID)
    batch_id VARCHAR(16) NOT NULL,

    -- Block range covered by this batch
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,

    -- Block hashes for reorg detection (optional)
    end_hash VARCHAR(66),
    start_parent_hash VARCHAR(66),

    -- Processing metadata
    processed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    -- Primary key ensures no duplicate batches
    PRIMARY KEY (connection_name, table_name, network, batch_id)
);

-- Comments for documentation
COMMENT ON TABLE amp_stream_state IS 'Persistent stream state for job resumption - tracks server-confirmed completed batches';
COMMENT ON COLUMN amp_stream_state.batch_id IS 'Compact 16-character hex identifier generated from block range + hash';
COMMENT ON COLUMN amp_stream_state.processed_at IS 'Timestamp when batch was marked as successfully processed';
