-- ERC20 Transfer Events Query for Streaming (from specific block)
--
-- This query decodes ERC20 Transfer events from raw Ethereum logs.
-- Filters to only include blocks >= {start_block} for streaming from recent data.
--
-- Required columns for parallel loading:
--   - block_num: Used for partitioning across workers
--
-- Label join column (if using --label-csv):
--   - token_address: Binary address of the ERC20 token contract
--
-- Example usage:
--   python apps/kafka_streaming_loader.py \
--     --query-file apps/queries/erc20_transfers_streaming.sql \
--     --start-block 21000000

select
    pc.block_num,
    pc.block_hash,
    pc.timestamp,
    pc.tx_hash,
    pc.tx_index,
    pc.log_index,
    pc.address as token_address,
    pc.dec['from'] as from_address,
    pc.dec['to'] as to_address,
    pc.dec['value'] as value
from (
    select
        l.block_num,
        l.block_hash,
        l.tx_hash,
        l.tx_index,
        l.log_index,
        l.timestamp,
        l.address,
        evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)') as dec
    from eth_firehose.logs l
    where
        l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)') and
        l.topic3 IS NULL and
        l.block_num >= {start_block}
) pc
