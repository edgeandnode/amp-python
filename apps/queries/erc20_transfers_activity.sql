-- ERC20 Transfer Events Query (Activity Schema)
--
-- This query decodes ERC20 Transfer events from raw Ethereum logs
-- and formats them according to the activity schema for Kafka streaming.
--
-- Output Schema:
--   - version: Schema version (1.0)
--   - chain: Chain identifier (e.g., 1 for Ethereum mainnet)
--   - block_num: Block number
--   - block_hash: Block hash (0x prefixed hex)
--   - transaction: Transaction hash (0x prefixed hex)
--   - activity_identity: Activity identity type (e.g., 'wallet')
--   - activity_type: Activity type (e.g., 'payment')
--   - activity_address: Primary address involved in activity
--   - activity_operations: Array of operations with debited/credited amounts
--   - token_address: ERC20 token contract address (0x prefixed hex)
--   - token_symbol: Token symbol (from label join)
--   - token_name: Token name (from label join)
--   - token_decimals: Token decimals (from label join)
--
-- Required columns for parallel loading:
--   - block_num: Used for partitioning across workers
--
-- Label join column (if using --label-csv):
--   - token_address: Binary address of the ERC20 token contract
--
-- Example usage:
--   uv apps/kafka_streaming_loader.py \
--     --query-file apps/queries/erc20_transfers_activity.sql \
--     --topic erc20_transfers \
--     --label-csv data/eth_mainnet_token_metadata.csv \
--     --raw-dataset eth_firehose \
--     --network ethereum

select
    1.0 as version,
    1 as chain,
    l.block_num,
    l.block_hash,
    l.tx_hash as transaction,
    'wallet' as activity_identity,
    'payment' as activity_type,
    evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')['from'] as activity_address,
    [
        struct(
            concat('log:', cast(l.block_num as string), ':', cast(l.log_index as string)) as id,
            l.address as token,
            evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')['from'] as address,
            -cast(evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')['value'] as double) as amount,
            'debited' as type
        ),
        struct(
            '' as id,
            l.address as token,
            evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')['to'] as address,
            cast(evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')['value'] as double) as amount,
            'credited' as type
        )
    ] as activity_operations,
    l.address as token_address,
    cast(null as string) as token_symbol,
    cast(null as string) as token_name,
    cast(null as int) as token_decimals
from eth_firehose.logs l
where
    l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)') and
    l.topic3 IS NULL
