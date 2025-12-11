-- Example model
{{ config(
    dependencies={'eth': '_/eth_firehose@1.0.0'},
    description='Example model showing how to use ref()'
) }}

SELECT
    block_num,
    block_hash,
    timestamp
FROM {{ ref('eth') }}.blocks
LIMIT 10
