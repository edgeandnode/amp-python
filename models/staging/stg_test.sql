{{ config(
    dependencies={'eth': '_/eth_firehose@1.0.0'},
    track_progress=True,
    track_column='block_num'
) }}
SELECT block_num, tx_hash FROM {{ ref('eth') }}.logs LIMIT 10
