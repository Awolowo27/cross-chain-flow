{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH across_deposits AS (
    {{ v2_across_v3_deposits('ethereum', ref('ethereum_bridge_events'), '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5') }}
    UNION ALL
    {{ v2_across_v3_deposits('arbitrum', ref('arbitrum_bridge_events'), '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a') }}
    UNION ALL
    {{ v2_across_v3_deposits('optimism', ref('optimism_bridge_events'), '0x6f26bf09b1c080fd11432a64117692e591789c62') }}
    UNION ALL
    {{ v2_across_v3_deposits('avalanche', ref('avalanche_bridge_events'), '0xfe9d541c92e4e90437c7152a00244886de37a658') }}
),
across_fills AS (
    {{ v2_across_v3_fills('ethereum', ref('ethereum_bridge_events'), '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5') }}
    UNION ALL
    {{ v2_across_v3_fills('arbitrum', ref('arbitrum_bridge_events'), '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a') }}
    UNION ALL
    {{ v2_across_v3_fills('optimism', ref('optimism_bridge_events'), '0x6f26bf09b1c080fd11432a64117692e591789c62') }}
    UNION ALL
    {{ v2_across_v3_fills('avalanche', ref('avalanche_bridge_events'), '0xfe9d541c92e4e90437c7152a00244886de37a658') }}
)

SELECT
    'Across V3' AS bridge_name,
    d.deposit_id AS correlation_id,
    d.deposit_chain AS source_chain,
    COALESCE(
        f.fill_chain,
        CASE d.destination_chain_id
            WHEN 1 THEN 'ethereum'
            WHEN 42161 THEN 'arbitrum'
            WHEN 10 THEN 'optimism'
            WHEN 43114 THEN 'avalanche'
            WHEN 137 THEN 'polygon'
            ELSE 'unknown'
        END
    ) AS destination_chain,
    d.block_time AS deposit_timestamp,
    f.block_time AS fill_timestamp,
    TIMESTAMP_DIFF(f.block_time, d.block_time, SECOND) AS time_to_fill_seconds,
    CASE 
      WHEN f.tx_hash IS NOT NULL THEN 'COMPLETED'
      ELSE 'PENDING'
    END AS status,
    d.depositor AS user_address,
    d.tx_hash AS deposit_tx_hash,
    f.tx_hash AS destination_tx_hash,
    d.input_token AS token_deposited,
    f.output_token AS token_received,
    d.input_amount AS amount_deposited,
    f.output_amount AS amount_received
FROM across_deposits d
LEFT JOIN across_fills f
    ON d.deposit_id = f.deposit_id
{% if is_incremental() %}
  WHERE d.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       d.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND CAST(d.deposit_id AS STRING) IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
