{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH mayan_sent AS (
    {{ v2_mayan_sent('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x40ffe85a28dc9993541449464d7529a922142960') }}
    UNION ALL
    {{ v2_mayan_sent('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x40ffe85a28dc9993541449464d7529a922142960') }}
    UNION ALL
    {{ v2_mayan_sent('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0x40ffe85a28dc9993541449464d7529a922142960') }}
    UNION ALL
    {{ v2_mayan_sent('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x40ffe85a28dc9993541449464d7529a922142960') }}
),
mayan_received AS (
    {{ v2_mayan_received('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe') }}
    UNION ALL
    {{ v2_mayan_received('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe') }}
    UNION ALL
    {{ v2_mayan_received('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe') }}
    UNION ALL
    {{ v2_mayan_received('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe') }}
)

SELECT
    'Mayan Swift' AS bridge_name,
    s.order_key AS correlation_id,
    s.source_chain AS source_chain,
    r.destination_chain AS destination_chain,
    s.block_time AS deposit_timestamp,
    r.block_time AS fill_timestamp,
    TIMESTAMP_DIFF(r.block_time, s.block_time, SECOND) AS time_to_fill_seconds,
    CASE 
      WHEN r.tx_hash IS NOT NULL THEN 'COMPLETED'
      ELSE 'PENDING'
    END AS status,
    s.sender AS user_address,
    s.tx_hash AS deposit_tx_hash,
    r.tx_hash AS destination_tx_hash,
    s.token_sent_address AS token_deposited,
    r.token_received_address AS token_received,
    s.amount_sent_local_decimals AS amount_deposited,
    r.amount_received_local_decimals AS amount_received
FROM mayan_sent s
LEFT JOIN mayan_received r
    ON s.order_key = r.order_key
{% if is_incremental() %}
  WHERE s.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       s.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND s.order_key IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
