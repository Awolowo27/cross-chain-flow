{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH debridge_sent AS (
    {{ v2_debridge_created_orders('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_created_orders('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_created_orders('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_created_orders('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions')) }}
),
debridge_received AS (
    {{ v2_debridge_fulfilled_orders('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_fulfilled_orders('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_fulfilled_orders('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions')) }}
    UNION ALL
    {{ v2_debridge_fulfilled_orders('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions')) }}
)

SELECT
    'deBridge DLN' AS bridge_name,
    s.order_id AS correlation_id,
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
FROM debridge_sent s
LEFT JOIN debridge_received r
    ON s.order_id = r.order_id
{% if is_incremental() %}
  WHERE s.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       s.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND s.order_id IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
