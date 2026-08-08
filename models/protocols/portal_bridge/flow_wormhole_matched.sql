{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH wormhole_published AS (
    {{ v2_portal_bridge_sent('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x3ee18b2214aff97000d974cf647e7c347e8fa585', '0x98f3c9e6e3face36baad05fe09d375ef1464288b') }}
    UNION ALL
    {{ v2_portal_bridge_sent('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x0b2402144bb366a632d14b83f244d2e0e21bd39c', '0xa5f208e072434bc67592e4c49c1b991ba79bca46') }}
    UNION ALL
    {{ v2_portal_bridge_sent('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0x1d68124e65fafc907325e3edbf8c4d84499daa8b', '0x98f3c9e6e3face36baad05fe09d375ef1464288b') }}
    UNION ALL
    {{ v2_portal_bridge_sent('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x0e082f06ff657d94310cb8ce8b0d9a04541d8052', '0x54a8e5f9c4cba08f9943965859f6c34eaf03e26c') }}
),
wormhole_redeemed AS (
    {{ v2_portal_bridge_received('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x3ee18b2214aff97000d974cf647e7c347e8fa585') }}
    UNION ALL
    {{ v2_portal_bridge_received('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x0b2402144bb366a632d14b83f244d2e0e21bd39c') }}
    UNION ALL
    {{ v2_portal_bridge_received('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0x1d68124e65fafc907325e3edbf8c4d84499daa8b') }}
    UNION ALL
    {{ v2_portal_bridge_received('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x0e082f06ff657d94310cb8ce8b0d9a04541d8052') }}
)

SELECT
    'Wormhole' AS bridge_name,
    CAST(p.sequence AS STRING) AS correlation_id,
    p.source_chain AS source_chain,
    r.destination_chain AS destination_chain,
    p.block_time AS deposit_timestamp,
    r.block_time AS fill_timestamp,
    TIMESTAMP_DIFF(r.block_time, p.block_time, SECOND) AS time_to_fill_seconds,
    CASE 
      WHEN r.tx_hash IS NOT NULL THEN 'COMPLETED'
      ELSE 'PENDING'
    END AS status,
    p.sender AS user_address,
    p.tx_hash AS deposit_tx_hash,
    r.tx_hash AS destination_tx_hash,
    p.token_sent_address AS token_deposited,
    r.token_received_address AS token_received,
    p.amount_sent_local_decimals AS amount_deposited,
    r.amount_received_local_decimals AS amount_received
FROM wormhole_published p
LEFT JOIN wormhole_redeemed r
    ON p.sequence = r.sequence
   AND LOWER(p.portal_bridge_address) = LOWER(r.emitter_address)
{% if is_incremental() %}
  WHERE p.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       p.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND CAST(p.sequence AS STRING) IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
