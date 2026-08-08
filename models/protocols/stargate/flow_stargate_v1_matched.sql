{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH stargate_v1_sent AS (
    {{ v2_stargate_v1_sent('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x352d8275aae3e0c2404d9f68f6cee084b5beb3dd', 110) }}
    UNION ALL
    {{ v2_stargate_v1_sent('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97', 101) }}
    UNION ALL
    {{ v2_stargate_v1_sent('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0x701a95707A0290AC8B90b3719e8EE5b210360883', 111) }}
    UNION ALL
    {{ v2_stargate_v1_sent('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x9d1B1669c73b033DFe47ae5a0164Ab96df25B944', 106) }}
),
stargate_v1_received AS (
    {{ v2_stargate_v1_received('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x352d8275aae3e0c2404d9f68f6cee084b5beb3dd', 110) }}
    UNION ALL
    {{ v2_stargate_v1_received('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97', 101) }}
    UNION ALL
    {{ v2_stargate_v1_received('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0x701a95707A0290AC8B90b3719e8EE5b210360883', 111) }}
    UNION ALL
    {{ v2_stargate_v1_received('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x9d1B1669c73b033DFe47ae5a0164Ab96df25B944', 106) }}
)

SELECT
    'Stargate V1' AS bridge_name,
    CAST(s.nonce AS STRING) AS correlation_id,
    s.source_chain AS source_chain,
    r.destination_chain AS destination_chain,
    s.block_time AS deposit_timestamp,
    r.block_time AS fill_timestamp,
    TIMESTAMP_DIFF(r.block_time, s.block_time, SECOND) AS time_to_fill_seconds,
    CASE 
      WHEN r.tx_hash IS NOT NULL THEN 'COMPLETED'
      ELSE 'PENDING'
    END AS status,
    s.depositor AS user_address,
    s.tx_hash AS deposit_tx_hash,
    r.tx_hash AS destination_tx_hash,
    s.token_sent_address AS token_deposited,
    r.token_received_address AS token_received,
    s.amount_sent_sd AS amount_deposited,
    r.amount_received_sd AS amount_received
FROM stargate_v1_sent s
LEFT JOIN stargate_v1_received r
    ON s.source_chain_id = r.source_chain_id
   AND LOWER(s.source_bridge_address) = LOWER(r.source_bridge_address)
   AND s.nonce = r.nonce
{% if is_incremental() %}
  WHERE s.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       s.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND CAST(s.nonce AS STRING) IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
