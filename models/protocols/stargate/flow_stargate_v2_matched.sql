{{ config(
    schema = 'protocol_flows',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

WITH stargate_v2_sent AS (
    {{ v2_stargate_v2_sent('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x6d6620eFa72948C5f68A3C8646d58C00d3f4A980', ['0x77b2043768d28E9C9aB44E1aBfC95944bcE57931', '0xc026395860Db2d07ee33e05fE50ed7bD583189C7', '0x933597a323Eb81cAe705C5bC29985172fd5A3973', '0x268Ca24DAefF1FaC2ed883c598200CcbB79E931D']) }}
    UNION ALL
    {{ v2_stargate_v2_sent('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x19cfce47ed54a88614648dc3f19a5980097007dd', ['0xa45b5130f36cdca45667738e2a258ab09f4a5f7f', '0xe8CDF27AcD73a434D661C84887215F7598e7d0d3', '0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0']) }}
    UNION ALL
    {{ v2_stargate_v2_sent('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0xF1fCb4CBd57B67d683972A59B6a7b1e2E8Bf27E6', ['0xe8CDF27AcD73a434D661C84887215F7598e7d0d3', '0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0', '0x19cFCE47eD54a88614648DC3f19A5980097007dD']) }}
    UNION ALL
    {{ v2_stargate_v2_sent('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x17E450Be3Ba9557F2378E20d64AD417E59Ef9A34', ['0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47', '0x12dC9256Acc9895B076f6638D628382881e62CeE']) }}
),
stargate_v2_received AS (
    {{ v2_stargate_v2_received('ethereum', ref('ethereum_bridge_events'), source('crypto_ethereum', 'logs'), source('goog_blockchain_ethereum_mainnet_us', 'transactions'), '0x6d6620eFa72948C5f68A3C8646d58C00d3f4A980', ['0x77b2043768d28E9C9aB44E1aBfC95944bcE57931', '0xc026395860Db2d07ee33e05fE50ed7bD583189C7', '0x933597a323Eb81cAe705C5bC29985172fd5A3973', '0x268Ca24DAefF1FaC2ed883c598200CcbB79E931D']) }}
    UNION ALL
    {{ v2_stargate_v2_received('arbitrum', ref('arbitrum_bridge_events'), source('goog_blockchain_arbitrum_one_us', 'logs'), source('goog_blockchain_arbitrum_one_us', 'transactions'), '0x19cfce47ed54a88614648dc3f19a5980097007dd', ['0xa45b5130f36cdca45667738e2a258ab09f4a5f7f', '0xe8CDF27AcD73a434D661C84887215F7598e7d0d3', '0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0']) }}
    UNION ALL
    {{ v2_stargate_v2_received('optimism', ref('optimism_bridge_events'), source('goog_blockchain_optimism_mainnet_us', 'logs'), source('goog_blockchain_optimism_mainnet_us', 'transactions'), '0xF1fCb4CBd57B67d683972A59B6a7b1e2E8Bf27E6', ['0xe8CDF27AcD73a434D661C84887215F7598e7d0d3', '0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0', '0x19cFCE47eD54a88614648DC3f19A5980097007dD']) }}
    UNION ALL
    {{ v2_stargate_v2_received('avalanche', ref('avalanche_bridge_events'), source('goog_blockchain_avalanche_contract_chain_us', 'logs'), source('goog_blockchain_avalanche_contract_chain_us', 'transactions'), '0x17E450Be3Ba9557F2378E20d64AD417E59Ef9A34', ['0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47', '0x12dC9256Acc9895B076f6638D628382881e62CeE']) }}
)

SELECT
    'Stargate V2' AS bridge_name,
    s.guid AS correlation_id,
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
FROM stargate_v2_sent s
LEFT JOIN stargate_v2_received r
    ON s.guid = r.guid
{% if is_incremental() %}
  WHERE s.block_time >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       s.block_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND s.guid IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
