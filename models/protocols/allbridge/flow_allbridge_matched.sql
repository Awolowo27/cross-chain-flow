{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='deposit_tx_hash',
    cluster_by=['source_chain', 'destination_chain', 'status']
  )
}}

WITH allbridge_sent AS (
    {{ v2_allbridge_sent('ethereum', ref('ethereum_bridge_events'), ref('ethereum_bridge_events'), '0xC51397b75B783E31469bFaADE79913F3f82210d6', '0x609c690e8f7d68a59885c9132e812eebdaaf0c9e', '0xec455ffc19811e573eb5700a1bdff6ee1c47ab7b') }}
    UNION ALL
    {{ v2_allbridge_sent('arbitrum', ref('arbitrum_bridge_events'), ref('arbitrum_bridge_events'), '0x23e1aec13c92158643cf2aa17e155d27a792ccdb', '0x9Ce3447B58D58e8602B7306316A5fF011B92d189', '0xB074e73e637E778BE6411c3732bD58D44194FDEa') }}
    UNION ALL
    {{ v2_allbridge_sent('polygon', ref('polygon_bridge_events'), ref('polygon_bridge_events'), '0x710282BfeB554Ed0A34dFaD061C7c343221AC82C', '0x7775d63836987f444E2F14AA0fA2602204D7D3E0') }}
    UNION ALL
    {{ v2_allbridge_sent('optimism', ref('optimism_bridge_events'), ref('optimism_bridge_events'), '0x08391edF36f41f05d27A1e0fD7a29448417C1CD0', '0x97E5BF5068eA6a9604Ee25851e6c9780Ff50d5ab') }}
    UNION ALL
    {{ v2_allbridge_sent('avalanche', ref('avalanche_bridge_events'), ref('avalanche_bridge_events'), '0x1eFE2C85989D97fEBbD0743cdd79B9F0826314f6', '0x9068E1C28941D0A680197Cc03be8aFe27ccaeea9') }}
),

allbridge_received AS (
    {{ v2_allbridge_received('ethereum', ref('ethereum_bridge_events'), ref('ethereum_bridge_events'), '0xC51397b75B783E31469bFaADE79913F3f82210d6', '0x609c690e8f7d68a59885c9132e812eebdaaf0c9e', '0xec455ffc19811e573eb5700a1bdff6ee1c47ab7b') }}
    UNION ALL
    {{ v2_allbridge_received('arbitrum', ref('arbitrum_bridge_events'), ref('arbitrum_bridge_events'), '0x23e1aec13c92158643cf2aa17e155d27a792ccdb', '0x9Ce3447B58D58e8602B7306316A5fF011B92d189', '0xB074e73e637E778BE6411c3732bD58D44194FDEa') }}
    UNION ALL
    {{ v2_allbridge_received('polygon', ref('polygon_bridge_events'), ref('polygon_bridge_events'), '0x710282BfeB554Ed0A34dFaD061C7c343221AC82C', '0x7775d63836987f444E2F14AA0fA2602204D7D3E0') }}
    UNION ALL
    {{ v2_allbridge_received('optimism', ref('optimism_bridge_events'), ref('optimism_bridge_events'), '0x08391edF36f41f05d27A1e0fD7a29448417C1CD0', '0x97E5BF5068eA6a9604Ee25851e6c9780Ff50d5ab') }}
    UNION ALL
    {{ v2_allbridge_received('avalanche', ref('avalanche_bridge_events'), ref('avalanche_bridge_events'), '0x1eFE2C85989D97fEBbD0743cdd79B9F0826314f6', '0x9068E1C28941D0A680197Cc03be8aFe27ccaeea9') }}
),

matched AS (
    SELECT
        'Allbridge' AS bridge_name,
        d.correlation_id,
        IF(f.fill_tx_hash IS NOT NULL, 'COMPLETED', 'PENDING') AS status,
        d.deposit_chain AS source_chain,
        COALESCE(f.fill_chain, 'unknown') AS destination_chain,
        d.block_time AS deposit_timestamp,
        f.block_time AS fill_timestamp,
        TIMESTAMP_DIFF(f.block_time, d.block_time, SECOND) AS time_to_fill_seconds,
        d.depositor AS user_address,
        d.tx_hash AS deposit_tx_hash,
        f.fill_tx_hash AS destination_tx_hash,
        d.token_address AS source_token_address,
        COALESCE(f.token_address, d.token_address) AS destination_token_address,
        d.raw_amount AS amount_deposited_raw,
        f.raw_amount AS amount_received_raw

    FROM allbridge_sent d
    LEFT JOIN allbridge_received f
        ON d.correlation_id = f.correlation_id
)

SELECT * FROM matched
