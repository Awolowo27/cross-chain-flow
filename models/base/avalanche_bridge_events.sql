{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['transaction_hash', 'log_index'],
    partition_by = { "field": "block_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["address", "topic0"]
) }}

{% set reg = v2_bridge_registry('avalanche') %}

SELECT
    block_timestamp,
    block_number,
    transaction_hash,
    log_index,
    LOWER(address) AS address,
    topics[SAFE_OFFSET(0)] AS topic0,
    topics[SAFE_OFFSET(1)] AS topic1,
    topics[SAFE_OFFSET(2)] AS topic2,
    topics[SAFE_OFFSET(3)] AS topic3,
    topics,
    data
FROM {{ source('goog_blockchain_avalanche_contract_chain_us', 'logs') }}
WHERE 
  (
    LOWER(address) IN ({% for addr in reg.contracts %}'{{ addr }}'{% if not loop.last %}, {% endif %}{% endfor %})
    OR
    topics[SAFE_OFFSET(0)] IN ({% for topic in reg.topics %}'{{ topic }}'{% if not loop.last %}, {% endif %}{% endfor %})
  )
{% if is_incremental() %}
  AND block_timestamp >= (SELECT TIMESTAMP_SUB(MAX(block_timestamp), INTERVAL 3 DAY) FROM {{ this }})
{% else %}
  AND block_timestamp >= TIMESTAMP('{{ var("start_date", "2026-08-06 00:00:00") }}')
{% endif %}
