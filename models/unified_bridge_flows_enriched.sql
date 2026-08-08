{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_name', 'correlation_id', 'source_chain'],
    partition_by = { "field": "deposit_timestamp", "data_type": "timestamp", "granularity": "day" },
    cluster_by = ["status", "bridge_name", "source_chain", "destination_chain"]
) }}

SELECT
  b.bridge_name,
  b.correlation_id,
  b.status,
  b.source_chain,
  b.destination_chain,
  b.deposit_timestamp,
  b.fill_timestamp,
  b.time_to_fill_seconds,
  b.user_address,
  b.deposit_tx_hash,
  b.destination_tx_hash,

  -- Deposited Token Raw & Metadata
  b.token_deposited AS token_deposited_address,
  COALESCE(t_src.symbol, 'UNKNOWN') AS token_deposited_symbol,
  COALESCE(CAST(t_src.decimals AS INT64), 18) AS token_deposited_decimals,
  b.amount_deposited AS amount_deposited_raw,
  b.amount_deposited / POWER(10, COALESCE(CAST(t_src.decimals AS INT64), 18)) AS amount_deposited_normalized,

  -- Received Token Raw & Metadata
  b.token_received AS token_received_address,
  COALESCE(t_dst.symbol, 'UNKNOWN') AS token_received_symbol,
  COALESCE(CAST(t_dst.decimals AS INT64), 18) AS token_received_decimals,
  b.amount_received AS amount_received_raw,
  b.amount_received / POWER(10, COALESCE(CAST(t_dst.decimals AS INT64), 18)) AS amount_received_normalized

FROM {{ ref('unified_bridge_flows') }} AS b

LEFT JOIN {{ ref('dim_tokens') }} AS t_src
    ON LOWER(b.token_deposited) = LOWER(t_src.token_address)
   AND LOWER(b.source_chain) = LOWER(t_src.blockchain)

LEFT JOIN {{ ref('dim_tokens') }} AS t_dst
    ON LOWER(b.token_received) = LOWER(t_dst.token_address)
   AND LOWER(b.destination_chain) = LOWER(t_dst.blockchain)

{% if is_incremental() %}
  WHERE b.deposit_timestamp >= (SELECT TIMESTAMP_SUB(MAX(deposit_timestamp), INTERVAL 3 DAY) FROM {{ this }})
     OR (
       b.deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       AND b.correlation_id IN (
         SELECT correlation_id 
         FROM {{ this }} 
         WHERE status = 'PENDING' 
           AND deposit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
       )
     )
{% endif %}
