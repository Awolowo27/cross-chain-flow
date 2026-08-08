{% macro v2_portal_bridge_sent(blockchain, base_events, source_logs, source_txs, portal_bridge_address='0x0b2402144bb366a632d14b83f244d2e0e21bd39c', wormhole_core_address='0xa5f208e072434bc67592e4c49c1b991ba79bca46') %}

{% if execute %}
  {% set bounds_query %}
    SELECT 
      COALESCE(CAST(MIN(block_timestamp) AS STRING), '1970-01-01 00:00:00 UTC') AS min_ts, 
      COALESCE(CAST(MAX(block_timestamp) AS STRING), '2099-12-31 23:59:59 UTC') AS max_ts
    FROM {{ base_events }}
  {% endset %}
  {% set bounds = run_query(bounds_query) %}
  {% set min_ts = bounds.columns[0].values()[0] %}
  {% set max_ts = bounds.columns[1].values()[0] %}
{% else %}
  {% set min_ts = '1970-01-01 00:00:00 UTC' %}
  {% set max_ts = '2099-12-31 23:59:59 UTC' %}
{% endif %}

SELECT
  '{{blockchain}}' AS source_chain,
  l_core.block_timestamp AS block_time,
  l_core.block_number,
  l_core.transaction_hash AS tx_hash,
  
  'PortalBridge' AS protocol_name,
  '{{portal_bridge_address}}' AS portal_bridge_address,
  l_core.address AS wormhole_core_address,

  -- Depositor / Sender Address
  tx.from_address AS sender,

  -- Wormhole Sequence Number (data slot 0: uint64)
  SAFE_CAST(('0x' || RIGHT(SUBSTR(l_core.data, 3 + 64*0, 64), 16)) AS INT64) AS sequence,
  
  -- Wormhole Nonce (data slot 1: uint32)
  SAFE_CAST(('0x' || RIGHT(SUBSTR(l_core.data, 3 + 64*1, 64), 16)) AS INT64) AS nonce,

  -- Dynamic Token Address
  COALESCE(
    l_token.address,
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_sent_address,

  -- Amount Sent
  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    SAFE_CAST(COALESCE(JSON_VALUE(TO_JSON_STRING(tx.value), '$.bignumeric_value'), JSON_VALUE(TO_JSON_STRING(tx.value), '$.string_value'), JSON_VALUE(TO_JSON_STRING(tx.value))) AS BIGNUMERIC)
  ) AS amount_sent_local_decimals

FROM {{ base_events }} AS l_core

INNER JOIN {{ source_txs }} AS tx
    ON l_core.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

LEFT JOIN {{ source_logs }} AS l_token
    ON l_core.transaction_hash = l_token.transaction_hash
   AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

WHERE l_core.address = LOWER('{{ wormhole_core_address }}')
  AND l_core.topic0 = '0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2'
  AND LOWER(CONCAT('0x', SUBSTR(l_core.topic1, 27))) = LOWER('{{ portal_bridge_address }}')

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_core.transaction_hash, CAST(('0x' || SUBSTR(l_core.data, 3 + 64*0, 64)) AS INT64)
  ORDER BY l_core.block_timestamp DESC
) = 1

{% endmacro %}
