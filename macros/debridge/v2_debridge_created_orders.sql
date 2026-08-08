{% macro v2_debridge_created_orders(blockchain, base_events, source_logs, source_txs, debridge_contract='0xef4fb24ad0916217251f553c0596f8edc630eb66') %}

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
  l_src.block_timestamp AS block_time,
  l_src.block_number,
  l_src.transaction_hash AS tx_hash,
  'deBridgeDLN' AS protocol_name,
  l_src.address AS dln_source_address,
  LOWER(CONCAT('0x', SUBSTR(l_src.data, 67, 64))) AS order_id,
  tx.from_address AS sender,
  COALESCE(l_token.address, '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') AS token_sent_address,
  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    SAFE_CAST(COALESCE(JSON_VALUE(TO_JSON_STRING(tx.value), '$.bignumeric_value'), JSON_VALUE(TO_JSON_STRING(tx.value), '$.string_value'), JSON_VALUE(TO_JSON_STRING(tx.value))) AS BIGNUMERIC)
  ) AS amount_sent_local_decimals

FROM {{ base_events }} AS l_src

INNER JOIN {{ source_txs }} AS tx
    ON l_src.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

LEFT JOIN {{ source_logs }} AS l_token
    ON l_src.transaction_hash = l_token.transaction_hash
   AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
   AND LOWER(CONCAT('0x', SUBSTR(l_token.topics[SAFE_OFFSET(2)], 27))) = LOWER('{{ debridge_contract }}')

WHERE l_src.address = LOWER('{{ debridge_contract }}')
  AND l_src.topic0 = '0xfc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471'

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_src.transaction_hash, l_src.log_index
  ORDER BY l_src.block_timestamp DESC
) = 1

{% endmacro %}
