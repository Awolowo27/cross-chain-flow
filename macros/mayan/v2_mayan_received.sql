{% macro v2_mayan_received(blockchain, base_events, source_logs, source_txs, mayan_dest_contract='0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe') %}

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
  '{{blockchain}}' AS destination_chain,
  l_mayan.block_timestamp AS block_time,
  l_mayan.block_number,
  l_mayan.transaction_hash AS tx_hash,
  'MayanSwiftV2' AS protocol_name,
  l_mayan.address AS mayan_dest_contract,
  SUBSTR(l_mayan.data, 3, 64) AS order_key,
  CONCAT('0x', SUBSTR(l_token.topics[SAFE_OFFSET(2)], 27)) AS recipient,
  COALESCE(
    l_token.address,
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_received_address,
  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    SAFE_CAST(COALESCE(JSON_VALUE(TO_JSON_STRING(tx.value), '$.bignumeric_value'), JSON_VALUE(TO_JSON_STRING(tx.value), '$.string_value'), JSON_VALUE(TO_JSON_STRING(tx.value))) AS BIGNUMERIC)
  ) AS amount_received_local_decimals

FROM {{ base_events }} AS l_mayan

INNER JOIN {{ source_txs }} AS tx
    ON l_mayan.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

INNER JOIN {{ source_logs }} AS l_token
  ON l_mayan.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
 AND LOWER(CONCAT('0x', SUBSTR(l_token.topics[SAFE_OFFSET(1)], 27))) IN (
    LOWER('{{ mayan_dest_contract }}')
 )

WHERE LOWER(l_mayan.address) = LOWER('{{ mayan_dest_contract }}')
  AND l_mayan.topic0 = '0x6ec9b1b5a9f54d929394f18dac4ba1b1cc79823f2266c2d09cab8a3b4700b40b'

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_mayan.transaction_hash, SUBSTR(l_mayan.data, 3, 64)
  ORDER BY l_mayan.block_timestamp DESC
) = 1

{% endmacro %}
