{% macro v2_debridge_fulfilled_orders(blockchain, base_events, source_logs, source_txs, debridge_contract='0xe7351fd770a37282b91d153ee690b63579d6dd7f') %}

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
  l_dst.block_timestamp AS block_time,
  l_dst.block_number,
  l_dst.transaction_hash AS tx_hash,
  'deBridgeDLN' AS protocol_name,
  l_dst.address AS dln_destination_address,
  CASE 
    WHEN LOWER(l_dst.topic0) = LOWER('0xe7b447743152a514d14217154942dcfb275ec9c490a6f8090715cf486e589926')
      THEN LOWER(CONCAT('0x', SUBSTR(l_dst.data, 3, 64)))
    ELSE LOWER(CONCAT('0x', SUBSTR(l_dst.data, 67, 64)))
  END AS order_id,
  tx.from_address AS fulfiller,
  COALESCE(
    IF(l_swap.data IS NOT NULL,
       IF(LOWER(CONCAT('0x', RIGHT(SUBSTR(l_swap.data, 195, 64), 40))) = '0x0000000000000000000000000000000000000000',
          '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
          LOWER(CONCAT('0x', RIGHT(SUBSTR(l_swap.data, 195, 64), 40)))
       ),
       NULL
    ),
    l_token.address,
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_received_address,
  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_dst.data, 131, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_dst.data, 67, 64)") }}
  ) AS amount_received_local_decimals

FROM {{ base_events }} AS l_dst

INNER JOIN {{ source_txs }} AS tx
    ON l_dst.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

LEFT JOIN {{ source_logs }} AS l_swap
  ON l_dst.transaction_hash = l_swap.transaction_hash
 AND l_swap.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND LOWER(l_swap.address) = LOWER('0x663dc15d3c1ac63ff12e45ab68fea3f0a883c251')
 AND LOWER(l_swap.topics[SAFE_OFFSET(0)]) = LOWER('0xdde2f3711ab09cdddcfee16ca03e54d21fb8cf3fa647b9797913c950d38ad693')

LEFT JOIN {{ source_logs }} AS l_token
  ON l_dst.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

WHERE LOWER(l_dst.address) = LOWER('0xE7351Fd770A37282b91D153Ee690B63579D6dd7f')
  AND LOWER(l_dst.topic0) = LOWER('0xc164aca37b9805a1c9027b6f32260a069723a82926f6e9ece4926e4dd3ea8ecf')

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_dst.transaction_hash, l_dst.log_index
  ORDER BY l_dst.block_timestamp DESC
) = 1

{% endmacro %}
