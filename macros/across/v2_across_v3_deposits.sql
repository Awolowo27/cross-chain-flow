{% macro v2_across_v3_deposits(blockchain, base_events, contract_address) %}

SELECT
  '{{blockchain}}' AS deposit_chain,
  block_timestamp AS block_time,
  block_number,
  transaction_hash AS tx_hash,
  SAFE_CAST(('0x' || RIGHT(topic1, 16)) AS INT64) AS destination_chain_id,
  COALESCE(
    CAST(SAFE_CAST(topic2 AS INT64) AS STRING),
    LOWER(TRIM(topic2))
  ) AS deposit_id,
  CONCAT('0x', SUBSTR(topic3, 27)) AS depositor,
  CONCAT('0x', SUBSTR(data, 3 + 64*0 + 24, 40)) AS input_token,
  CONCAT('0x', SUBSTR(data, 3 + 64*1 + 24, 40)) AS output_token,
  {{ hex_to_bignumeric("SUBSTR(data, 3 + 64*2, 64)") }} AS input_amount,
  {{ hex_to_bignumeric("SUBSTR(data, 3 + 64*3, 64)") }} AS output_amount,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*4, 64), 16)) AS INT64) AS quote_timestamp,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*5, 64), 16)) AS INT64) AS fill_deadline,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*6, 64), 16)) AS INT64) AS exclusivity_deadline,
  CONCAT('0x', SUBSTR(data, 3 + 64*7 + 24, 40)) AS recipient,
  CONCAT('0x', SUBSTR(data, 3 + 64*8 + 24, 40)) AS exclusive_relayer

FROM {{ base_events }}
WHERE 
  address = LOWER('{{ contract_address }}')
  AND topic0 IN (
    '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f', 
    '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3'
  )

{% endmacro %}
