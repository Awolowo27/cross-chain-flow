{% macro v2_across_v3_fills(blockchain, base_events, contract_address) %}

SELECT
  '{{blockchain}}' AS fill_chain,
  block_timestamp AS block_time,
  block_number,
  transaction_hash AS tx_hash,
  SAFE_CAST(('0x' || RIGHT(topic1, 16)) AS INT64) AS origin_chain_id,
  COALESCE(
    CAST(SAFE_CAST(topic2 AS INT64) AS STRING),
    LOWER(TRIM(topic2))
  ) AS deposit_id,
  CONCAT('0x', SUBSTR(topic3, 27)) AS relayer,
  CONCAT('0x', SUBSTR(data, 3 + 64*0 + 24, 40)) AS input_token,
  CONCAT('0x', SUBSTR(data, 3 + 64*1 + 24, 40)) AS output_token,
  {{ hex_to_bignumeric("SUBSTR(data, 3 + 64*2, 64)") }} AS input_amount,
  {{ hex_to_bignumeric("SUBSTR(data, 3 + 64*3, 64)") }} AS output_amount,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*4, 64), 16)) AS INT64) AS repayment_chain_id,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*5, 64), 16)) AS INT64) AS fill_deadline,
  SAFE_CAST(('0x' || RIGHT(SUBSTR(data, 3 + 64*6, 64), 16)) AS INT64) AS exclusivity_deadline,
  CONCAT('0x', SUBSTR(data, 3 + 64*7 + 24, 40)) AS exclusive_relayer,
  CONCAT('0x', SUBSTR(data, 3 + 64*8 + 24, 40)) AS depositor,
  CONCAT('0x', SUBSTR(data, 3 + 64*9 + 24, 40)) AS recipient

FROM {{ base_events }}
WHERE 
  address = LOWER('{{ contract_address }}')
  AND topic0 IN (
    '0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7',
    '0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208'
  )

{% endmacro %}
