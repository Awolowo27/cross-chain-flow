{% macro v2_allbridge_sent(blockchain, base_events, source_logs, cctp_contract='0xC51397b75B783E31469bFaADE79913F3f82210d6', core_contract='0x609c690e8f7d68a59885c9132e812eebdaaf0c9e', oft_contract='0xec455ffc19811e573eb5700a1bdff6ee1c47ab7b') %}

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
  '{{ blockchain }}' AS deposit_chain,
  l_all.block_timestamp AS block_time,
  l_all.block_number,
  l_all.transaction_hash AS tx_hash,
  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a')
      THEN LOWER(l_all.topic1)
    ELSE LOWER(CONCAT('0x', SUBSTR(l_all.data, 3 + 64*4, 64)))
  END AS correlation_id,

  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a')
      THEN CONCAT('0x', SUBSTR(l_all.topic2, 27))
    ELSE CONCAT('0x', SUBSTR(l_all.data, 3 + 64*1 + 24, 40))
  END AS depositor,

  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a')
      THEN CONCAT('0x', SUBSTR(l_all.data, 3 + 64*2, 64))
    ELSE LOWER(CONCAT('0x', SUBSTR(l_all.data, 3 + 64*2, 64)))
  END AS recipient,

  LOWER(l_token.address) AS token_address,

  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    IF(LOWER(l_all.topic0) = LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'),
       {{ hex_to_bignumeric("SUBSTR(l_all.data, 67, 64)") }},
       {{ hex_to_bignumeric("SUBSTR(l_all.data, 3, 64)") }}
    )
  ) AS raw_amount,

  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a')
      THEN SAFE_CAST({{ hex_to_bignumeric("SUBSTR(l_all.data, 3, 64)") }} AS INT64)
    WHEN LOWER(l_all.topic0) = LOWER('0x9cd6008e8d4ebd34fd9d022278fec7f95d133780ecc1a0dea459fae3e9675390')
      THEN SAFE_CAST(('0x' || RIGHT(SUBSTR(l_all.data, 3 + 64*2, 64), 16)) AS INT64)
    ELSE SAFE_CAST(('0x' || RIGHT(SUBSTR(l_all.data, 3 + 64*3, 64), 16)) AS INT64)
  END AS destination_chain_id

FROM {{ base_events }} AS l_all

LEFT JOIN {{ source_logs }} AS l_token
  ON l_all.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND LOWER(l_token.topic0) = LOWER('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') -- ERC-20 Transfer

WHERE 
  LOWER(l_all.topic0) IN (
    LOWER('0xaf142c08d51839efeb25c71c958dec48ffa3a832ba72324fbf8802dea6ec2bd1'), -- CCTP TokensSent
    LOWER('0x9cd6008e8d4ebd34fd9d022278fec7f95d133780ecc1a0dea459fae3e9675390'), -- Core TokensSent
    LOWER('0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a')  -- LZ OFTSent
  )
  AND (
    LOWER(l_all.address) IN (
      LOWER('{{ cctp_contract }}'),
      LOWER('{{ core_contract }}'),
      LOWER('{{ oft_contract }}')
    )
    OR LOWER(CONCAT('0x', SUBSTR(l_all.topic2, 27))) = LOWER('{{ oft_contract }}')
  )

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_all.transaction_hash, l_all.log_index
  ORDER BY l_all.block_timestamp DESC
) = 1

{% endmacro %}
