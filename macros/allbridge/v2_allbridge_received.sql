{% macro v2_allbridge_received(blockchain, base_events, source_logs, cctp_contract='0xC51397b75B783E31469bFaADE79913F3f82210d6', core_contract='0x609c690e8f7d68a59885c9132e812eebdaaf0c9e', oft_contract='0xec455ffc19811e573eb5700a1bdff6ee1c47ab7b') %}

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
  '{{ blockchain }}' AS fill_chain,
  l_all.block_timestamp AS block_time,
  l_all.block_number,
  l_all.transaction_hash AS fill_tx_hash,
  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c')
      THEN LOWER(l_all.topic1)
    WHEN LOWER(l_all.topic0) = LOWER('0x58200b4c34ae05ee816d710053fff3fb75af4395915d3d2a771b24aa10e3cc5d')
      THEN LOWER(CONCAT('0x', SUBSTR(l_all.topic2, 27)))
    ELSE LOWER(CONCAT('0x', SUBSTR(l_all.data, 3 + 64*2, 64)))
  END AS correlation_id,

  CASE
    WHEN LOWER(l_all.topic0) = LOWER('0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c')
      THEN CONCAT('0x', SUBSTR(l_all.topic2, 27))
    WHEN LOWER(l_all.topic0) = LOWER('0x58200b4c34ae05ee816d710053fff3fb75af4395915d3d2a771b24aa10e3cc5d')
      THEN CONCAT('0x', SUBSTR(l_all.data, 3 + 64*5 + 24, 40))
    ELSE CONCAT('0x', SUBSTR(l_all.data, 3 + 64*1 + 24, 40))
  END AS recipient,

  LOWER(l_token.address) AS token_address,

  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    IF(LOWER(l_all.topic0) = LOWER('0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'),
       {{ hex_to_bignumeric("SUBSTR(l_all.data, 67, 64)") }},
       IF(LOWER(l_all.topic0) = LOWER('0x58200b4c34ae05ee816d710053fff3fb75af4395915d3d2a771b24aa10e3cc5d'),
          {{ hex_to_bignumeric("SUBSTR(l_all.data, 3 + 64*6, 64)") }},
          {{ hex_to_bignumeric("SUBSTR(l_all.data, 3, 64)") }}
       )
    )
  ) AS raw_amount

FROM {{ base_events }} AS l_all

LEFT JOIN {{ source_logs }} AS l_token
  ON l_all.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND LOWER(l_token.topic0) = LOWER('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') -- ERC-20 Transfer

WHERE 
  LOWER(l_all.topic0) IN (
    LOWER('0x58200b4c34ae05ee816d710053fff3fb75af4395915d3d2a771b24aa10e3cc5d'), -- CCTP MessageReceived
    LOWER('0xe9d840d27ab4032a839c20760fb995af8e3ad1980b9428980ca1c7e072acd87a'), -- Core TokensReceived
    LOWER('0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c')  -- LZ OFTReceived
  )
  AND (
    LOWER(l_all.address) IN (
      LOWER(COALESCE('{{ cctp_contract }}', '0x0')),
      LOWER(COALESCE('{{ core_contract }}', '0x0')),
      LOWER(COALESCE('{{ oft_contract }}', '0x0'))
    )
    OR LOWER(CONCAT('0x', SUBSTR(l_all.topic1, 27))) IN (
      LOWER(COALESCE('{{ cctp_contract }}', '0x0')),
      LOWER(COALESCE('{{ core_contract }}', '0x0')),
      LOWER(COALESCE('{{ oft_contract }}', '0x0'))
    )
  )

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_all.transaction_hash, l_all.log_index
  ORDER BY l_all.block_timestamp DESC
) = 1

{% endmacro %}
