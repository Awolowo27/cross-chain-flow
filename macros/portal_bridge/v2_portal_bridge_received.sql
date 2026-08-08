{% macro v2_portal_bridge_received(blockchain, base_events, source_logs, source_txs, portal_bridge_address='0x0b2402144bb366a632d14b83f244d2e0e21bd39c') %}

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
  l_token.block_timestamp AS block_time,
  l_token.block_number,
  l_token.transaction_hash AS tx_hash,

  'PortalBridge' AS protocol_name,
  '{{portal_bridge_address}}' AS portal_bridge_address,

  -- Wormhole Correlation Connector Keys (20-byte formatted emitter address)
  SAFE_CAST(('0x' || RIGHT(l_token.topic1, 16)) AS INT64) AS emitter_chain_id,
  CONCAT('0x', SUBSTR(l_token.topic2, 27)) AS emitter_address,
  SAFE_CAST(('0x' || RIGHT(l_token.topic3, 16)) AS INT64) AS sequence,

  -- Recipient Address (from ERC-20 Transfer log topic2)
  CONCAT('0x', SUBSTR(txo.topics[SAFE_OFFSET(2)], 27)) AS recipient,

  -- Dynamic Token Address
  txo.address AS token_received_address,

  -- Amount Received
  {{ hex_to_bignumeric("SUBSTR(txo.data, 3, 64)") }} AS amount_received_local_decimals

FROM {{ base_events }} AS l_token

INNER JOIN {{ source_logs }} AS txo
    ON l_token.transaction_hash = txo.transaction_hash
   AND txo.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND txo.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

WHERE l_token.topic0 = '0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169'
  AND l_token.address = LOWER('{{ portal_bridge_address }}')
  AND LOWER(CONCAT('0x', SUBSTR(txo.topics[SAFE_OFFSET(2)], 27))) != '0x0000000000000000000000000000000000000000'

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_token.transaction_hash, txo.address
  ORDER BY l_token.block_timestamp DESC
) = 1

{% endmacro %}
