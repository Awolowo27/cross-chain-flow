{% macro v2_stargate_v2_received(blockchain, base_events, source_logs, source_txs, token_messaging_contract='0x19cfce47ed54a88614648dc3f19a5980097007dd', pool_contract_addresses=None) %}

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
  l_oft.block_timestamp AS block_time,
  l_oft.block_number,
  l_oft.transaction_hash AS tx_hash,
  
  l_oft.address AS pool_address,

  -- Dynamic Token Address (ERC20 Transfer token -> Pool fallback -> Native ETH fallback)
  COALESCE(
    l_token.address, 
    l_oft.address, 
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_received_address,

  l_oft.topic1 AS guid,
  CONCAT('0x', SUBSTR(l_oft.topic3, 27)) AS receiver,

  CAST(('0x' || SUBSTR(l_oft.data, 3 + 64*0, 64)) AS INT64) AS source_endpoint_id,
  {{ hex_to_bignumeric("SUBSTR(l_oft.data, 3 + 64*1, 64)") }} AS amount_received_local_decimals

FROM {{ base_events }} AS l_oft

INNER JOIN {{ source_txs }} AS tx
    ON l_oft.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

LEFT JOIN {{ source_logs }} AS l_token
  ON l_oft.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

LEFT JOIN {{ source_logs }} AS l_msg
  ON l_oft.transaction_hash = l_msg.transaction_hash
 AND l_msg.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND LOWER(l_msg.address) = LOWER('{{token_messaging_contract}}')

WHERE l_oft.topic0 = '0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c'
  AND (
    {% if pool_contract_addresses and pool_contract_addresses | length > 0 %}
      LOWER(l_oft.address) IN (
        {% for addr in pool_contract_addresses %}
          LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}
        {% endfor %}
      )
      OR
    {% endif %}
    l_msg.transaction_hash IS NOT NULL
    OR LOWER(l_oft.address) = LOWER('{{token_messaging_contract}}')
  )

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_oft.transaction_hash, l_oft.topic1
  ORDER BY l_oft.block_timestamp DESC
) = 1

{% endmacro %}
