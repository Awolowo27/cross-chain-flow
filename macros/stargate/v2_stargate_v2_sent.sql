{% macro v2_stargate_v2_sent(blockchain, base_events, source_logs, source_txs, token_messaging_contract='0x19cfce47ed54a88614648dc3f19a5980097007dd', pool_contract_addresses=None) %}

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
  l_oft.block_timestamp AS block_time,
  l_oft.block_number,
  l_oft.transaction_hash AS tx_hash,
  
  -- Event Type: OFTSent (Taxi / Individual), BusDriven (Bus Batch Dispatch), or BusRode (Bus Passenger Ticket)
  CASE l_oft.topic0
      WHEN '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' THEN 'OFTSent'
      WHEN '0x1623f9ea59bd6f214c9571a892da012fc23534aa5906bef4ae8c5d15ee7d2d6e' THEN 'BusDriven'
      ELSE 'BusRode'
  END AS event_type,

  l_oft.address AS pool_address,
  
  -- Dynamic Token Address (ERC20 Transfer token -> Pool fallback -> Native ETH fallback)
  COALESCE(
    l_token.address, 
    l_oft.address, 
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_sent_address,
  
  -- Extract GUID (topics[1] for OFTSent, data slot 3 for BusDriven)
  COALESCE(
    l_oft.topic1,
    SUBSTR(l_oft.data, 3 + 64*3, 64)
  ) AS guid,

  -- Sender Address (topics[2] -> Fallback to Transaction Initiator tx.from_address)
  COALESCE(
    CONCAT('0x', SUBSTR(l_oft.topic2, 27)),
    tx.from_address
  ) AS sender,

  -- Destination Endpoint ID (dstEid: slot 0 for OFTSent, BusDriven, and BusRode)
  CAST(('0x' || SUBSTR(l_oft.data, 3 + 64*0, 64)) AS INT64) AS destination_endpoint_id,

  -- Amount Sent (OFTSent slot 1 -> ERC20 Transfer amount -> Transaction ETH Value)
  COALESCE(
    CASE l_oft.topic0
        WHEN '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' THEN {{ hex_to_bignumeric("SUBSTR(l_oft.data, 3 + 64*1, 64)") }}
        ELSE NULL
    END,
    {{ hex_to_bignumeric("SUBSTR(l_token.data, 3, 64)") }},
    SAFE_CAST(COALESCE(JSON_VALUE(TO_JSON_STRING(tx.value), '$.bignumeric_value'), JSON_VALUE(TO_JSON_STRING(tx.value), '$.string_value'), JSON_VALUE(TO_JSON_STRING(tx.value))) AS BIGNUMERIC)
  ) AS amount_sent_local_decimals

FROM {{ base_events }} AS l_oft

-- Join with Transactions table to guarantee non-null sender & fallback ETH value
INNER JOIN {{ source_txs }} AS tx
    ON l_oft.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

-- Extract exact token contract address and amount from ERC-20 Transfer log in same TX
LEFT JOIN {{ source_logs }} AS l_token
  ON l_oft.transaction_hash = l_token.transaction_hash
 AND l_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND l_token.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

-- Cross-verify TX interaction with official Stargate V2 Token Messaging Router (0x19cfce47...)
LEFT JOIN {{ source_logs }} AS l_msg
  ON l_oft.transaction_hash = l_msg.transaction_hash
 AND l_msg.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
 AND LOWER(l_msg.address) = LOWER('{{token_messaging_contract}}')

WHERE l_oft.topic0 IN (
    '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a', -- OFTSent (Taxi Mode)
    '0x1623f9ea59bd6f214c9571a892da012fc23534aa5906bef4ae8c5d15ee7d2d6e', -- BusDriven (Bus Mode)
    '0xe62c9535eb9faefdf05a0b784a0d9b4b025a1e2f8ff5a3b2b4e85785006b528a'  -- BusRode (Bus Passenger Ticket)
  )
  AND (
    {% if pool_contract_addresses and pool_contract_addresses | length > 0 %}
      LOWER(l_oft.address) IN (
        {% for addr in pool_contract_addresses %}
          LOWER('{{ addr }}'){% if not loop.last %}, {% endif %}
        {% endfor %}
      )
      OR
    {% endif %}
    LOWER(l_oft.address) = LOWER('{{token_messaging_contract}}')
  )

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l_oft.transaction_hash, COALESCE(l_oft.topic1, SUBSTR(l_oft.data, 3 + 64*3, 64))
  ORDER BY l_oft.block_timestamp DESC
) = 1

{% endmacro %}
