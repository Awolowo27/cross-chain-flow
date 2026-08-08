{% macro v2_stargate_v1_received(blockchain, base_events, source_logs, source_txs, bridge_address, chain_id) %}

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
  {{chain_id}} AS destination_chain_id,
  l_pkt.block_timestamp AS block_time,
  l_pkt.block_number,
  l_pkt.transaction_hash AS tx_hash,
  CASE 
      WHEN l_recv.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'Receive'
      WHEN l_oft_recv.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'ReceiveFromChain'
      WHEN l_recv_token.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'ReceiveToken'
      WHEN l_swap.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'SwapRemote'
      WHEN l_redeem_cb.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'RedeemRemoteCallback'
      WHEN l_local_cb.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 'RedeemLocalCallback'
  END AS event_type,
  '{{bridge_address}}' AS destination_bridge_address,
  CAST(('0x' || SUBSTR(l_pkt.topics[SAFE_OFFSET(1)], 27)) AS INT64) AS source_chain_id,
  CONCAT('0x', SUBSTR(l_pkt.data, 3 + 64*4, 40)) AS source_bridge_address,
  CAST(('0x' || SUBSTR(l_pkt.data, 3 + 64*1, 64)) AS INT64) AS nonce,
  COALESCE(
    CONCAT('0x', SUBSTR(l_recv.topics[SAFE_OFFSET(2)], 27)),
    CONCAT('0x', SUBSTR(l_oft_recv.topics[SAFE_OFFSET(2)], 27)),
    CONCAT('0x', SUBSTR(l_token_recv.topics[SAFE_OFFSET(2)], 27)),
    CONCAT('0x', SUBSTR(l_recv_token.data, 3 + 64*1 + 24, 40)),
    CONCAT('0x', SUBSTR(l_swap.data, 3 + 64*0 + 24, 40)),
    CONCAT('0x', SUBSTR(l_redeem_cb.data, 3 + 64*2 + 24, 40)),
    CONCAT('0x', SUBSTR(l_local_cb.data, 3 + 64*3 + 24, 40))
  ) AS recipient,
  COALESCE(l_recv.address, l_oft_recv.address, l_recv_token.address, l_swap.address, l_redeem_cb.address, l_local_cb.address) AS pool_address,
  COALESCE(
    CONCAT('0x', SUBSTR(l_recv.topics[SAFE_OFFSET(1)], 27)),
    l_oft_recv.address,
    CONCAT('0x', SUBSTR(l_recv_token.data, 3 + 64*0 + 24, 40)),
    l_token_recv.address,
    COALESCE(l_swap.address, l_redeem_cb.address, l_local_cb.address),
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_received_address,
  COALESCE(
    {{ hex_to_bignumeric("SUBSTR(l_recv.data, 3 + 64*0, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_oft_recv.data, 3 + 64*0, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_oft_recv.data, 3 + 64*2, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_recv_token.data, 3 + 64*2, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_swap.data, 3 + 64*1, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_redeem_cb.data, 3 + 64*3, 64)") }},
    {{ hex_to_bignumeric("SUBSTR(l_local_cb.data, 3 + 64*4, 64)") }}
  ) AS amount_received_sd

FROM {{ base_events }} AS l_pkt

LEFT JOIN {{ source_logs }} AS l_recv
    ON l_pkt.transaction_hash = l_recv.transaction_hash
   AND l_recv.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_recv.topics[SAFE_OFFSET(0)] = '0xfd19781f43410d9594fd4c02dd1d98dbe99099cbd222d5851a6e183c468d33ca'

LEFT JOIN {{ source_logs }} AS l_oft_recv
    ON l_pkt.transaction_hash = l_oft_recv.transaction_hash
   AND l_oft_recv.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_oft_recv.topics[SAFE_OFFSET(0)] IN (
      '0xbf551ec93859b170f9b2141bd9298bf3f64322c6f7beb2543a0cb669834118bf',
      '0x831bc68226f8d1f734ffcca73602efc4eca13711402ba1d2cc05ee17bb54f631'
   )

LEFT JOIN {{ source_logs }} AS l_recv_token
    ON l_pkt.transaction_hash = l_recv_token.transaction_hash
   AND l_recv_token.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_recv_token.topics[SAFE_OFFSET(0)] = '0x5e3da8fba24af91505c66214c9e629ba712ce2c1b8c318f14f7024fdcba544a8'

LEFT JOIN {{ source_logs }} AS l_swap
    ON l_pkt.transaction_hash = l_swap.transaction_hash
   AND l_swap.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_swap.topics[SAFE_OFFSET(0)] = '0xfb2b592367452f1c437675bed47f5e1e6c25188c17d7ba01a12eb030bc41ccef'

LEFT JOIN {{ source_logs }} AS l_redeem_cb
    ON l_pkt.transaction_hash = l_redeem_cb.transaction_hash
   AND l_redeem_cb.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_redeem_cb.topics[SAFE_OFFSET(0)] = '0x17b3f9b2d8ff559f9a464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c'

LEFT JOIN {{ source_logs }} AS l_local_cb
    ON l_pkt.transaction_hash = l_local_cb.transaction_hash
   AND l_local_cb.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_local_cb.topics[SAFE_OFFSET(0)] = '0xc7379a02e530fbd0a46ea1ce6fd91987e96535798231a796bdc0e1a688a50873'

LEFT JOIN {{ source_logs }} AS l_token_recv
    ON l_pkt.transaction_hash = l_token_recv.transaction_hash
   AND l_token_recv.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_token_recv.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

WHERE l_pkt.topic0 = '0x2bd2d8a84b748439fd50d79a49502b4eb5faa25b864da6a9ab5c150704be9a4d'
  AND (
     l_recv.topics[SAFE_OFFSET(0)] IS NOT NULL
  OR l_oft_recv.topics[SAFE_OFFSET(0)] IS NOT NULL
  OR l_recv_token.topics[SAFE_OFFSET(0)] IS NOT NULL
  OR l_swap.topics[SAFE_OFFSET(0)] IS NOT NULL
  OR l_redeem_cb.topics[SAFE_OFFSET(0)] IS NOT NULL
  OR l_local_cb.topics[SAFE_OFFSET(0)] IS NOT NULL
  )

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY l_pkt.transaction_hash, CAST(('0x' || SUBSTR(l_pkt.data, 3 + 64*1, 64)) AS INT64)
    ORDER BY CASE 
        WHEN l_recv.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 1
        WHEN l_oft_recv.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 2
        WHEN l_recv_token.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 3
        WHEN l_swap.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 4
        WHEN l_redeem_cb.topics[SAFE_OFFSET(0)] IS NOT NULL THEN 5
        ELSE 6
    END
) = 1

{% endmacro %}
