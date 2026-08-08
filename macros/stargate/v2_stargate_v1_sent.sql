{% macro v2_stargate_v1_sent(blockchain, base_events, source_logs, source_txs, bridge_address, chain_id) %}

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
  {{chain_id}} AS source_chain_id,
  l1.block_timestamp AS block_time,
  l1.block_number,
  l1.transaction_hash AS tx_hash,
  CASE l1.topic0
      WHEN '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f' THEN 'Swap'
      WHEN '0xa33f5c0b76f00f6737b1780a8a7f18e19c3fe8fe9ee01a6c1b8ce1eae5ed54f9' THEN 'RedeemRemote'
      WHEN '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2' THEN 'RedeemLocal'
      WHEN '0x6939f93e3f21cf1362eb17155b740277de5687dae9a83a85909fd71da95944e7' THEN 'SendCredits'
      WHEN '0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b' THEN 'SendToChain'
  END AS event_type,
  tx.from_address AS depositor,
  l1.address AS pool_address,
  COALESCE(
      l_token_sent.address,
      l1.address,
      '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
  ) AS token_sent_address,
  CASE l1.topic0
      WHEN '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2' THEN CAST(('0x' || SUBSTR(l1.data, 3 + 64*3, 64)) AS INT64)
      ELSE CAST(('0x' || SUBSTR(l1.data, 3 + 64*0, 64)) AS INT64)
  END AS destination_chain_id,
  CASE l1.topic0
      WHEN '0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b' THEN NULL
      WHEN '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2' THEN CAST(('0x' || SUBSTR(l1.data, 3 + 64*4, 64)) AS INT64)
      ELSE CAST(('0x' || SUBSTR(l1.data, 3 + 64*1, 64)) AS INT64)
  END AS destination_pool_id,
  CASE l1.topic0
      WHEN '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f' THEN {{ hex_to_bignumeric("SUBSTR(l1.data, 3 + 64*3, 64)") }}
      WHEN '0xa33f5c0b76f00f6737b1780a8a7f18e19c3fe8fe9ee01a6c1b8ce1eae5ed54f9' THEN {{ hex_to_bignumeric("SUBSTR(l1.data, 3 + 64*4, 64)") }}
      WHEN '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2' THEN {{ hex_to_bignumeric("SUBSTR(l1.data, 3 + 64*2, 64)") }}
      WHEN '0x6939f93e3f21cf1362eb17155b740277de5687dae9a83a85909fd71da95944e7' THEN {{ hex_to_bignumeric("SUBSTR(l1.data, 3 + 64*2, 64)") }}
      WHEN '0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b' THEN {{ hex_to_bignumeric("SUBSTR(l1.data, 3 + 64*2, 64)") }}
  END AS amount_sent_sd,
  '{{bridge_address}}' AS source_bridge_address,
  COALESCE(
    CAST(('0x' || SUBSTR(l2.data, 3 + 64*2, 16)) AS INT64),
    0
  ) AS nonce

FROM {{ base_events }} AS l1

INNER JOIN {{ source_txs }} AS tx
    ON l1.transaction_hash = tx.transaction_hash
   AND tx.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')

LEFT JOIN {{ source_logs }} AS l2 
    ON l1.transaction_hash = l2.transaction_hash
   AND l2.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l2.topics[SAFE_OFFSET(0)] IN (
      '0x8d3ee0df6a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a',
      '0xe9bded5f24a4168e4f3bf44e00298c993b22376aad8c58c7dda9718a54cbea82'
   )

LEFT JOIN {{ source_logs }} AS l_token_sent
    ON l1.transaction_hash = l_token_sent.transaction_hash
   AND l_token_sent.block_timestamp BETWEEN TIMESTAMP('{{ min_ts }}') AND TIMESTAMP('{{ max_ts }}')
   AND l_token_sent.topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

WHERE l1.topic0 IN (
      '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
      '0xa33f5c0b76f00f6737b1780a8a7f18e19c3fe8fe9ee01a6c1b8ce1eae5ed54f9',
      '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2',
      '0x6939f93e3f21cf1362eb17155b740277de5687dae9a83a85909fd71da95944e7',
      '0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b'
  )

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY l1.transaction_hash, COALESCE(CAST(('0x' || SUBSTR(l2.data, 3 + 64*2, 16)) AS INT64), 0)
    ORDER BY CASE l1.topic0
        WHEN '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f' THEN 1
        WHEN '0xa33f5c0b76f00f6737b1780a8a7f18e19c3fe8fe9ee01a6c1b8ce1eae5ed54f9' THEN 2
        WHEN '0x53c03ee0722b52efeb42444f48d90173854501b3de3c590fcb445743377115c2' THEN 3
        WHEN '0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b' THEN 4
        ELSE 5
    END
) = 1

{% endmacro %}
