{% macro hex_to_bignumeric(hex_expr) %}
    SAFE_CAST(
      IF(
        {{ hex_expr }} IS NULL OR CHAR_LENGTH(TRIM({{ hex_expr }})) = 0,
        NULL,
        IF(
          STARTS_WITH(LOWER(TRIM({{ hex_expr }})), '0x'),
          TRIM({{ hex_expr }}),
          CONCAT('0x', TRIM({{ hex_expr }}))
        )
      ) AS BIGNUMERIC
    )
{% endmacro %}
