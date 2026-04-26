WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY coin_id
               ORDER BY ingestion_ts DESC
           ) AS rn
    FROM {{ ref('stg_coins') }}
)

SELECT
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    ingestion_ts
FROM ranked
WHERE rn = 1