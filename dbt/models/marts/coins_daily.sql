SELECT
    coin_id,
    ingestion_date,
    AVG(current_price) AS avg_price,
    MAX(market_cap) AS max_market_cap,
    SUM(total_volume) AS total_volume
FROM {{ ref('stg_coins') }}
GROUP BY coin_id, ingestion_date
