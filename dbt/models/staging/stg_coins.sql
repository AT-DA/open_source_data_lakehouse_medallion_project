SELECT
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    ingestion_ts,
    DATE(ingestion_ts) AS ingestion_date
FROM {{ source('silver', 'coins_markets') }}
