CREATE VIEW order_fill_24h_summary_view AS
WITH base_and_price AS (
  SELECT
    pool_id,
    SUM(volume_base) AS base_volume_24h,
    FIRST(open, bucket) AS price_open_24h,
    LAST(close, bucket) AS price_close_24h
  FROM ohlcv_1min
  WHERE bucket >= now() - INTERVAL '24 hours'
  GROUP BY pool_id
),
trade_count AS (
  SELECT
    pool_id,
    SUM(trade_count) AS trade_count_24h
  FROM trade_count_1min
  WHERE bucket >= now() - INTERVAL '24 hours'
  GROUP BY pool_id
)
SELECT
  b.pool_id,
  b.base_volume_24h,
  t.trade_count_24h,
  b.price_open_24h,
  b.price_close_24h
FROM base_and_price b
LEFT JOIN trade_count t ON b.pool_id = t.pool_id;