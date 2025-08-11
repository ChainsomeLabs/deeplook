-- 15-minute OHLCV from raw trades
CREATE MATERIALIZED VIEW ohlcv_15min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('15 minutes', timestamp) AS bucket,
  pool_id,
  FIRST(price, timestamp) AS open,
  MAX(price)              AS high,
  MIN(price)              AS low,
  LAST(price, timestamp)  AS close,
  SUM(base_quantity)      AS volume_base,
  SUM(quote_quantity)     AS volume_quote
FROM order_fills
GROUP BY bucket, pool_id
WITH NO DATA;

-- 1-hour OHLCV from raw trades
CREATE MATERIALIZED VIEW ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', timestamp) AS bucket,
  pool_id,
  FIRST(price, timestamp) AS open,
  MAX(price)              AS high,
  MIN(price)              AS low,
  LAST(price, timestamp)  AS close,
  SUM(base_quantity)      AS volume_base,
  SUM(quote_quantity)     AS volume_quote
FROM order_fills
GROUP BY bucket, pool_id
WITH NO DATA;

-- 4-hour OHLCV from raw trades
CREATE MATERIALIZED VIEW ohlcv_4h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', timestamp) AS bucket,
  pool_id,
  FIRST(price, timestamp) AS open,
  MAX(price)              AS high,
  MIN(price)              AS low,
  LAST(price, timestamp)  AS close,
  SUM(base_quantity)      AS volume_base,
  SUM(quote_quantity)     AS volume_quote
FROM order_fills
GROUP BY bucket, pool_id
WITH NO DATA;

-- Refresh policies (tune to your data lateness + query patterns)
SELECT add_continuous_aggregate_policy(
  'ohlcv_15min',
  start_offset => INTERVAL '1 hour',      -- re-process to capture late trades
  end_offset   => INTERVAL '2 minutes',   -- leave a small buffer for current window
  schedule_interval => INTERVAL '1 minute'
);

SELECT add_continuous_aggregate_policy(
  'ohlcv_1h',
  start_offset => INTERVAL '4 hours',
  end_offset   => INTERVAL '2 minutes',
  schedule_interval => INTERVAL '5 minutes'
);

SELECT add_continuous_aggregate_policy(
  'ohlcv_4h',
  start_offset => INTERVAL '4 hours',
  end_offset   => INTERVAL '2 minutes',
  schedule_interval => INTERVAL '5 minutes'
);

-- Helpful indexes on the materialized views (applied to their mat hypertables)
CREATE INDEX IF NOT EXISTS ohlcv_15min_pool_time_idx ON ohlcv_15min (pool_id, bucket);
CREATE INDEX IF NOT EXISTS ohlcv_1h_pool_time_idx    ON ohlcv_1h    (pool_id, bucket);
CREATE INDEX IF NOT EXISTS ohlcv_4h_pool_time_idx    ON ohlcv_4h    (pool_id, bucket);

-- Immediately populate the aggregates
CALL refresh_continuous_aggregate('ohlcv_15min', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcv_1h', NULL, NULL);
CALL refresh_continuous_aggregate('ohlcv_4h', NULL, NULL);
