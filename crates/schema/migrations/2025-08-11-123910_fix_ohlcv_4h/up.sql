-- Remove any existing refresh policy first (if present)
DO $$
BEGIN
  PERFORM remove_continuous_aggregate_policy('ohlcv_4h');
EXCEPTION WHEN undefined_function OR undefined_object THEN
  -- ignore if it didn't exist
END$$;

-- Drop the old index on the incorrect view (safe if it doesn't exist)
DROP INDEX IF EXISTS ohlcv_4h_pool_time_idx;

-- Drop the incorrect cagg definition
DROP MATERIALIZED VIEW IF EXISTS ohlcv_4h;

-- Recreate with 4-hour buckets
CREATE MATERIALIZED VIEW ohlcv_4h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('4 hours', timestamp) AS bucket,
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

-- Recreate the index for query performance
CREATE INDEX IF NOT EXISTS ohlcv_4h_pool_time_idx ON ohlcv_4h (pool_id, bucket DESC);

-- Recreate a refresh policy
SELECT add_continuous_aggregate_policy(
  'ohlcv_4h',
  start_offset => INTERVAL '12 hours',
  end_offset   => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '30 minutes'
);
