-- Drop indexes first (needed if CASCADE is not used)
DROP INDEX IF EXISTS ohlcv_15min_pool_time_idx;
DROP INDEX IF EXISTS ohlcv_1h_pool_time_idx;

-- Remove refresh policies
SELECT remove_continuous_aggregate_policy('ohlcv_15min');
SELECT remove_continuous_aggregate_policy('ohlcv_1h');

-- Drop continuous aggregates
DROP MATERIALIZED VIEW IF EXISTS ohlcv_15min;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1h;