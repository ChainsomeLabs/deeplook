CREATE MATERIALIZED VIEW trade_count_1min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', timestamp) AS bucket,
  pool_id,
  COUNT(*) AS trade_count
FROM order_fills
GROUP BY bucket, pool_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
  'trade_count_1min',
  start_offset => INTERVAL '1 hour',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute'
);