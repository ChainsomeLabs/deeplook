-- Drop continuous aggregate and its policy
SELECT
  remove_continuous_aggregate_policy('ohlcv_1min');

DROP MATERIALIZED VIEW IF EXISTS ohlcv_1min;

-- Remove compression policies and disable compression for each table
SELECT
  remove_compression_policy('order_fills');

ALTER TABLE
  order_fills RESET (timescaledb.compress);

SELECT
  remove_compression_policy('order_updates');

ALTER TABLE
  order_updates RESET (timescaledb.compress);

SELECT
  remove_compression_policy('balances');

ALTER TABLE
  balances RESET (timescaledb.compress);

SELECT
  remove_compression_policy('flashloans');

ALTER TABLE
  flashloans RESET (timescaledb.compress);

SELECT
  remove_compression_policy('pool_prices');

ALTER TABLE
  pool_prices RESET (timescaledb.compress);

SELECT
  remove_compression_policy('proposals');

ALTER TABLE
  proposals RESET (timescaledb.compress);

SELECT
  remove_compression_policy('rebates');

ALTER TABLE
  rebates RESET (timescaledb.compress);

SELECT
  remove_compression_policy('rebates_v2');

ALTER TABLE
  rebates_v2 RESET (timescaledb.compress);

SELECT
  remove_compression_policy('stakes');

ALTER TABLE
  stakes RESET (timescaledb.compress);

SELECT
  remove_compression_policy('trade_params_update');

ALTER TABLE
  trade_params_update RESET (timescaledb.compress);

SELECT
  remove_compression_policy('votes');

ALTER TABLE
  votes RESET (timescaledb.compress);

DROP EXTENSION IF EXISTS timescaledb;