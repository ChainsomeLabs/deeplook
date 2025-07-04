-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Add checkpoint_ts column and backfill
ALTER TABLE order_fills
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE order_fills
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE order_updates
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE order_updates
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE proposals
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE proposals
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE rebates
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE rebates
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE stakes
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE stakes
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE trade_params_update
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE trade_params_update
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE votes
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE votes
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE balances
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE balances
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE flashloans
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE flashloans
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

ALTER TABLE pool_prices
ADD COLUMN IF NOT EXISTS checkpoint_ts TIMESTAMPTZ;

UPDATE pool_prices
SET
  checkpoint_ts = TO_TIMESTAMP(checkpoint_timestamp_ms::DOUBLE PRECISION / 1000)
WHERE
  checkpoint_ts IS NULL;

-- Migrate order_fills
ALTER TABLE order_fills
RENAME TO order_fills_old;

CREATE TABLE order_fills AS TABLE order_fills_old
WITH
  NO DATA;

ALTER TABLE order_fills
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'order_fills',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  order_fills
SELECT
  *
FROM
  order_fills_old;

ALTER TABLE order_fills
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('order_fills', INTERVAL '7 days');

-- Migrate order_updates
ALTER TABLE order_updates
RENAME TO order_updates_old;

CREATE TABLE order_updates AS TABLE order_updates_old
WITH
  NO DATA;

ALTER TABLE order_updates
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'order_updates',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  order_updates
SELECT
  *
FROM
  order_updates_old;

ALTER TABLE order_updates
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('order_updates', INTERVAL '7 days');

-- Migrate proposals
ALTER TABLE proposals
RENAME TO proposals_old;

CREATE TABLE proposals AS TABLE proposals_old
WITH
  NO DATA;

ALTER TABLE proposals
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'proposals',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  proposals
SELECT
  *
FROM
  proposals_old;

ALTER TABLE proposals
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('proposals', INTERVAL '7 days');

-- Migrate rebates
ALTER TABLE rebates
RENAME TO rebates_old;

CREATE TABLE rebates AS TABLE rebates_old
WITH
  NO DATA;

ALTER TABLE rebates
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'rebates',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  rebates
SELECT
  *
FROM
  rebates_old;

ALTER TABLE rebates
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('rebates', INTERVAL '7 days');

-- Migrate stakes
ALTER TABLE stakes
RENAME TO stakes_old;

CREATE TABLE stakes AS TABLE stakes_old
WITH
  NO DATA;

ALTER TABLE stakes
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'stakes',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  stakes
SELECT
  *
FROM
  stakes_old;

ALTER TABLE stakes
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('stakes', INTERVAL '7 days');

-- Migrate trade_params_update
ALTER TABLE trade_params_update
RENAME TO trade_params_update_old;

CREATE TABLE trade_params_update AS TABLE trade_params_update_old
WITH
  NO DATA;

ALTER TABLE trade_params_update
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'trade_params_update',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  trade_params_update
SELECT
  *
FROM
  trade_params_update_old;

ALTER TABLE trade_params_update
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('trade_params_update', INTERVAL '7 days');

-- Migrate votes
ALTER TABLE votes
RENAME TO votes_old;

CREATE TABLE votes AS TABLE votes_old
WITH
  NO DATA;

ALTER TABLE votes
ADD PRIMARY KEY (event_digest, checkpoint_ts, pool_id);

SELECT
  create_hypertable (
    'votes',
    'checkpoint_ts',
    'pool_id',
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  votes
SELECT
  *
FROM
  votes_old;

ALTER TABLE votes
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('votes', INTERVAL '7 days');

-- Migrate balances
ALTER TABLE balances
RENAME TO balances_old;

CREATE TABLE balances AS TABLE balances_old
WITH
  NO DATA;

ALTER TABLE balances
ADD PRIMARY KEY (event_digest, checkpoint_ts);

SELECT
  create_hypertable (
    'balances',
    'checkpoint_ts',
    NULL,
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  balances
SELECT
  *
FROM
  balances_old;

ALTER TABLE balances
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('balances', INTERVAL '7 days');

-- Migrate flashloans
ALTER TABLE flashloans
RENAME TO flashloans_old;

CREATE TABLE flashloans AS TABLE flashloans_old
WITH
  NO DATA;

ALTER TABLE flashloans
ADD PRIMARY KEY (event_digest, checkpoint_ts);

SELECT
  create_hypertable (
    'flashloans',
    'checkpoint_ts',
    NULL,
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  flashloans
SELECT
  *
FROM
  flashloans_old;

ALTER TABLE flashloans
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('flashloans', INTERVAL '7 days');

-- Migrate pool_prices
ALTER TABLE pool_prices
RENAME TO pool_prices_old;

CREATE TABLE pool_prices AS TABLE pool_prices_old
WITH
  NO DATA;

ALTER TABLE pool_prices
ADD PRIMARY KEY (event_digest, checkpoint_ts);

SELECT
  create_hypertable (
    'pool_prices',
    'checkpoint_ts',
    NULL,
    number_partitions => 8,
    migrate_data => false,
    if_not_exists => true
  );

INSERT INTO
  pool_prices
SELECT
  *
FROM
  pool_prices_old;

ALTER TABLE pool_prices
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('pool_prices', INTERVAL '7 days');

-- Recreate ohlcv_1min materialized view
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1min;

CREATE MATERIALIZED VIEW ohlcv_1min
WITH
  (timescaledb.continuous) AS
SELECT
  time_bucket ('1 minute', checkpoint_ts) AS bucket,
  pool_id,
  FIRST (price, checkpoint_ts) AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  LAST (price, checkpoint_ts) AS
close,
SUM(base_quantity) AS volume_base,
SUM(quote_quantity) AS volume_quote
FROM
  order_fills
GROUP BY
  bucket,
  pool_id
WITH
  NO DATA;

SELECT
  add_continuous_aggregate_policy (
    'ohlcv_1min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => '1 minute'
  );

-- DROP old tables
DROP TABLE order_fills_old;

DROP TABLE order_updates_old;

DROP TABLE proposals_old;

DROP TABLE rebates_old;

DROP TABLE stakes_old;

DROP TABLE trade_params_update_old;

DROP TABLE votes_old;

DROP TABLE balances_old;

DROP TABLE flashloans_old;

DROP TABLE pool_prices_old;

-- recalculate ohlcv_1min 
CALL refresh_continuous_aggregate ('ohlcv_1min', NULL, NULL);
