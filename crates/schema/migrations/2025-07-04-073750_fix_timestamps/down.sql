-- Revert order_fills
SELECT
  delete_hypertable ('order_fills', if_exists => true);

ALTER TABLE order_fills
DROP CONSTRAINT IF EXISTS order_fills_pkey;

ALTER TABLE order_fills
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'order_fills',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE order_fills
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('order_fills', INTERVAL '7 days');

ALTER TABLE order_fills
DROP COLUMN IF EXISTS checkpoint_ts;

-- Remove continuous aggregate
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1min;

-- Recreate ohlcv_1min view using original timestamp
CREATE MATERIALIZED VIEW ohlcv_1min
WITH
  (timescaledb.continuous) AS
SELECT
  time_bucket ('1 minute', timestamp) AS bucket,
  pool_id,
  FIRST (price, timestamp) AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  LAST (price, timestamp) AS
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

-- Revert order_updates
SELECT
  delete_hypertable ('order_updates', if_exists => true);

ALTER TABLE order_updates
DROP CONSTRAINT IF EXISTS order_updates_pkey;

ALTER TABLE order_updates
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'order_updates',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE order_updates
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('order_updates', INTERVAL '7 days');

ALTER TABLE order_updates
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert balances
SELECT
  delete_hypertable ('balances', if_exists => true);

ALTER TABLE balances
DROP CONSTRAINT IF EXISTS balances_pkey;

ALTER TABLE balances
ADD PRIMARY KEY (event_digest, timestamp);

SELECT
  create_hypertable (
    'balances',
    'timestamp',
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE balances
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('balances', INTERVAL '7 days');

ALTER TABLE balances
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert flashloans
SELECT
  delete_hypertable ('flashloans', if_exists => true);

ALTER TABLE flashloans
DROP CONSTRAINT IF EXISTS flashloans_pkey;

ALTER TABLE flashloans
ADD PRIMARY KEY (event_digest, timestamp);

SELECT
  create_hypertable (
    'flashloans',
    'timestamp',
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE flashloans
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('flashloans', INTERVAL '7 days');

ALTER TABLE flashloans
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert pool_prices
SELECT
  delete_hypertable ('pool_prices', if_exists => true);

ALTER TABLE pool_prices
DROP CONSTRAINT IF EXISTS pool_prices_pkey;

ALTER TABLE pool_prices
ADD PRIMARY KEY (event_digest, timestamp);

SELECT
  create_hypertable (
    'pool_prices',
    'timestamp',
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE pool_prices
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('pool_prices', INTERVAL '7 days');

ALTER TABLE pool_prices
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert proposals
SELECT
  delete_hypertable ('proposals', if_exists => true);

ALTER TABLE proposals
DROP CONSTRAINT IF EXISTS proposals_pkey;

ALTER TABLE proposals
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'proposals',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE proposals
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('proposals', INTERVAL '7 days');

ALTER TABLE proposals
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert rebates
SELECT
  delete_hypertable ('rebates', if_exists => true);

ALTER TABLE rebates
DROP CONSTRAINT IF EXISTS rebates_pkey;

ALTER TABLE rebates
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'rebates',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE rebates
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('rebates', INTERVAL '7 days');

ALTER TABLE rebates
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert stakes
SELECT
  delete_hypertable ('stakes', if_exists => true);

ALTER TABLE stakes
DROP CONSTRAINT IF EXISTS stakes_pkey;

ALTER TABLE stakes
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'stakes',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE stakes
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('stakes', INTERVAL '7 days');

ALTER TABLE stakes
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert trade_params_update
SELECT
  delete_hypertable ('trade_params_update', if_exists => true);

ALTER TABLE trade_params_update
DROP CONSTRAINT IF EXISTS trade_params_update_pkey;

ALTER TABLE trade_params_update
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'trade_params_update',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE trade_params_update
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('trade_params_update', INTERVAL '7 days');

ALTER TABLE trade_params_update
DROP COLUMN IF EXISTS checkpoint_ts;

-- Revert votes
SELECT
  delete_hypertable ('votes', if_exists => true);

ALTER TABLE votes
DROP CONSTRAINT IF EXISTS votes_pkey;

ALTER TABLE votes
ADD PRIMARY KEY (event_digest, timestamp, pool_id);

SELECT
  create_hypertable (
    'votes',
    'timestamp',
    'pool_id',
    number_partitions => 8,
    migrate_data => true,
    if_not_exists => true
  );

ALTER TABLE votes
SET
  (timescaledb.compress = true);

SELECT
  add_compression_policy ('votes', INTERVAL '7 days');

ALTER TABLE votes
DROP COLUMN IF EXISTS checkpoint_ts;
