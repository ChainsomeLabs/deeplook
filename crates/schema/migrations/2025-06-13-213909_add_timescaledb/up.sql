-- Add timescaledb
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- order_fills
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

CREATE MATERIALIZED VIEW ohlcv_1min
WITH
  (timescaledb.continuous) AS
SELECT
  time_bucket ('1 minute', timestamp) AS bucket,
  pool_id,
  FIRST (price, timestamp) as open,
  MAX(price) as high,
  MIN(price) as low,
  LAST (price, timestamp) as
close,
SUM(base_quantity) as volume_base,
SUM(quote_quantity) as volume_quote
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

-- order_updates
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

-- balances
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

-- flashloans
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

-- pool_prices
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

-- proposals
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

-- rebates
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

-- stakes
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

-- trade_params_update
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

-- votes
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
