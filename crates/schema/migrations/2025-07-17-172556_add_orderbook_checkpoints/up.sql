CREATE TABLE IF NOT EXISTS
  orderbook_snapshots (
    checkpoint BIGINT NOT NULL,
    pool_id TEXT NOT NULL,
    asks JSONB NOT NULL,
    bids JSONB NOT NULL,
    PRIMARY KEY (
      pool_id,
      checkpoint
    )
  );