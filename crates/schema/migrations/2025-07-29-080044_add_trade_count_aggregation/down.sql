-- First remove the continuous aggregate policy
SELECT remove_continuous_aggregate_policy('trade_count_1min');

-- Then drop the materialized view
DROP MATERIALIZED VIEW IF EXISTS trade_count_1min;