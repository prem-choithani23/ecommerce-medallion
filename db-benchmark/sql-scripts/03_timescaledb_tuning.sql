-- Round 2 tuning for TimescaleDB (benchmark database "bench").
-- Run once:  sudo docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/03_timescaledb_tuning.sql

-- 1. Point lookups (Q10) filter on order_id. Without an index Postgres must
--    scan the whole table; with one it can jump straight to the rows.
--    On a hypertable this creates one index per monthly chunk.
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id
    ON fact_order_items (order_id);

-- 2. Larger working memory for sorts, hashes and aggregations. The default
--    (4MB) makes big GROUP BYs, like the cart-collapsing queries Q05 and Q06,
--    spill to disk. Applies to every NEW connection to this database.
ALTER DATABASE bench SET work_mem = '256MB';

-- 3. Refresh planner statistics so the new index is used sensibly.
ANALYZE fact_order_items;