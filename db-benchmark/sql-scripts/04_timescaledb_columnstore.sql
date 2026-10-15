-- Round 3: columnar storage ("columnstore") for the TimescaleDB hypertables.
-- Run once, after 03_timescaledb_order_id_index.sql:
--   time sudo docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/04_timescaledb_columnstore.sql
--
-- Syntax follows the current TimescaleDB docs (ALTER TABLE ... enable_columnstore,
-- CALL convert_to_columnstore). NOTE: DROP/recreate by the load_timescale DAG
-- removes these settings, so re-run this file after every reload.

-- segmentby = a low-cardinality column that queries filter or group on; rows are
--             stored together per value, which compresses well and lets whole
--             groups be skipped.
-- orderby   = the sort order inside each group (time, newest first).

ALTER TABLE fact_order_items SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'channel',
    timescaledb.orderby   = 'order_datetime DESC'
);

ALTER TABLE fact_cart_items SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'cart_status',
    timescaledb.orderby   = 'cart_created_at DESC'
);

-- Convert every existing monthly chunk to columnar form now (instead of waiting
-- for a background policy). \gexec runs each generated CALL as its own statement.
SELECT format('CALL convert_to_columnstore(%L);', c) FROM show_chunks('fact_order_items') AS c \gexec
SELECT format('CALL convert_to_columnstore(%L);', c) FROM show_chunks('fact_cart_items')  AS c \gexec

ANALYZE fact_order_items;
ANALYZE fact_cart_items;

-- Size on disk after conversion (before: 252 MB and 940 MB; ClickHouse: 74 MB and 277 MB).
SELECT 'fact_order_items' AS table_name, pg_size_pretty(hypertable_size('fact_order_items')) AS size
UNION ALL
SELECT 'fact_cart_items',                pg_size_pretty(hypertable_size('fact_cart_items'));