-- Round 4: precomputed daily summary in ClickHouse (materialized view).
-- Run once, after the load_clickhouse DAG:
--   sudo docker exec -i bench_clickhouse clickhouse-client --user bench --password bench --multiquery < db-benchmark/sql-scripts/05_clickhouse_materialized_view.sql
--
-- How it works: a ClickHouse materialized view is an INSERT trigger. Every block
-- of rows inserted into fact_order_items is aggregated and appended to the
-- target table. It does NOT see rows that were already there, so we backfill once.
-- SummingMergeTree adds up rows with the same (day, channel) key during background
-- merges, so queries still use sum() ... GROUP BY to be exact.
--
-- Re-run this file after every reload of the facts. (The DAG schema script drops
-- the view first, so a reload never double-counts.)

DROP VIEW IF EXISTS bench.daily_channel_revenue_mv;
DROP TABLE IF EXISTS bench.daily_channel_revenue;

CREATE TABLE bench.daily_channel_revenue (
    day         Date,
    channel     LowCardinality(String),
    net_revenue Decimal(38, 6),
    units       Int64,
    lines       UInt64
) ENGINE = SummingMergeTree
ORDER BY (day, channel);

CREATE MATERIALIZED VIEW bench.daily_channel_revenue_mv
TO bench.daily_channel_revenue AS
SELECT toDate(order_datetime) AS day,
       channel,
       sum(line_total)        AS net_revenue,
       sum(qty)               AS units,
       count()                AS lines
FROM bench.fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY day, channel;

-- One-time backfill of rows that were loaded before the view existed.
INSERT INTO bench.daily_channel_revenue
SELECT toDate(order_datetime) AS day,
       channel,
       sum(line_total),
       sum(qty),
       count()
FROM bench.fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY day, channel;

-- Check: the summary must add up to the same revenue as the raw table.
SELECT
    (SELECT count() FROM (SELECT day, channel FROM bench.daily_channel_revenue GROUP BY day, channel)) AS summary_rows,
    (SELECT sum(net_revenue) FROM bench.daily_channel_revenue) AS summary_revenue,
    (SELECT sum(line_total) FROM bench.fact_order_items
      WHERE order_status NOT IN ('CANCELLED', 'RETURNED')) AS raw_revenue;

