-- Round 4: precomputed daily summary in TimescaleDB (continuous aggregate).
-- Run once, after 03 and 04:
--   sudo docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/06_timescaledb_continuous_aggregate.sql
--
-- A continuous aggregate is a materialized view that TimescaleDB keeps up to date
-- from the hypertable. It cannot contain COUNT(DISTINCT ...), so the summary stores
-- revenue, units and line counts only. Syntax follows the current TimescaleDB docs.
--
-- Re-run this file after every reload (the load DAG drops the hypertable).

DROP MATERIALIZED VIEW IF EXISTS daily_channel_revenue;

CREATE MATERIALIZED VIEW daily_channel_revenue
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', order_datetime) AS day,
       channel,
       SUM(line_total) AS net_revenue,
       SUM(qty)        AS units,
       COUNT(*)        AS lines
FROM fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY time_bucket('1 day', order_datetime), channel
WITH NO DATA;

-- Fill the summary from the existing data (everything up to tomorrow).
CALL refresh_continuous_aggregate('daily_channel_revenue', NULL, localtimestamp + INTERVAL '1 day');

-- Check: the summary must add up to the same revenue as the raw table.
SELECT (SELECT count(*) FROM daily_channel_revenue)        AS summary_rows,
       (SELECT sum(net_revenue) FROM daily_channel_revenue) AS summary_revenue,
       (SELECT sum(line_total) FROM fact_order_items
         WHERE order_status NOT IN ('CANCELLED', 'RETURNED')) AS raw_revenue;