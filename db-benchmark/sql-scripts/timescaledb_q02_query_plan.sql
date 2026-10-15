-- Show how TimescaleDB executes Q02 (monthly revenue) in its current state.
-- Run:
--   sudo docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/timescaledb_q02_query_plan.sql > db-benchmark/results/round3_q02_query_plan_columnstore.txt

-- Warm-up run (result discarded) so the plan below reflects a warm cache.
\o /dev/null
SELECT date_trunc('month', order_datetime)::date AS month,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY 1
ORDER BY 1;
\o

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON)
SELECT date_trunc('month', order_datetime)::date AS month,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY 1
ORDER BY 1;