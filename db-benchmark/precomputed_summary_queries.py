"""Round 4 queries: raw table versus precomputed daily summary.

The same business question is asked twice on each engine:
  * from the raw fact table            (ids Q14, Q15)
  * from the precomputed daily summary (ids Q14s, Q15s)

The summary table is `daily_channel_revenue`, one row per day and channel:
  * TimescaleDB: continuous aggregate  (sql-scripts/06_timescaledb_continuous_aggregate.sql)
  * ClickHouse : materialized view     (sql-scripts/05_clickhouse_materialized_view.sql)

A summary query must return exactly the same rows as its raw-table twin; the
runner checks this (see SUMMARY_PAIRS). Each query exists in two SQL dialects,
"pg" for TimescaleDB and "ch" for ClickHouse.
"""
from analytical_queries import VALID

SUMMARY_QUERIES = [
    {
        "id": "Q14", "kind": "time",
        "name": "Daily revenue by channel (from raw table)",
        "pg": f"""
SELECT order_datetime::date AS day,
       channel,
       SUM(line_total) AS net_revenue,
       SUM(qty)        AS units,
       COUNT(*)        AS lines
FROM fact_order_items
WHERE {VALID}
GROUP BY 1, 2
ORDER BY 1, 2
""",
        "ch": f"""
SELECT toDate(order_datetime) AS day,
       channel,
       SUM(line_total) AS net_revenue,
       SUM(qty)        AS units,
       COUNT(*)        AS lines
FROM fact_order_items
WHERE {VALID}
GROUP BY day, channel
ORDER BY day, channel
""",
    },
    {
        "id": "Q14s", "kind": "precomputed",
        "name": "Daily revenue by channel (from summary)",
        "pg": """
SELECT day::date AS day, channel, net_revenue, units, lines
FROM daily_channel_revenue
ORDER BY 1, 2
""",
        # SummingMergeTree may hold unmerged rows, so always sum + group
        "ch": """
SELECT day, channel,
       SUM(net_revenue) AS net_revenue,
       SUM(units)       AS units,
       SUM(lines)       AS lines
FROM daily_channel_revenue
GROUP BY day, channel
ORDER BY day, channel
""",
    },
    {
        "id": "Q15", "kind": "time",
        "name": "Monthly revenue and units (from raw table)",
        "pg": f"""
SELECT date_trunc('month', order_datetime)::date AS month,
       SUM(line_total) AS net_revenue,
       SUM(qty)        AS units,
       COUNT(*)        AS lines
FROM fact_order_items
WHERE {VALID}
GROUP BY 1
ORDER BY 1
""",
        "ch": f"""
SELECT toStartOfMonth(order_datetime) AS month,
       SUM(line_total) AS net_revenue,
       SUM(qty)        AS units,
       COUNT(*)        AS lines
FROM fact_order_items
WHERE {VALID}
GROUP BY month
ORDER BY month
""",
    },
    {
        "id": "Q15s", "kind": "precomputed",
        "name": "Monthly revenue and units (from summary)",
        "pg": """
SELECT date_trunc('month', day)::date AS month,
       SUM(net_revenue)      AS net_revenue,
       SUM(units)::bigint    AS units,
       SUM(lines)::bigint    AS lines
FROM daily_channel_revenue
GROUP BY 1
ORDER BY 1
""",
        "ch": """
SELECT toStartOfMonth(day) AS month,
       SUM(net_revenue)    AS net_revenue,
       SUM(units)          AS units,
       SUM(lines)          AS lines
FROM daily_channel_revenue
GROUP BY month
ORDER BY month
""",
    },
]

# (raw-table query, summary query) pairs that must return identical rows.
SUMMARY_PAIRS = [("Q14", "Q14s"), ("Q15", "Q15s")]