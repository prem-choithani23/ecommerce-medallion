"""
Analytical queries for the TimescaleDB vs ClickHouse comparison.

Every query exists in two dialects ("pg" for TimescaleDB, "ch" for ClickHouse)
and returns the SAME rows on both engines. Where the SQL is identical the
dialects share one string. The two tables in the TimescaleDB schema "bench" and
the ClickHouse database "bench" have the same names, so queries are unqualified.

Tables (loaded from Silver by the two loader DAGs):
    fact_order_items  fact_payments  fact_cart_items  dim_product  dim_customer

"Valid order" = order_status NOT IN ('CANCELLED', 'RETURNED'), the same rule
used by the Gold layer.

Query kinds (used in the written conclusion):
    scan      wide aggregation over many rows
    join      fact joined to a dimension
    distinct  COUNT(DISTINCT ...)
    highcard  GROUP BY a high-cardinality column
    time      time-range filter / time bucketing
    point     look up one order (OLTP-style)
"""

VALID = "order_status NOT IN ('CANCELLED', 'RETURNED')"

# SQL that is identical on both engines -----------------------------------

_Q1 = f"""
SELECT channel,
       COUNT(DISTINCT order_id) AS orders,
       SUM(qty)                 AS units,
       SUM(line_total)          AS net_revenue
FROM fact_order_items
WHERE {VALID}
GROUP BY channel
ORDER BY channel
"""

_Q3 = f"""
SELECT d.product_name,
       SUM(f.qty)        AS units,
       SUM(f.line_total) AS net_revenue
FROM fact_order_items f
JOIN dim_product d ON d.variant_id = f.variant_id
WHERE f.{VALID}
GROUP BY d.product_name
ORDER BY net_revenue DESC, d.product_name
LIMIT 10
"""

_Q7 = """
SELECT payment_method,
       COUNT(*)                                                   AS payments,
       SUM(CASE WHEN payment_status = 'SUCCESS'  THEN 1 ELSE 0 END) AS success,
       SUM(CASE WHEN payment_status = 'FAILED'   THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN payment_status = 'PENDING'  THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN payment_status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded,
       SUM(amount)                                                AS total_amount
FROM fact_payments
GROUP BY payment_method
ORDER BY payment_method
"""

_Q9 = f"""
SELECT customer_id,
       SUM(line_total)          AS lifetime_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE {VALID}
GROUP BY customer_id
ORDER BY lifetime_revenue DESC, customer_id
LIMIT 100
"""

_Q10 = """
SELECT order_number, order_status, channel, variant_id, qty, line_total
FROM fact_order_items
WHERE order_id = {sample_order_id}
ORDER BY variant_id
"""

_Q13 = """
SELECT cart_status,
       COUNT(*)                          AS cart_lines,
       SUM(qty)                          AS units,
       SUM(qty * unit_price - discount_amount) AS value
FROM fact_cart_items
GROUP BY cart_status
ORDER BY cart_status
"""

_CART_CTE = """
WITH cart AS (
    SELECT cart_id,
           MAX(channel)      AS channel,
           MAX(cart_status)  AS cart_status,
           MAX(cart_created_at) AS created_at,
           SUM(qty * unit_price - discount_amount) AS value
    FROM fact_cart_items
    GROUP BY cart_id
)
"""

_Q5 = _CART_CTE + """
SELECT channel,
       COUNT(*) AS carts,
       SUM(CASE WHEN cart_status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned,
       SUM(CASE WHEN cart_status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted,
       SUM(CASE WHEN cart_status = 'EXPIRED'   THEN 1 ELSE 0 END) AS expired,
       SUM(CASE WHEN cart_status = 'ABANDONED' THEN value ELSE 0 END) AS abandoned_value
FROM cart
GROUP BY channel
ORDER BY channel
"""

# Dialect-specific SQL ------------------------------------------------------

QUERIES = [
    {
        "id": "Q01", "kind": "scan",
        "name": "Revenue by channel (valid orders)",
        "pg": _Q1, "ch": _Q1,
    },
    {
        "id": "Q02", "kind": "time",
        "name": "Monthly revenue trend",
        "pg": f"""
SELECT date_trunc('month', order_datetime)::date AS month,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE {VALID}
GROUP BY 1
ORDER BY 1
""",
        "ch": f"""
SELECT toStartOfMonth(order_datetime) AS month,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE {VALID}
GROUP BY month
ORDER BY month
""",
    },
    {
        "id": "Q03", "kind": "join",
        "name": "Top 10 products by revenue",
        "pg": _Q3, "ch": _Q3,
    },
    {
        "id": "Q04", "kind": "join",
        "name": "Category revenue by month",
        "pg": f"""
SELECT date_trunc('month', f.order_datetime)::date AS month,
       d.category_name,
       SUM(f.line_total) AS net_revenue
FROM fact_order_items f
JOIN dim_product d ON d.variant_id = f.variant_id
WHERE f.{VALID}
GROUP BY 1, 2
ORDER BY 1, 2
""",
        "ch": f"""
SELECT toStartOfMonth(f.order_datetime) AS month,
       d.category_name AS category_name,
       SUM(f.line_total) AS net_revenue
FROM fact_order_items f
JOIN dim_product d ON d.variant_id = f.variant_id
WHERE f.{VALID}
GROUP BY month, category_name
ORDER BY month, category_name
""",
    },
    {
        "id": "Q05", "kind": "scan",
        "name": "Cart funnel by channel (6M cart lines collapsed to carts)",
        "pg": _Q5, "ch": _Q5,
    },
    {
        "id": "Q06", "kind": "time",
        "name": "Abandoned cart value by month",
        "pg": _CART_CTE + """
SELECT date_trunc('month', created_at)::date AS month,
       COUNT(*) AS carts,
       SUM(CASE WHEN cart_status = 'ABANDONED' THEN value ELSE 0 END) AS abandoned_value
FROM cart
GROUP BY 1
ORDER BY 1
""",
        "ch": _CART_CTE + """
SELECT toStartOfMonth(created_at) AS month,
       COUNT(*) AS carts,
       SUM(CASE WHEN cart_status = 'ABANDONED' THEN value ELSE 0 END) AS abandoned_value
FROM cart
GROUP BY month
ORDER BY month
""",
    },
    {
        "id": "Q07", "kind": "scan",
        "name": "Payment status by method",
        "pg": _Q7, "ch": _Q7,
    },
    {
        "id": "Q08", "kind": "distinct",
        "name": "Active customers per month (COUNT DISTINCT)",
        "pg": f"""
SELECT date_trunc('month', order_datetime)::date AS month,
       COUNT(DISTINCT customer_id) AS active_customers
FROM fact_order_items
WHERE {VALID}
GROUP BY 1
ORDER BY 1
""",
        "ch": f"""
SELECT toStartOfMonth(order_datetime) AS month,
       COUNT(DISTINCT customer_id) AS active_customers
FROM fact_order_items
WHERE {VALID}
GROUP BY month
ORDER BY month
""",
    },
    {
        "id": "Q09", "kind": "highcard",
        "name": "Top 100 customers by lifetime revenue",
        "pg": _Q9, "ch": _Q9,
    },
    {
        "id": "Q10", "kind": "point",
        "name": "Look up one order by order_id",
        "pg": _Q10, "ch": _Q10,
        "needs_sample_order_id": True,
    },
    {
        "id": "Q11", "kind": "time",
        "name": "Last 7 days of data by channel",
        "pg": f"""
SELECT channel,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE order_datetime >= (SELECT MAX(order_datetime) FROM fact_order_items) - INTERVAL '7 days'
  AND {VALID}
GROUP BY channel
ORDER BY channel
""",
        "ch": f"""
SELECT channel,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE order_datetime >= (SELECT MAX(order_datetime) FROM fact_order_items) - INTERVAL 7 DAY
  AND {VALID}
GROUP BY channel
ORDER BY channel
""",
    },
    {
        "id": "Q12", "kind": "time",
        "name": "Daily revenue rollup (all days)",
        "pg": f"""
SELECT order_datetime::date AS day,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE {VALID}
GROUP BY 1
ORDER BY 1
""",
        "ch": f"""
SELECT toDate(order_datetime) AS day,
       SUM(line_total) AS net_revenue,
       COUNT(DISTINCT order_id) AS orders
FROM fact_order_items
WHERE {VALID}
GROUP BY day
ORDER BY day
""",
    },
    {
        "id": "Q13", "kind": "scan",
        "name": "Full scan of 6M cart lines by status",
        "pg": _Q13, "ch": _Q13,
    },
]