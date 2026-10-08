-- =====================================================================
-- gold.customer_summary
-- Grain : one row per customer_id  (ALL customers, including never-bought)
-- Source: silver.dim_customer LEFT JOIN silver.fact_order_items
--
-- Same revenue treatment as the other Gold tables (Option A):
--   * "Valid" orders = order_status NOT IN ('CANCELLED','RETURNED')
--   * total_orders, total_items, net_revenue, first/last_order_date
--       -> valid orders only
--   * cancelled_* and returned_* -> separate columns
--
-- Customers with no orders appear with zeros and NULL dates (LEFT JOIN),
-- so "how many customers never bought" is answerable from Gold.
-- A customer whose only orders were cancelled/returned also shows
-- total_orders = 0 and NULL dates, but has non-zero cancelled/returned.
--
-- Additive: orders belong to exactly one customer, so every measure here
-- can be SUMmed across customers (e.g. by city in Power BI).
-- Stored as sums/counts/dates only. Compute in BI, do not store:
--   average order value = SUM(net_revenue) / SUM(total_orders)
--   days since last order = today - last_order_date   (goes stale if stored)
--
-- pan, gstin, email, phone are intentionally NOT copied into Gold.
-- They remain in silver.dim_customer.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.customer_summary (
    customer_id        BIGINT         PRIMARY KEY,
    full_name          VARCHAR(200),

    -- valid orders (not cancelled / returned)
    total_orders       INT            NOT NULL,
    total_items        BIGINT         NOT NULL,   -- SUM(qty)
    net_revenue        DECIMAL(16,2)  NOT NULL,   -- SUM(line_total), lifetime value
    first_order_date   DATE,                      -- NULL if no valid order
    last_order_date    DATE,                      -- NULL if no valid order

    -- kept separate for their own charts
    cancelled_orders   INT            NOT NULL,
    cancelled_revenue  DECIMAL(16,2)  NOT NULL,
    returned_orders    INT            NOT NULL,
    returned_revenue   DECIMAL(16,2)  NOT NULL,

    _loaded_at         TIMESTAMP      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_summary_last_order
    ON gold.customer_summary (last_order_date);