-- =====================================================================
-- gold.daily_sales_summary
-- Grain : one row per (order date, channel)
-- Source: silver.fact_order_items  (date_key joins to gold.dim_date)
--
-- Revenue treatment (decision: Option A)
--   * "Valid" orders = order_status NOT IN ('CANCELLED','RETURNED')
--   * total_orders, total_items_sold, gross_revenue, total_discount,
--     total_tax, net_revenue  -> valid orders only
--   * cancelled_* and returned_* -> separate columns, so they never
--     disturb the actual revenue figures
--
-- All columns are additive (sums / counts), so Power BI can roll up to
-- month / year / all channels with SUM. Do NOT store averages here:
--   average order value = SUM(net_revenue) / SUM(total_orders)
--
-- total_orders uses COUNT(DISTINCT order_id) because the source grain
-- is one row per line item. channel belongs to the order, so an order
-- lands in exactly one channel and per-channel counts sum correctly.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
    date_key           INT            NOT NULL REFERENCES gold.dim_date (date_key),
    order_date         DATE           NOT NULL,
    channel            VARCHAR(50)    NOT NULL,

    -- valid orders (not cancelled / returned)
    total_orders       INT            NOT NULL,
    total_items_sold   BIGINT         NOT NULL,
    gross_revenue      DECIMAL(16,2)  NOT NULL,   -- SUM(line_taxable)
    total_discount     DECIMAL(16,2)  NOT NULL,   -- SUM(discount_line)
    total_tax          DECIMAL(16,2)  NOT NULL,   -- SUM(gst_line_total)
    net_revenue        DECIMAL(16,2)  NOT NULL,   -- SUM(line_total)

    -- kept separate for their own charts
    cancelled_orders   INT            NOT NULL,
    cancelled_revenue  DECIMAL(16,2)  NOT NULL,   -- SUM(line_total) of CANCELLED
    returned_orders    INT            NOT NULL,
    returned_revenue   DECIMAL(16,2)  NOT NULL,   -- SUM(line_total) of RETURNED

    _loaded_at         TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, channel)
);

CREATE INDEX IF NOT EXISTS idx_daily_sales_summary_date
    ON gold.daily_sales_summary (order_date);