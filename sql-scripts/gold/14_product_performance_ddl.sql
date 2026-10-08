-- =====================================================================
-- gold.product_performance
-- Grain : one row per (order date, variant_id)
-- Source: silver.fact_order_items LEFT JOIN silver.dim_product (on variant_id)
--
-- Same revenue treatment as gold.daily_sales_summary (Option A):
--   * "Valid" orders = order_status NOT IN ('CANCELLED','RETURNED')
--   * units_sold, total_orders, gross_revenue, total_discount, total_tax,
--     net_revenue -> valid orders only
--   * cancelled_* and returned_* -> separate columns
--
-- Variant attributes (size / color) are deliberately NOT in this table:
-- they are one-to-many per variant and would break the grain.
-- Join silver.dim_variant_attribute on variant_id in Power BI when needed.
--
-- WARNING - total_orders is NOT additive across variants.
--   An order containing 2 different variants is counted once in EACH
--   variant's row. Summing total_orders over products overcounts.
--   Use gold.daily_sales_summary for order totals. Revenue and units
--   columns ARE safe to SUM across any dimension.
--
-- The descriptive columns (product_id, product_name, category_name,
-- brand_name) are copied from dim_product so BI can group by category or
-- brand without extra joins. They are NULL only if a variant is missing
-- from dim_product (LEFT JOIN keeps those sales instead of dropping them).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.product_performance (
    date_key           INT            NOT NULL REFERENCES gold.dim_date (date_key),
    order_date         DATE           NOT NULL,
    variant_id         BIGINT         NOT NULL,

    -- descriptive, carried in from silver.dim_product
    product_id         BIGINT,
    product_name       VARCHAR(255),
    category_name      VARCHAR(100),
    brand_name         VARCHAR(100),

    -- valid orders (not cancelled / returned)
    units_sold         BIGINT         NOT NULL,   -- SUM(qty)
    total_orders       INT            NOT NULL,   -- NOT additive across variants
    gross_revenue      DECIMAL(16,2)  NOT NULL,   -- SUM(line_taxable)
    total_discount     DECIMAL(16,2)  NOT NULL,   -- SUM(discount_line)
    total_tax          DECIMAL(16,2)  NOT NULL,   -- SUM(gst_line_total)
    net_revenue        DECIMAL(16,2)  NOT NULL,   -- SUM(line_total)

    -- kept separate for their own charts
    cancelled_orders   INT            NOT NULL,
    cancelled_revenue  DECIMAL(16,2)  NOT NULL,
    returned_orders    INT            NOT NULL,
    returned_revenue   DECIMAL(16,2)  NOT NULL,

    _loaded_at         TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, variant_id)
);

CREATE INDEX IF NOT EXISTS idx_product_performance_variant
    ON gold.product_performance (variant_id);
CREATE INDEX IF NOT EXISTS idx_product_performance_category
    ON gold.product_performance (category_name);
CREATE INDEX IF NOT EXISTS idx_product_performance_brand
    ON gold.product_performance (brand_name);