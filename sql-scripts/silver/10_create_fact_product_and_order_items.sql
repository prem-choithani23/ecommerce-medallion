-- Silver layer: two fact tables at two different, correct grains.
-- Run this once against ecommerce_warehouse before triggering the Silver DAG.

-- Grain: ONE ROW PER order_item_id (atomic grain - the finest available).
-- order_number, order_status, channel, order_datetime are DEGENERATE
-- DIMENSIONS - pulled from the parent order but kept directly on the fact
-- row rather than given their own dimension table, since they're simple
-- attributes with no further detail to normalize out.
CREATE TABLE IF NOT EXISTS silver.fact_order_items (
    order_item_id        BIGINT PRIMARY KEY,
    order_id              BIGINT NOT NULL,
    order_number          VARCHAR(50),       -- degenerate dimension
    order_datetime         TIMESTAMP,
    order_status           VARCHAR(50),       -- degenerate dimension
    channel                 VARCHAR(50),       -- degenerate dimension
    customer_id             BIGINT,            -- FK -> silver.dim_customer
    variant_id               BIGINT,            -- FK -> silver.dim_product
    hsn_code                  VARCHAR(20),
    gst_rate                  DECIMAL(5,2),
    qty                        INT,               -- additive
    unit_price_taxable        DECIMAL(12,2),     -- SEMI-ADDITIVE: do not SUM across rows, only AVG or take per-row
    line_taxable               DECIMAL(12,2),     -- additive
    discount_line               DECIMAL(12,2),     -- additive
    cgst_line                   DECIMAL(12,2),     -- additive
    sgst_line                   DECIMAL(12,2),     -- additive
    igst_line                   DECIMAL(12,2),     -- additive
    gst_line_total               DECIMAL(12,2),     -- additive
    line_total                   DECIMAL(12,2),     -- additive
    _loaded_at                    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_customer ON silver.fact_order_items (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_variant ON silver.fact_order_items (variant_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order ON silver.fact_order_items (order_id);

-- Grain: ONE ROW PER payment_id - deliberately NOT merged into
-- fact_order_items, since a payment is order-level, not line-item-level.
-- Joining this 1:1 against fact_order_items would duplicate the payment
-- amount across every line item of that order.
CREATE TABLE IF NOT EXISTS silver.fact_payments (
    payment_id      BIGINT PRIMARY KEY,
    order_id          BIGINT NOT NULL,       -- FK -> orders, via fact_order_items.order_id (no separate order dim)
    payment_method     VARCHAR(50),
    payment_status      VARCHAR(50),
    amount               DECIMAL(12,2),         -- additive, but only within this table's own grain
    gateway_ref           VARCHAR(100),
    paid_at                TIMESTAMP,
    _loaded_at              TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_payments_order ON silver.fact_payments (order_id);