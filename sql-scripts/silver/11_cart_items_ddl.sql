-- Silver layer: cart/funnel fact table.
-- Run this once against ecommerce_warehouse before triggering the Silver DAG.

CREATE TABLE IF NOT EXISTS silver.fact_cart_items (
    cart_item_id        BIGINT PRIMARY KEY,
    cart_id               BIGINT NOT NULL,
    customer_id            BIGINT,            -- nullable: anonymous/guest carts exist
    cart_status             VARCHAR(20),       -- degenerate dimension
    channel                  VARCHAR(20),       -- degenerate dimension
    cart_created_at           TIMESTAMP,
    cart_updated_at           TIMESTAMP,
    abandoned_at               TIMESTAMP,
    converted_order_id         BIGINT,           -- bridge to fact_order_items, if converted
    variant_id                  BIGINT,           -- FK -> silver.dim_product
    qty                           INT,
    unit_price                    DECIMAL(12,2),
    discount_amount                 DECIMAL(12,2),
    added_at                         TIMESTAMP,
    updated_at                         TIMESTAMP,
    _loaded_at                           TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_cart_items_customer ON silver.fact_cart_items (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_cart_items_variant ON silver.fact_cart_items (variant_id);
CREATE INDEX IF NOT EXISTS idx_fact_cart_items_cart ON silver.fact_cart_items (cart_id);
CREATE INDEX IF NOT EXISTS idx_fact_cart_items_converted_order ON silver.fact_cart_items (converted_order_id);