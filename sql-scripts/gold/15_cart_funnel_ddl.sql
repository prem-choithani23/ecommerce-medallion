-- =====================================================================
-- gold.cart_funnel
-- Grain : one row per (cart creation date, channel)
-- Source: silver.fact_cart_items (one row per cart item, cart attributes
--         repeated on every item). Collapsed to one row per cart first,
--         so each cart is counted exactly once, on the day it was created,
--         with its CURRENT status.
--
-- Cart value formula (confirmed from data):
--   discount_amount is a per-LINE amount (its ratio to unit_price grows
--   with qty), so  cart_value = SUM(qty * unit_price - discount_amount)
--
-- Counts are additive across dates and channels. Rates are NOT stored
-- (a stored rate cannot be rolled up). Compute in Power BI:
--   conversion rate  = SUM(converted_carts)  / SUM(total_carts)
--   abandonment rate = SUM(abandoned_carts)  / SUM(total_carts)
--   expiry rate      = SUM(expired_carts)    / SUM(total_carts)
--
-- Data-quality finding kept visible on purpose:
--   converted_carts        = cart_status = 'CONVERTED'  (status as recorded)
--   converted_carts_linked = converted AND converted_order_id IS NOT NULL
--   The difference = carts marked CONVERTED that no order points back to.
--
-- active_carts is 0 in the current data (no ACTIVE carts exist) but the
-- source status allows it, so the column stays for future loads.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.cart_funnel (
    date_key                INT            NOT NULL REFERENCES gold.dim_date (date_key),
    cart_date               DATE           NOT NULL,   -- cart_created_at::date
    channel                 VARCHAR(20)    NOT NULL,

    -- cart counts (each cart counted once)
    total_carts             INT            NOT NULL,
    active_carts            INT            NOT NULL,
    abandoned_carts         INT            NOT NULL,
    converted_carts         INT            NOT NULL,
    converted_carts_linked  INT            NOT NULL,
    expired_carts           INT            NOT NULL,

    -- item and value measures
    items_in_carts          BIGINT         NOT NULL,   -- SUM(qty)
    cart_value              DECIMAL(18,2)  NOT NULL,   -- all carts
    abandoned_cart_value    DECIMAL(18,2)  NOT NULL,   -- revenue left on the table
    converted_cart_value    DECIMAL(18,2)  NOT NULL,

    _loaded_at              TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, channel)
);

CREATE INDEX IF NOT EXISTS idx_cart_funnel_date
    ON gold.cart_funnel (cart_date);