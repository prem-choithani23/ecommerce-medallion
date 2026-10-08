-- ============================================================
-- BRONZE LAYER
-- ============================================================
-- Purpose:
--   Create the 20 Bronze tables corresponding to the source
--   MySQL tables.
--
-- Design:
--   - Preserve source table and column names
--   - Preserve source nullability
--   - Preserve source data semantics
--   - Use PostgreSQL-compatible data types
--   - Keep Bronze as close to the source as possible
--
-- PostgreSQL type mappings:
--   MySQL DATETIME      -> TIMESTAMP
--   MySQL DECIMAL       -> NUMERIC
--   MySQL TINYINT(1)    -> BOOLEAN
--   MySQL ENUM          -> VARCHAR
--
-- ENUM values are stored as VARCHAR in Bronze deliberately.
-- Business/domain validation can be applied in Silver.
-- ============================================================


-- ============================================================
-- CREATE BRONZE SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;


-- ============================================================
-- 1. ATTRIBUTE_VALUES
-- ============================================================

CREATE TABLE bronze.attribute_values (
    attribute_value_id BIGINT NOT NULL,
    attribute_id       INTEGER NOT NULL,
    value_text         VARCHAR(64) NOT NULL
);


-- ============================================================
-- 2. ATTRIBUTES
-- ============================================================

CREATE TABLE bronze.attributes (
    attribute_id   INTEGER NOT NULL,
    attribute_name VARCHAR(64) NOT NULL
);


-- ============================================================
-- 3. BRANDS
-- ============================================================

CREATE TABLE bronze.brands (
    brand_id   INTEGER NOT NULL,
    brand_name VARCHAR(64) NOT NULL
);


-- ============================================================
-- 4. CART_ITEMS
-- ============================================================

CREATE TABLE bronze.cart_items (
    cart_item_id    BIGINT NOT NULL,
    cart_id         BIGINT NOT NULL,
    variant_id      BIGINT NOT NULL,
    qty             INTEGER NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    added_at        TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);


-- ============================================================
-- 5. CARTS
-- ============================================================

CREATE TABLE bronze.carts (
    cart_id            BIGINT NOT NULL,
    customer_id        BIGINT NULL,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL,
    channel            VARCHAR(20) NOT NULL,
    cart_status        VARCHAR(20) NOT NULL,
    abandoned_at       TIMESTAMP NULL,
    converted_order_id BIGINT NULL
);


-- ============================================================
-- 6. CATEGORIES
-- ============================================================

CREATE TABLE bronze.categories (
    category_id   INTEGER NOT NULL,
    category_name VARCHAR(64) NOT NULL
);


-- ============================================================
-- 7. CITIES
-- ============================================================

CREATE TABLE bronze.cities (
    city_id    BIGINT NOT NULL,
    state_code CHAR(2) NOT NULL,
    city_name  VARCHAR(64) NOT NULL
);


-- ============================================================
-- 8. CUSTOMER_ADDRESSES
-- ============================================================

CREATE TABLE bronze.customer_addresses (
    address_id   BIGINT NOT NULL,
    customer_id  BIGINT NOT NULL,
    address_type VARCHAR(10) NOT NULL,
    line1        VARCHAR(255) NOT NULL,
    line2        VARCHAR(255) NULL,
    city_id      BIGINT NOT NULL,
    pincode      CHAR(6) NOT NULL,
    is_default   BOOLEAN NOT NULL,
    created_at   TIMESTAMP NOT NULL
);


-- ============================================================
-- 9. CUSTOMERS
-- ============================================================

CREATE TABLE bronze.customers (
    customer_id BIGINT NOT NULL,
    first_name  VARCHAR(64) NOT NULL,
    last_name   VARCHAR(64) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    phone       VARCHAR(16) NOT NULL,
    pan         CHAR(10) NULL,
    gstin       VARCHAR(15) NULL,
    created_at  TIMESTAMP NOT NULL
);


-- ============================================================
-- 10. INVENTORY
-- ============================================================

CREATE TABLE bronze.inventory (
    warehouse_id INTEGER NOT NULL,
    variant_id   BIGINT NOT NULL,
    on_hand      INTEGER NOT NULL,
    reserved     INTEGER NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);


-- ============================================================
-- 11. ORDER_ITEMS
-- ============================================================

CREATE TABLE bronze.order_items (
    order_item_id       BIGINT NOT NULL,
    order_id            BIGINT NOT NULL,
    product_id          BIGINT NOT NULL,
    variant_id          BIGINT NOT NULL,
    hsn_code            VARCHAR(8) NOT NULL,
    gst_rate            NUMERIC(5,4) NOT NULL,
    qty                 INTEGER NOT NULL,
    unit_price_taxable  NUMERIC(12,2) NOT NULL,
    line_taxable        NUMERIC(12,2) NOT NULL,
    discount_line       NUMERIC(12,2) NOT NULL,
    cgst_line           NUMERIC(12,2) NOT NULL,
    sgst_line           NUMERIC(12,2) NOT NULL,
    igst_line           NUMERIC(12,2) NOT NULL,
    gst_line_total      NUMERIC(12,2) NOT NULL,
    line_total          NUMERIC(12,2) NOT NULL
);


-- ============================================================
-- 12. ORDERS
-- ============================================================

CREATE TABLE bronze.orders (
    order_id             BIGINT NOT NULL,
    order_number         VARCHAR(32) NOT NULL,
    cart_id              BIGINT NULL,
    customer_id          BIGINT NULL,
    shipping_address_id  BIGINT NOT NULL,
    billing_address_id   BIGINT NULL,
    order_datetime       TIMESTAMP NOT NULL,
    channel              VARCHAR(20) NOT NULL,
    payment_method       VARCHAR(20) NOT NULL,
    order_status         VARCHAR(20) NOT NULL,
    subtotal_taxable     NUMERIC(12,2) NOT NULL,
    discount_total       NUMERIC(12,2) NOT NULL,
    shipping_fee_taxable NUMERIC(12,2) NOT NULL,
    cgst                  NUMERIC(12,2) NOT NULL,
    sgst                  NUMERIC(12,2) NOT NULL,
    igst                  NUMERIC(12,2) NOT NULL,
    gst_total             NUMERIC(12,2) NOT NULL,
    grand_total           NUMERIC(12,2) NOT NULL
);


-- ============================================================
-- 13. PAYMENTS
-- ============================================================

CREATE TABLE bronze.payments (
    payment_id     BIGINT NOT NULL,
    order_id       BIGINT NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    gateway_ref    VARCHAR(64) NULL,
    paid_at        TIMESTAMP NULL
);


-- ============================================================
-- 14. PRODUCT_TAX_PROFILES
-- ============================================================

CREATE TABLE bronze.product_tax_profiles (
    product_id    BIGINT NOT NULL,
    hsn_code      VARCHAR(8) NOT NULL,
    gst_rate      NUMERIC(5,4) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to   DATE NULL
);


-- ============================================================
-- 15. PRODUCT_VARIANTS
-- ============================================================

CREATE TABLE bronze.product_variants (
    variant_id    BIGINT NOT NULL,
    product_id    BIGINT NOT NULL,
    sku           VARCHAR(40) NOT NULL,
    mrp           NUMERIC(12,2) NOT NULL,
    selling_price NUMERIC(12,2) NOT NULL,
    is_active     BOOLEAN NOT NULL,
    created_at    TIMESTAMP NOT NULL
);


-- ============================================================
-- 16. PRODUCTS
-- ============================================================

CREATE TABLE bronze.products (
    product_id   BIGINT NOT NULL,
    category_id  INTEGER NOT NULL,
    brand_id     INTEGER NOT NULL,
    product_name VARCHAR(128) NOT NULL,
    description  TEXT NULL,
    is_active    BOOLEAN NOT NULL,
    created_at   TIMESTAMP NOT NULL
);


-- ============================================================
-- 17. SELLER_PROFILES
-- ============================================================

CREATE TABLE bronze.seller_profiles (
    seller_id         INTEGER NOT NULL,
    seller_name       VARCHAR(128) NOT NULL,
    origin_state_code CHAR(2) NOT NULL,
    gstin             VARCHAR(15) NOT NULL,
    created_at        TIMESTAMP NOT NULL
);


-- ============================================================
-- 18. STATES
-- ============================================================

CREATE TABLE bronze.states (
    state_code CHAR(2) NOT NULL,
    state_name VARCHAR(64) NOT NULL
);


-- ============================================================
-- 19. VARIANT_ATTRIBUTE_VALUES
-- ============================================================

CREATE TABLE bronze.variant_attribute_values (
    variant_id          BIGINT NOT NULL,
    attribute_id        INTEGER NOT NULL,
    attribute_value_id  BIGINT NOT NULL
);


-- ============================================================
-- 20. WAREHOUSES
-- ============================================================

CREATE TABLE bronze.warehouses (
    warehouse_id   INTEGER NOT NULL,
    warehouse_name VARCHAR(64) NOT NULL,
    city_id        BIGINT NOT NULL
);
