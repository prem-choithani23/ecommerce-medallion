-- Silver layer: customer dimension + full address history.
-- Run this once against ecommerce_warehouse before triggering the Silver DAG.

CREATE TABLE IF NOT EXISTS silver.dim_customer_address (
    address_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    address_type    VARCHAR(50),
    line1           TEXT,
    line2           TEXT,
    city_id         BIGINT,
    pincode         VARCHAR(20),
    is_default      BOOLEAN,
    created_at      TIMESTAMP,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.dim_customer (
    customer_id         BIGINT PRIMARY KEY,
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    full_name            VARCHAR(200),
    email                VARCHAR(255),
    phone                VARCHAR(20),
    pan                  VARCHAR(20),
    gstin                VARCHAR(20),
    created_at           TIMESTAMP,
    default_address_id   BIGINT,
    default_line1        TEXT,
    default_line2        TEXT,
    default_city_id      BIGINT,
    default_pincode      VARCHAR(20),
    _loaded_at           TIMESTAMP NOT NULL DEFAULT now()
);