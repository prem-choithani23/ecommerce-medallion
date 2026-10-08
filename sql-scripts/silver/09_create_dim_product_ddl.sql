-- Silver layer: product dimension (grain = variant_id) + EAV attribute table.
-- Run this once against ecommerce_warehouse before triggering the Silver DAG.

CREATE TABLE IF NOT EXISTS silver.dim_product (
    variant_id          BIGINT PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    product_name        VARCHAR(255),
    description          TEXT,
    category_id          BIGINT,
    category_name        VARCHAR(100),
    brand_id             BIGINT,
    brand_name           VARCHAR(100),
    hsn_code             VARCHAR(20),
    gst_rate             DECIMAL(5,2),
    sku                   VARCHAR(100),
    mrp                   DECIMAL(12,2),
    selling_price         DECIMAL(12,2),
    product_is_active     BOOLEAN,
    variant_is_active     BOOLEAN,
    product_created_at    TIMESTAMP,
    variant_created_at    TIMESTAMP,
    _loaded_at            TIMESTAMP NOT NULL DEFAULT now()
);

-- EAV: one row per (variant, attribute) pair. Keeps this open-ended -
-- new attribute types (e.g. "Material") need zero schema changes here,
-- unlike pivoting attributes into dim_product columns would have required.
CREATE TABLE IF NOT EXISTS silver.dim_variant_attribute (
    variant_id          BIGINT NOT NULL,
    attribute_id         BIGINT NOT NULL,
    attribute_name        VARCHAR(100),
    attribute_value_id   BIGINT,
    value_text            VARCHAR(255),
    _loaded_at            TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (variant_id, attribute_id)
);