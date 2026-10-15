DROP VIEW IF EXISTS bench.daily_channel_revenue_mv;
DROP TABLE IF EXISTS bench.fact_order_items;
DROP TABLE IF EXISTS bench.fact_cart_items;
DROP TABLE IF EXISTS bench.fact_payments;
DROP TABLE IF EXISTS bench.dim_product;
DROP TABLE IF EXISTS bench.dim_customer;

CREATE TABLE bench.dim_customer (
    customer_id Int64, first_name String, last_name String, full_name String,
    email String, phone String, created_at DateTime64(6), default_address_id Int64,
    default_line1 String, default_line2 Nullable(String), default_city_id Int64,
    default_pincode String
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE bench.dim_product (
    variant_id Int64, product_id Int64, product_name String, description String,
    category_id Int64, category_name LowCardinality(String), brand_id Int64,
    brand_name LowCardinality(String), hsn_code String, gst_rate Decimal(38,6),
    sku String, mrp Decimal(38,6), selling_price Decimal(38,6),
    product_is_active Bool, variant_is_active Bool,
    product_created_at DateTime64(6), variant_created_at DateTime64(6)
) ENGINE = MergeTree ORDER BY variant_id;

CREATE TABLE bench.fact_payments (
    payment_id Int64, order_id Int64, payment_method LowCardinality(String),
    payment_status LowCardinality(String), amount Decimal(38,6),
    gateway_ref String, paid_at Nullable(DateTime64(6))
) ENGINE = MergeTree ORDER BY (payment_method, payment_id);

CREATE TABLE bench.fact_order_items (
    order_item_id Int64, order_id Int64, order_number String,
    order_datetime DateTime64(6), order_status LowCardinality(String),
    channel LowCardinality(String), customer_id Int64, variant_id Int64,
    hsn_code String, gst_rate Decimal(38,6), qty Int32,
    unit_price_taxable Decimal(38,6), line_taxable Decimal(38,6),
    discount_line Decimal(38,6), cgst_line Decimal(38,6), sgst_line Decimal(38,6),
    igst_line Decimal(38,6), gst_line_total Decimal(38,6), line_total Decimal(38,6)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(order_datetime)
ORDER BY (order_datetime, order_id);

CREATE TABLE bench.fact_cart_items (
    cart_item_id Int64, cart_id Int64, customer_id Nullable(Int64),
    cart_status LowCardinality(String), channel LowCardinality(String),
    cart_created_at DateTime64(6), cart_updated_at DateTime64(6),
    abandoned_at Nullable(DateTime64(6)), converted_order_id Nullable(Int64),
    variant_id Int64, qty Int32, unit_price Decimal(38,6),
    discount_amount Decimal(38,6), added_at DateTime64(6), updated_at DateTime64(6)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(cart_created_at)
ORDER BY (cart_created_at, cart_id);
