DROP TABLE IF EXISTS fact_order_items, fact_cart_items, fact_payments, dim_product, dim_customer CASCADE;

CREATE TABLE dim_customer (
    customer_id bigint, first_name varchar, last_name varchar, full_name varchar,
    email varchar, phone varchar, created_at timestamp, default_address_id bigint,
    default_line1 text, default_line2 text, default_city_id bigint, default_pincode varchar
);

CREATE TABLE dim_product (
    variant_id bigint, product_id bigint, product_name varchar, description text,
    category_id bigint, category_name varchar, brand_id bigint, brand_name varchar,
    hsn_code varchar, gst_rate numeric, sku varchar, mrp numeric, selling_price numeric,
    product_is_active boolean, variant_is_active boolean,
    product_created_at timestamp, variant_created_at timestamp
);

CREATE TABLE fact_payments (
    payment_id bigint, order_id bigint, payment_method varchar, payment_status varchar,
    amount numeric, gateway_ref varchar, paid_at timestamp
);

CREATE TABLE fact_order_items (
    order_item_id bigint, order_id bigint, order_number varchar,
    order_datetime timestamp NOT NULL, order_status varchar, channel varchar,
    customer_id bigint, variant_id bigint, hsn_code varchar, gst_rate numeric, qty integer,
    unit_price_taxable numeric, line_taxable numeric, discount_line numeric,
    cgst_line numeric, sgst_line numeric, igst_line numeric, gst_line_total numeric,
    line_total numeric
);
SELECT create_hypertable('fact_order_items', 'order_datetime', chunk_time_interval => INTERVAL '1 month');

CREATE TABLE fact_cart_items (
    cart_item_id bigint, cart_id bigint, customer_id bigint, cart_status varchar,
    channel varchar, cart_created_at timestamp NOT NULL, cart_updated_at timestamp,
    abandoned_at timestamp, converted_order_id bigint, variant_id bigint, qty integer,
    unit_price numeric, discount_amount numeric, added_at timestamp, updated_at timestamp
);
SELECT create_hypertable('fact_cart_items', 'cart_created_at', chunk_time_interval => INTERVAL '1 month');
