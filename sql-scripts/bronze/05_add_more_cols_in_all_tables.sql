ALTER TABLE bronze.order_items
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.payments
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.customers
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.customer_addresses
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.products
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.product_variants
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.carts
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.cart_items
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.inventory
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.states
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.cities
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.categories
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.brands
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.attributes
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.attribute_values
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.warehouses
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.seller_profiles
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.product_tax_profiles
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);

ALTER TABLE bronze.variant_attribute_values
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);