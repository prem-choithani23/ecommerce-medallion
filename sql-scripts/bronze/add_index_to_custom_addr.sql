CREATE INDEX IF NOT EXISTS idx_bronze_customer_addresses_default
    ON bronze.customer_addresses (customer_id, address_id)
    WHERE is_default;

