ALTER TABLE bronze.order_items ADD PRIMARY KEY (order_item_id);
ALTER TABLE bronze.customers ADD PRIMARY KEY (customer_id);
ALTER TABLE bronze.customer_addresses ADD PRIMARY KEY (address_id);
ALTER TABLE bronze.products ADD PRIMARY KEY (product_id);
ALTER TABLE bronze.product_variants ADD PRIMARY KEY (variant_id);