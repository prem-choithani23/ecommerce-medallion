ALTER TABLE bronze.orders
    ADD COLUMN _ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN _source_system VARCHAR(32) NOT NULL DEFAULT 'mysql',
    ADD COLUMN _batch_id VARCHAR(64);