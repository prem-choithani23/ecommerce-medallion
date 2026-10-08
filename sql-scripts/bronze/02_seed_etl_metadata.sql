-- ============================================================
-- SEED ETL METADATA
-- ============================================================
-- Purpose:
--   Initialize the ETL metadata table with the loading strategy
--   for each Bronze source table.
--
-- Watermark strategies:
--   full_reload -> Reload the complete source table
--   id          -> Incremental loading based on an increasing ID
--   timestamp   -> Incremental loading based on a timestamp column
--
-- Initial watermark values:
--   ID-based tables       -> last_id = 0
--   Timestamp-based tables-> last_timestamp = '1970-01-01'
--   Full reload tables    -> No watermark required
-- ============================================================


INSERT INTO bronze.etl_metadata
    (table_name, layer, watermark_type, last_id, last_timestamp)
VALUES

    -- ========================================================
    -- FULL RELOAD TABLES
    -- Small/reference tables that are reloaded completely.
    -- ========================================================

    ('orders',                  'bronze', 'full_reload', NULL, NULL),
    ('payments',                'bronze', 'full_reload', NULL, NULL),
    ('states',                  'bronze', 'full_reload', NULL, NULL),
    ('cities',                  'bronze', 'full_reload', NULL, NULL),
    ('categories',              'bronze', 'full_reload', NULL, NULL),
    ('brands',                  'bronze', 'full_reload', NULL, NULL),
    ('attributes',              'bronze', 'full_reload', NULL, NULL),
    ('attribute_values',        'bronze', 'full_reload', NULL, NULL),
    ('warehouses',              'bronze', 'full_reload', NULL, NULL),
    ('seller_profiles',         'bronze', 'full_reload', NULL, NULL),
    ('product_tax_profiles',    'bronze', 'full_reload', NULL, NULL),
    ('variant_attribute_values','bronze', 'full_reload', NULL, NULL),

    -- ========================================================
    -- ID-BASED INCREMENTAL TABLES
    -- Initial watermark starts at ID = 0.
    -- ========================================================

    ('order_items',             'bronze', 'id', 0, NULL),
    ('customers',               'bronze', 'id', 0, NULL),
    ('customer_addresses',      'bronze', 'id', 0, NULL),
    ('products',                'bronze', 'id', 0, NULL),
    ('product_variants',        'bronze', 'id', 0, NULL),

    -- ========================================================
    -- TIMESTAMP-BASED INCREMENTAL TABLES
    -- Initial watermark starts at Unix epoch.
    -- ========================================================

    ('carts',                   'bronze', 'timestamp', NULL, '1970-01-01'),
    ('cart_items',              'bronze', 'timestamp', NULL, '1970-01-01'),
    ('inventory',               'bronze', 'timestamp', NULL, '1970-01-01');
