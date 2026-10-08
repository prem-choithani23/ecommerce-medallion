-- ============================================================
-- ETL METADATA & LOGGING TABLES
-- ============================================================
-- Purpose:
--   Stores ETL state and execution history for Bronze/Silver/Gold
--   pipelines.
--
-- Tables:
--   1. bronze.etl_metadata
--      Stores the current watermark/state for each table and layer.
--
--   2. bronze.etl_log
--      Stores the execution history of ETL runs.
-- ============================================================


-- ============================================================
-- 1. ETL METADATA
-- ============================================================
-- Tracks the latest processed position for each table/layer.
--
-- watermark_type:
--   'id'          -> incremental loading based on an ID
--   'timestamp'  -> incremental loading based on a timestamp
--   'full_reload'-> reload the complete source table
--
-- last_id:
--   Used when watermark_type = 'id'
--
-- last_timestamp:
--   Used when watermark_type = 'timestamp'
--
-- Primary key:
--   (table_name, layer) ensures one metadata record per
--   table per Medallion layer.
-- ============================================================

CREATE TABLE bronze.etl_metadata (
    table_name      VARCHAR(64) NOT NULL,
    layer           VARCHAR(16) NOT NULL,        -- 'bronze', 'silver', 'gold'
    watermark_type  VARCHAR(16) NOT NULL,        -- 'id', 'timestamp', 'full_reload'
    last_id         BIGINT,                      -- Used if watermark_type = 'id'
    last_timestamp  TIMESTAMP,                   -- Used if watermark_type = 'timestamp'
    last_run_at     TIMESTAMP,

    PRIMARY KEY (table_name, layer)
);


-- ============================================================
-- 2. ETL LOG
-- ============================================================
-- Stores the execution history of ETL pipeline runs.
--
-- run_id:
--   Airflow's run_id, used to trace a particular pipeline run.
--
-- status:
--   'RUNNING' -> ETL execution is currently in progress
--   'SUCCESS' -> ETL execution completed successfully
--   'FAILED'  -> ETL execution failed
--
-- rows_extracted:
--   Number of rows extracted from the source.
--
-- rows_upserted:
--   Number of rows inserted/updated in the target layer.
--
-- error_message:
--   Stores the error details when an ETL run fails.
-- ============================================================

CREATE TABLE bronze.etl_log (
    log_id          BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(64) NOT NULL,
    layer           VARCHAR(16) NOT NULL,
    run_id          VARCHAR(64) NOT NULL,        -- Airflow's run_id, for tracing
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    rows_extracted  INT,
    rows_upserted   INT,
    status          VARCHAR(16) NOT NULL,        -- 'RUNNING', 'SUCCESS', 'FAILED'
    error_message   TEXT
);
