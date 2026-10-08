-- =====================================================================
-- gold.dim_date : calendar dimension
-- Grain : one row per calendar date
-- Range : Jan 1 of the earliest year in Silver data -> Dec 31 of the latest
--         (orders, carts, abandoned carts, payments), derived from the data
-- Idempotent: safe to re-run (CREATE IF NOT EXISTS + ON CONFLICT DO NOTHING)
-- Run once, manually. No Airflow DAG needed.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key       INT          PRIMARY KEY,   -- yyyymmdd, e.g. 20260228
    full_date      DATE         NOT NULL UNIQUE,
    year           SMALLINT     NOT NULL,
    quarter        SMALLINT     NOT NULL,      -- 1-4
    month          SMALLINT     NOT NULL,      -- 1-12
    month_name     VARCHAR(10)  NOT NULL,
    year_month     CHAR(7)      NOT NULL,      -- 'YYYY-MM', handy for charts
    week_of_year   SMALLINT     NOT NULL,      -- ISO week
    day_of_month   SMALLINT     NOT NULL,
    day_of_week    SMALLINT     NOT NULL,      -- ISO: 1 = Monday ... 7 = Sunday
    day_name       VARCHAR(10)  NOT NULL,
    is_weekend     BOOLEAN      NOT NULL,
    _loaded_at     TIMESTAMP    NOT NULL DEFAULT now()
);

-- LEAST/GREATEST ignore NULLs in Postgres, so empty or NULL columns are safe.
WITH bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(order_datetime)   FROM silver.fact_order_items),
            (SELECT MIN(cart_created_at)  FROM silver.fact_cart_items),
            (SELECT MIN(cart_updated_at)  FROM silver.fact_cart_items),
            (SELECT MIN(abandoned_at)     FROM silver.fact_cart_items),
            (SELECT MIN(paid_at)          FROM silver.fact_payments)
        )::date AS min_d,
        GREATEST(
            (SELECT MAX(order_datetime)   FROM silver.fact_order_items),
            (SELECT MAX(cart_created_at)  FROM silver.fact_cart_items),
            (SELECT MAX(cart_updated_at)  FROM silver.fact_cart_items),
            (SELECT MAX(abandoned_at)     FROM silver.fact_cart_items),
            (SELECT MAX(paid_at)          FROM silver.fact_payments)
        )::date AS max_d
),
span AS (
    SELECT
        make_date(EXTRACT(YEAR FROM min_d)::int, 1, 1)   AS start_d,
        make_date(EXTRACT(YEAR FROM max_d)::int, 12, 31) AS end_d
    FROM bounds
)
INSERT INTO gold.dim_date (
    date_key, full_date, year, quarter, month, month_name, year_month,
    week_of_year, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d::date,
    EXTRACT(YEAR    FROM d)::smallint,
    EXTRACT(QUARTER FROM d)::smallint,
    EXTRACT(MONTH   FROM d)::smallint,
    trim(to_char(d, 'Month')),
    to_char(d, 'YYYY-MM'),
    EXTRACT(WEEK    FROM d)::smallint,
    EXTRACT(DAY     FROM d)::smallint,
    EXTRACT(ISODOW  FROM d)::smallint,
    trim(to_char(d, 'Day')),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM span, generate_series(span.start_d, span.end_d, interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;