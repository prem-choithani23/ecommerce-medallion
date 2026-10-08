"""
gold_dim_date

Extends gold.dim_date so it always covers every date present in Silver.
Runs as the FIRST Gold step: every other Gold table has a foreign key on
gold.dim_date(date_key), so the calendar must be complete before they load.

Pattern (same as the other Gold DAGs):
    log_start -> build_dim_date -> log_finish

Strategy: INSERT ... ON CONFLICT DO NOTHING. NOT truncate-and-rebuild,
because the other Gold tables reference this table by foreign key (Postgres
refuses to TRUNCATE a referenced table), and calendar rows never change.
Safe to re-run any number of times: on a normal run it inserts 0 rows.

Range: Jan 1 of the earliest year to Dec 31 of the latest year found in
orders, carts, abandoned carts and payments (derived from data).

Prerequisite: gold_dim_date_ddl.sql has been run once (creates the table).
rows_upserted in bronze.etl_log = rows NEWLY inserted (0 when nothing new).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_warehouse"
TABLE_NAME = "dim_date"
LAYER = "gold"

BUILD_SQL = """
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
"""


def _update_log(sql, params):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def log_start(**context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.etl_log
                    (table_name, layer, run_id, started_at, status)
                VALUES (%s, %s, %s, now(), 'RUNNING')
                RETURNING log_id;
                """,
                (TABLE_NAME, LAYER, context["run_id"]),
            )
            log_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return log_id


def build_dim_date(**context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(BUILD_SQL)
            rows = cur.rowcount        # newly inserted rows only
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return rows


def log_finish(**context):
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="log_start")
    rows = ti.xcom_pull(task_ids="build_dim_date")
    _update_log(
        """
        UPDATE bronze.etl_log
        SET status = 'SUCCESS', finished_at = now(), rows_upserted = %s
        WHERE log_id = %s;
        """,
        (rows, log_id),
    )


def mark_failed(context):
    log_id = context["ti"].xcom_pull(task_ids="log_start")
    if log_id is None:
        return
    _update_log(
        """
        UPDATE bronze.etl_log
        SET status = 'FAILED', finished_at = now(), error_message = %s
        WHERE log_id = %s;
        """,
        (str(context.get("exception"))[:2000], log_id),
    )


with DAG(
    dag_id="gold_dim_date",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by master_pipeline (phase 3, first step)
    catchup=False,
    default_args={"on_failure_callback": mark_failed},
    tags=["gold", "calendar"],
) as dag:
    t_start = PythonOperator(task_id="log_start", python_callable=log_start)
    t_build = PythonOperator(task_id="build_dim_date", python_callable=build_dim_date)
    t_finish = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t_start >> t_build >> t_finish
