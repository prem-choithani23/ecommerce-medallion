"""
gold_product_performance

Rebuilds gold.product_performance from silver.fact_order_items joined to
silver.dim_product.

Pattern (same as gold_daily_sales_summary):
    log_start -> build_performance -> log_finish

Strategy: full TRUNCATE + INSERT every run, inside ONE transaction.
TRUNCATE is transactional in Postgres, so a failed INSERT rolls back and
the previous table contents are restored.

Grain: one row per (order date, variant_id).
Valid orders = order_status NOT IN ('CANCELLED', 'RETURNED').
Cancelled / returned amounts live in their own columns.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_warehouse"
TABLE_NAME = "product_performance"
LAYER = "gold"

BUILD_SQL = """
INSERT INTO gold.product_performance (
    date_key, order_date, variant_id,
    product_id, product_name, category_name, brand_name,
    units_sold, total_orders,
    gross_revenue, total_discount, total_tax, net_revenue,
    cancelled_orders, cancelled_revenue,
    returned_orders, returned_revenue
)
SELECT
    to_char(f.order_datetime::date, 'YYYYMMDD')::int           AS date_key,
    f.order_datetime::date                                     AS order_date,
    f.variant_id,
    p.product_id,
    p.product_name,
    p.category_name,
    p.brand_name,

    COALESCE(SUM(f.qty) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS units_sold,
    COUNT(DISTINCT f.order_id) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED'))              AS total_orders,
    COALESCE(SUM(f.line_taxable) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS gross_revenue,
    COALESCE(SUM(f.discount_line) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS total_discount,
    COALESCE(SUM(f.gst_line_total) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS total_tax,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS net_revenue,

    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'CANCELLED')  AS cancelled_orders,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status = 'CANCELLED'), 0)                             AS cancelled_revenue,
    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'RETURNED')   AS returned_orders,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status = 'RETURNED'), 0)                              AS returned_revenue
FROM silver.fact_order_items f
LEFT JOIN silver.dim_product p ON p.variant_id = f.variant_id
GROUP BY
    f.order_datetime::date, f.variant_id,
    p.product_id, p.product_name, p.category_name, p.brand_name;
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


def build_performance(**context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.product_performance;")
            cur.execute(BUILD_SQL)
            rows = cur.rowcount
        conn.commit()          # TRUNCATE + INSERT become visible together
    except Exception:
        conn.rollback()        # old table contents are restored
        raise
    finally:
        conn.close()
    return rows


def log_finish(**context):
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="log_start")
    rows = ti.xcom_pull(task_ids="build_performance")
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
    dag_id="gold_product_performance",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by master_pipeline (phase 3)
    catchup=False,
    default_args={"on_failure_callback": mark_failed},
    tags=["gold", "products"],
) as dag:
    t_start = PythonOperator(task_id="log_start", python_callable=log_start)
    t_build = PythonOperator(task_id="build_performance", python_callable=build_performance)
    t_finish = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t_start >> t_build >> t_finish