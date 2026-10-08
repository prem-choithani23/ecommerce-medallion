"""
gold_customer_summary

Rebuilds gold.customer_summary from silver.dim_customer LEFT JOIN
silver.fact_order_items.

Pattern (same as the other Gold DAGs):
    log_start -> build_summary -> log_finish

Strategy: full TRUNCATE + INSERT every run, inside ONE transaction.
TRUNCATE is transactional in Postgres, so a failed INSERT rolls back and
the previous table contents are restored.

Grain: one row per customer_id (all customers, including never-bought).
Valid orders = order_status NOT IN ('CANCELLED', 'RETURNED').
Cancelled / returned amounts live in their own columns.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_warehouse"
TABLE_NAME = "customer_summary"
LAYER = "gold"

BUILD_SQL = """
INSERT INTO gold.customer_summary (
    customer_id, full_name,
    total_orders, total_items, net_revenue,
    first_order_date, last_order_date,
    cancelled_orders, cancelled_revenue,
    returned_orders, returned_revenue
)
SELECT
    c.customer_id,
    c.full_name,

    COUNT(DISTINCT f.order_id) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED'))              AS total_orders,
    COALESCE(SUM(f.qty) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS total_items,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')), 0)          AS net_revenue,
    MIN(f.order_datetime::date) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED'))              AS first_order_date,
    MAX(f.order_datetime::date) FILTER (
        WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED'))              AS last_order_date,

    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'CANCELLED')  AS cancelled_orders,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status = 'CANCELLED'), 0)                             AS cancelled_revenue,
    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'RETURNED')   AS returned_orders,
    COALESCE(SUM(f.line_total) FILTER (
        WHERE f.order_status = 'RETURNED'), 0)                              AS returned_revenue
FROM silver.dim_customer c
LEFT JOIN silver.fact_order_items f ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name;
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


def build_summary(**context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.customer_summary;")
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
    rows = ti.xcom_pull(task_ids="build_summary")
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
    dag_id="gold_customer_summary",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by master_pipeline (phase 3)
    catchup=False,
    default_args={"on_failure_callback": mark_failed},
    tags=["gold", "customers"],
) as dag:
    t_start = PythonOperator(task_id="log_start", python_callable=log_start)
    t_build = PythonOperator(task_id="build_summary", python_callable=build_summary)
    t_finish = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t_start >> t_build >> t_finish