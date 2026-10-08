"""
gold_payment_summary

Rebuilds gold.payment_summary from silver.fact_payments joined to one row
per order (order date + channel) taken from silver.fact_order_items.

Pattern (same as the other Gold DAGs):
    log_start -> build_summary -> log_finish

Strategy: full TRUNCATE + INSERT every run, inside ONE transaction.
TRUNCATE is transactional in Postgres, so a failed INSERT rolls back and
the previous table contents are restored.

Grain: one row per (order date, channel, payment_method).

Step 1 (CTE "ord") collapses fact_order_items to ONE row per order, so the
join to payments cannot multiply rows. Order-level attributes are identical
on every line item of an order; MAX() just picks that single value.

No cancelled/returned filter: payments are reported as recorded.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_warehouse"
TABLE_NAME = "payment_summary"
LAYER = "gold"

BUILD_SQL = """
INSERT INTO gold.payment_summary (
    date_key, order_date, channel, payment_method,
    total_payments, success_payments, failed_payments,
    pending_payments, refunded_payments,
    total_amount, success_amount, failed_amount,
    pending_amount, refunded_amount
)
WITH ord AS (
    SELECT
        order_id,
        MAX(order_datetime)::date AS order_date,
        MAX(channel)              AS channel
    FROM silver.fact_order_items
    GROUP BY order_id
)
SELECT
    to_char(o.order_date, 'YYYYMMDD')::int          AS date_key,
    o.order_date,
    o.channel,
    p.payment_method,

    COUNT(*)                                                              AS total_payments,
    COUNT(*) FILTER (WHERE p.payment_status = 'SUCCESS')                  AS success_payments,
    COUNT(*) FILTER (WHERE p.payment_status = 'FAILED')                   AS failed_payments,
    COUNT(*) FILTER (WHERE p.payment_status = 'PENDING')                  AS pending_payments,
    COUNT(*) FILTER (WHERE p.payment_status = 'REFUNDED')                 AS refunded_payments,

    COALESCE(SUM(p.amount), 0)                                            AS total_amount,
    COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'SUCCESS'), 0)  AS success_amount,
    COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'FAILED'), 0)   AS failed_amount,
    COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'PENDING'), 0)  AS pending_amount,
    COALESCE(SUM(p.amount) FILTER (WHERE p.payment_status = 'REFUNDED'), 0) AS refunded_amount
FROM silver.fact_payments p
JOIN ord o ON o.order_id = p.order_id
GROUP BY o.order_date, o.channel, p.payment_method;
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
            cur.execute("TRUNCATE TABLE gold.payment_summary;")
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
    dag_id="gold_payment_summary",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by master_pipeline (phase 3)
    catchup=False,
    default_args={"on_failure_callback": mark_failed},
    tags=["gold", "payments"],
) as dag:
    t_start = PythonOperator(task_id="log_start", python_callable=log_start)
    t_build = PythonOperator(task_id="build_summary", python_callable=build_summary)
    t_finish = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t_start >> t_build >> t_finish