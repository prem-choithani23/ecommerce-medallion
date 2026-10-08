"""
gold_cart_funnel

Rebuilds gold.cart_funnel from silver.fact_cart_items.

Pattern (same as the other Gold DAGs):
    log_start -> build_funnel -> log_finish

Strategy: full TRUNCATE + INSERT every run, inside ONE transaction.
TRUNCATE is transactional in Postgres, so a failed INSERT rolls back and
the previous table contents are restored.

Grain: one row per (cart creation date, channel).

Step 1 (CTE "cart") collapses fact_cart_items to ONE row per cart, so the
counts below are plain COUNT(*) and a cart can never be counted twice.
Cart-level attributes are identical on every item row of a cart; MAX() just
picks that single value and guarantees one row per cart_id.

cart value = SUM(qty * unit_price - discount_amount)
(discount_amount is a per-line amount, confirmed from the data.)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_warehouse"
TABLE_NAME = "cart_funnel"
LAYER = "gold"

BUILD_SQL = """
INSERT INTO gold.cart_funnel (
    date_key, cart_date, channel,
    total_carts, active_carts, abandoned_carts,
    converted_carts, converted_carts_linked, expired_carts,
    items_in_carts, cart_value, abandoned_cart_value, converted_cart_value
)
WITH cart AS (
    SELECT
        cart_id,
        MAX(cart_created_at)::date                 AS cart_date,
        MAX(channel)                               AS channel,
        MAX(cart_status)                           AS cart_status,
        MAX(converted_order_id)                    AS converted_order_id,
        SUM(qty)                                   AS items,
        SUM(qty * unit_price - discount_amount)    AS value
    FROM silver.fact_cart_items
    GROUP BY cart_id
)
SELECT
    to_char(cart_date, 'YYYYMMDD')::int            AS date_key,
    cart_date,
    channel,

    COUNT(*)                                                               AS total_carts,
    COUNT(*) FILTER (WHERE cart_status = 'ACTIVE')                         AS active_carts,
    COUNT(*) FILTER (WHERE cart_status = 'ABANDONED')                      AS abandoned_carts,
    COUNT(*) FILTER (WHERE cart_status = 'CONVERTED')                      AS converted_carts,
    COUNT(*) FILTER (WHERE cart_status = 'CONVERTED'
                       AND converted_order_id IS NOT NULL)                 AS converted_carts_linked,
    COUNT(*) FILTER (WHERE cart_status = 'EXPIRED')                        AS expired_carts,

    COALESCE(SUM(items), 0)                                                AS items_in_carts,
    COALESCE(SUM(value), 0)                                                AS cart_value,
    COALESCE(SUM(value) FILTER (WHERE cart_status = 'ABANDONED'), 0)       AS abandoned_cart_value,
    COALESCE(SUM(value) FILTER (WHERE cart_status = 'CONVERTED'), 0)       AS converted_cart_value
FROM cart
GROUP BY cart_date, channel;
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


def build_funnel(**context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.cart_funnel;")
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
    rows = ti.xcom_pull(task_ids="build_funnel")
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
    dag_id="gold_cart_funnel",
    start_date=datetime(2026, 1, 1),
    schedule=None,            # triggered by master_pipeline (phase 3)
    catchup=False,
    default_args={"on_failure_callback": mark_failed},
    tags=["gold", "carts"],
) as dag:
    t_start = PythonOperator(task_id="log_start", python_callable=log_start)
    t_build = PythonOperator(task_id="build_funnel", python_callable=build_funnel)
    t_finish = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t_start >> t_build >> t_finish