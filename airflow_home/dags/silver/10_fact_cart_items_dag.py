"""
Silver DAG - fact_cart_items (cart/funnel fact table).

Same pattern as every other Silver DAG: pure SQL, Postgres-to-Postgres,
full TRUNCATE + rebuild every run.

Grain: one row per cart_item_id - same atomic-grain logic as fact_order_items.
cart_status/channel/abandoned_at/converted_order_id are degenerate dimensions
pulled from the parent cart.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "silver"

TRANSFORM_CART_ITEMS_SQL = """
TRUNCATE TABLE silver.fact_cart_items;

INSERT INTO silver.fact_cart_items (
    cart_item_id, cart_id, customer_id, cart_status, channel,
    cart_created_at, cart_updated_at, abandoned_at, converted_order_id,
    variant_id, qty, unit_price, discount_amount, added_at, updated_at
)
SELECT
    ci.cart_item_id, ci.cart_id, c.customer_id, c.cart_status, c.channel,
    c.created_at, c.updated_at, c.abandoned_at, c.converted_order_id,
    ci.variant_id, ci.qty, ci.unit_price, ci.discount_amount,
    ci.added_at, ci.updated_at
FROM bronze.cart_items ci
JOIN bronze.carts c ON c.cart_id = ci.cart_id;
"""


def log_start(**context):
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    run_id = context["run_id"]
    log_id = pg.get_first(
        """
        INSERT INTO bronze.etl_log (table_name, layer, run_id, started_at, status)
        VALUES (%s, %s, %s, now(), 'RUNNING')
        RETURNING log_id;
        """,
        parameters=("fact_cart_items", LAYER, run_id),
    )[0]
    context["ti"].xcom_push(key="log_id", value=log_id)


def transform(**context):
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = pg.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(TRANSFORM_CART_ITEMS_SQL)
        rows_loaded = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)


def log_finish(**context):
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="log_start", key="log_id")
    rows_loaded = ti.xcom_pull(task_ids="transform", key="rows_loaded")

    pg.run(
        """
        UPDATE bronze.etl_log
        SET finished_at = now(), rows_extracted = %s, rows_upserted = %s, status = 'SUCCESS'
        WHERE log_id = %s;
        """,
        parameters=(rows_loaded, rows_loaded, log_id),
    )


with DAG(
    dag_id="silver_fact_cart_items",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "full_rebuild", "fact"],
) as dag:

    t1 = PythonOperator(task_id="log_start", python_callable=log_start)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t1 >> t2 >> t3