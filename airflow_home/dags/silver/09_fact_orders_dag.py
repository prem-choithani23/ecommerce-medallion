"""
Silver DAG - fact_order_items + fact_payments.

Same pattern as the other Silver DAGs: pure SQL, Postgres-to-Postgres,
full TRUNCATE + rebuild every run. See silver_dim_customer_dag.py for the
fuller reasoning on why full rebuild is fine at this data volume.

TWO SEPARATE FACT TABLES, deliberately not one:
  - fact_order_items: grain = one row per order_item_id (atomic grain).
  - fact_payments: grain = one row per payment_id.
  These are DIFFERENT grains. orders:order_items is 1:many, orders:payments
  is also 1:many (a payment could be split/retried) but NOT the same "many"
  as order_items - there's no natural 1:1 relationship between a specific
  order_item and a specific payment. Joining payment amounts onto the
  order-item-grain table would duplicate every payment once per line item
  of that order - a classic grain-mismatch bug. Keeping them as two fact
  tables, both joinable back to `orders`/`order_id`, is the correct fix.

No dependency between the two transforms - they run as independent branches.

Note the comment on unit_price_taxable in the DDL: it's SEMI-ADDITIVE.
Nothing in this DAG enforces that at write time (Postgres has no "don't sum
this column" constraint) - it's a modeling fact that downstream Gold/BI
queries need to respect, which is exactly why it's called out explicitly
in the column comment rather than left implicit.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "silver"

TRANSFORM_ORDER_ITEMS_SQL = """
TRUNCATE TABLE silver.fact_order_items;

INSERT INTO silver.fact_order_items (
    order_item_id, order_id, order_number, order_datetime, order_status,
    channel, customer_id, variant_id, hsn_code, gst_rate, qty,
    unit_price_taxable, line_taxable, discount_line,
    cgst_line, sgst_line, igst_line, gst_line_total, line_total
)
SELECT
    oi.order_item_id, oi.order_id, o.order_number, o.order_datetime,
    o.order_status, o.channel, o.customer_id, oi.variant_id,
    oi.hsn_code, oi.gst_rate, oi.qty, oi.unit_price_taxable,
    oi.line_taxable, oi.discount_line, oi.cgst_line, oi.sgst_line,
    oi.igst_line, oi.gst_line_total, oi.line_total
FROM bronze.order_items oi
JOIN bronze.orders o ON o.order_id = oi.order_id;
"""

TRANSFORM_PAYMENTS_SQL = """
TRUNCATE TABLE silver.fact_payments;

INSERT INTO silver.fact_payments (
    payment_id, order_id, payment_method, payment_status,
    amount, gateway_ref, paid_at
)
SELECT
    payment_id, order_id, payment_method, payment_status,
    amount, gateway_ref, paid_at
FROM bronze.payments;
"""


def make_log_start(table_name: str):
    def log_start(**context):
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        run_id = context["run_id"]
        log_id = pg.get_first(
            """
            INSERT INTO bronze.etl_log (table_name, layer, run_id, started_at, status)
            VALUES (%s, %s, %s, now(), 'RUNNING')
            RETURNING log_id;
            """,
            parameters=(table_name, LAYER, run_id),
        )[0]
        context["ti"].xcom_push(key=f"log_id_{table_name}", value=log_id)

    return log_start


def make_transform(table_name: str, sql: str):
    def transform(**context):
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        conn = pg.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows_loaded = cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        context["ti"].xcom_push(key=f"rows_loaded_{table_name}", value=rows_loaded)

    return transform


def make_log_finish(table_name: str):
    def log_finish(**context):
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        ti = context["ti"]
        log_id = ti.xcom_pull(task_ids=f"log_start_{table_name}", key=f"log_id_{table_name}")
        rows_loaded = ti.xcom_pull(task_ids=f"transform_{table_name}", key=f"rows_loaded_{table_name}")

        pg.run(
            """
            UPDATE bronze.etl_log
            SET finished_at = now(), rows_extracted = %s, rows_upserted = %s, status = 'SUCCESS'
            WHERE log_id = %s;
            """,
            parameters=(rows_loaded, rows_loaded, log_id),
        )

    return log_finish


with DAG(
    dag_id="silver_fact_orders",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "full_rebuild", "fact"],
) as dag:

    # --- branch 1: fact_order_items (atomic grain) ---
    oi_log_start = PythonOperator(
        task_id="log_start_fact_order_items",
        python_callable=make_log_start("fact_order_items"),
    )
    oi_transform = PythonOperator(
        task_id="transform_fact_order_items",
        python_callable=make_transform("fact_order_items", TRANSFORM_ORDER_ITEMS_SQL),
    )
    oi_log_finish = PythonOperator(
        task_id="log_finish_fact_order_items",
        python_callable=make_log_finish("fact_order_items"),
    )
    oi_log_start >> oi_transform >> oi_log_finish

    # --- branch 2: fact_payments (order grain, independent of branch 1) ---
    pay_log_start = PythonOperator(
        task_id="log_start_fact_payments",
        python_callable=make_log_start("fact_payments"),
    )
    pay_transform = PythonOperator(
        task_id="transform_fact_payments",
        python_callable=make_transform("fact_payments", TRANSFORM_PAYMENTS_SQL),
    )
    pay_log_finish = PythonOperator(
        task_id="log_finish_fact_payments",
        python_callable=make_log_finish("fact_payments"),
    )
    pay_log_start >> pay_transform >> pay_log_finish