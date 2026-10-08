"""
Bronze DAG FACTORY - orders & payments (full reload, BATCHED, UPSERT-based).

v2: fixes a retry-duplication gap in v1. Previously TRUNCATE ran as a separate
task before the batch loop; if the loop crashed partway and Airflow retried
just the loop task, retry restarted at offset 0 and re-INSERTed rows that were
still sitting there from the failed attempt -> duplicates.

Fix: no TRUNCATE at all. Every batch uses INSERT ... ON CONFLICT (pk) DO UPDATE,
exactly like the original orders_analytics_dag pattern. This makes the batch
loop safe to retry from offset 0 as many times as needed - re-upserting a row
that's already correct is a no-op in effect, never a duplicate.

Pattern per table: log_start -> extract_and_load_batches (upsert) -> log_finish
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "bronze"
BATCH_SIZE = 10_000

TABLES = {
    "orders": [
        "order_id", "order_number", "cart_id", "customer_id",
        "shipping_address_id", "billing_address_id", "order_datetime",
        "channel", "payment_method", "order_status", "subtotal_taxable",
        "discount_total", "shipping_fee_taxable", "cgst", "sgst", "igst",
        "gst_total", "grand_total",
    ],
    "payments": [
        "payment_id", "order_id", "payment_method", "payment_status",
        "amount", "gateway_ref", "paid_at",
    ],
}

PRIMARY_KEY = {"orders": "order_id", "payments": "payment_id"}


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
        context["ti"].xcom_push(key="log_id", value=log_id)

    return log_start


def _build_upsert_sql(table_name: str, columns: list[str]) -> str:
    pk = PRIMARY_KEY[table_name]
    col_list_sql = ", ".join(columns)
    insert_cols = col_list_sql + ", _ingested_at, _source_system, _batch_id"
    placeholders = ", ".join(["%s"] * len(columns)) + ", now(), 'mysql', %s"

    # Every non-PK source column gets overwritten on conflict, plus _ingested_at
    # refreshed to "now" so you can see a row was touched by this run.
    update_cols = [c for c in columns if c != pk]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    set_clause += ", _ingested_at = now(), _batch_id = EXCLUDED._batch_id"

    return (
        f"INSERT INTO bronze.{table_name} ({insert_cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {set_clause};"
    )


def make_extract_and_load_batches(table_name: str, columns: list[str]):
    def extract_and_load_batches(**context):
        mysql = MySqlHook(mysql_conn_id="mysql_source")
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        run_id = context["run_id"]
        pk = PRIMARY_KEY[table_name]
        col_list_sql = ", ".join(columns)
        upsert_sql = _build_upsert_sql(table_name, columns)

        offset = 0
        total_loaded = 0

        while True:
            batch = mysql.get_records(
                f"SELECT {col_list_sql} FROM {table_name} "
                f"ORDER BY {pk} LIMIT {BATCH_SIZE} OFFSET {offset};"
            )
            if not batch:
                break

            conn = pg.get_conn()
            cur = conn.cursor()
            try:
                for row in batch:
                    cur.execute(upsert_sql, tuple(row) + (run_id,))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
                conn.close()

            total_loaded += len(batch)
            offset += BATCH_SIZE

        context["ti"].xcom_push(key="rows_loaded", value=total_loaded)

    return extract_and_load_batches


def make_log_finish(table_name: str):
    def log_finish(**context):
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        ti = context["ti"]
        log_id = ti.xcom_pull(task_ids="log_start", key="log_id")
        rows_loaded = ti.xcom_pull(task_ids="extract_and_load_batches", key="rows_loaded")

        pg.run(
            """
            UPDATE bronze.etl_log
            SET finished_at = now(), rows_extracted = %s, rows_upserted = %s, status = 'SUCCESS'
            WHERE log_id = %s;
            """,
            parameters=(rows_loaded, rows_loaded, log_id),
        )
        pg.run(
            """
            UPDATE bronze.etl_metadata
            SET last_run_at = now()
            WHERE table_name = %s AND layer = %s;
            """,
            parameters=(table_name, LAYER),
        )

    return log_finish


for tbl_name, cols in TABLES.items():
    dag_id = f"bronze_{tbl_name}"

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["bronze", "full_reload", "batched", "upsert"],
    ) as dag:

        t1 = PythonOperator(task_id="log_start", python_callable=make_log_start(tbl_name))
        t2 = PythonOperator(
            task_id="extract_and_load_batches",
            python_callable=make_extract_and_load_batches(tbl_name, cols),
        )
        t3 = PythonOperator(task_id="log_finish", python_callable=make_log_finish(tbl_name))

        t1 >> t2 >> t3

    globals()[dag_id] = dag