"""
Bronze DAG FACTORY - ID-watermark tables (append-only, incremental).

Tables: order_items, customers, customer_addresses, products, product_variants.
None of these are ever updated after creation - only new rows ever appear.

Pattern per table:
  log_start -> extract_and_load_batches (reads + advances last_id) -> log_finish

Key difference from orders/payments:
  - We READ last_id from etl_metadata before querying MySQL (that's the watermark).
  - WHERE pk > last_id means only NEW rows are ever pulled - not a full reload.
  - last_id is advanced in etl_metadata AFTER each batch commits, not once at
    the end. This matches the idempotency lesson: commit data first, advance
    checkpoint second, and do it per-batch so a mid-run crash only costs you
    the one batch you were on, not the whole run's progress.
  - Insert uses ON CONFLICT DO NOTHING, not DO UPDATE - these rows never change
    after creation, so there's nothing to update on a retry, just skip the ones
    that already made it in.

v2 fix: MySQL's tinyint(1) columns (is_default, is_active) come back from
mysql.get_records() as plain Python ints (0/1). The corresponding Bronze
columns were created as Postgres `boolean`, which does NOT implicitly cast
from integer on INSERT - psycopg2/Postgres raises:
    column "is_active" is of type boolean but expression is of type integer
Fix: explicitly cast those specific columns to Python bool before binding,
via the BOOLEAN_COLUMNS map + _cast_row() helper below.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "bronze"
BATCH_SIZE = 10_000

TABLES = {
    "order_items": [
        "order_item_id", "order_id", "product_id", "variant_id", "hsn_code",
        "gst_rate", "qty", "unit_price_taxable", "line_taxable", "discount_line",
        "cgst_line", "sgst_line", "igst_line", "gst_line_total", "line_total",
    ],
    "customers": [
        "customer_id", "first_name", "last_name", "email", "phone",
        "pan", "gstin", "created_at",
    ],
    "customer_addresses": [
        "address_id", "customer_id", "address_type", "line1", "line2",
        "city_id", "pincode", "is_default", "created_at",
    ],
    "products": [
        "product_id", "category_id", "brand_id", "product_name",
        "description", "is_active", "created_at",
    ],
    "product_variants": [
        "variant_id", "product_id", "sku", "mrp", "selling_price",
        "is_active", "created_at",
    ],
}

PRIMARY_KEY = {
    "order_items": "order_item_id",
    "customers": "customer_id",
    "customer_addresses": "address_id",
    "products": "product_id",
    "product_variants": "variant_id",
}

# table_name -> set of columns that are MySQL tinyint(1) but Postgres boolean.
# These need an explicit Python bool() cast before binding into the INSERT.
BOOLEAN_COLUMNS = {
    "customer_addresses": {"is_default"},
    "products": {"is_active"},
    "product_variants": {"is_active"},
}


def _cast_row(table_name: str, columns: list[str], row: tuple) -> tuple:
    bool_cols = BOOLEAN_COLUMNS.get(table_name)
    if not bool_cols:
        return row
    return tuple(
        (bool(val) if (col in bool_cols and val is not None) else val)
        for col, val in zip(columns, row)
    )


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


def make_extract_and_load_batches(table_name: str, columns: list[str]):
    def extract_and_load_batches(**context):
        mysql = MySqlHook(mysql_conn_id="mysql_source")
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        run_id = context["run_id"]
        pk = PRIMARY_KEY[table_name]
        col_list_sql = ", ".join(columns)

        insert_cols = col_list_sql + ", _ingested_at, _source_system, _batch_id"
        placeholders = ", ".join(["%s"] * len(columns)) + ", now(), 'mysql', %s"
        insert_sql = (
            f"INSERT INTO bronze.{table_name} ({insert_cols}) VALUES ({placeholders}) "
            f"ON CONFLICT ({pk}) DO NOTHING;"
        )

        # STEP 1: read the watermark - where did we leave off last time?
        last_id_row = pg.get_first(
            "SELECT last_id FROM bronze.etl_metadata WHERE table_name = %s AND layer = %s;",
            parameters=(table_name, LAYER),
        )
        last_id = last_id_row[0] if last_id_row and last_id_row[0] is not None else 0

        total_loaded = 0

        while True:
            # STEP 2: only ever ask for rows newer than the watermark
            batch = mysql.get_records(
                f"SELECT {col_list_sql} FROM {table_name} "
                f"WHERE {pk} > {last_id} ORDER BY {pk} LIMIT {BATCH_SIZE};"
            )
            if not batch:
                break

            conn = pg.get_conn()
            cur = conn.cursor()
            try:
                for row in batch:
                    cur.execute(
                        insert_sql,
                        _cast_row(table_name, columns, tuple(row)) + (run_id,),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
                conn.close()

            # STEP 3: advance the watermark to this batch's max id,
            # right after this batch's data is safely committed - not
            # after the whole loop finishes.
            batch_max_id = batch[-1][0]  # pk is first column, batch is ORDER BY pk
            pg.run(
                """
                UPDATE bronze.etl_metadata
                SET last_id = %s, last_run_at = now()
                WHERE table_name = %s AND layer = %s;
                """,
                parameters=(batch_max_id, table_name, LAYER),
            )

            last_id = batch_max_id
            total_loaded += len(batch)

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
        tags=["bronze", "id_watermark", "incremental"],
    ) as dag:

        t1 = PythonOperator(task_id="log_start", python_callable=make_log_start(tbl_name))
        t2 = PythonOperator(
            task_id="extract_and_load_batches",
            python_callable=make_extract_and_load_batches(tbl_name, cols),
        )
        t3 = PythonOperator(task_id="log_finish", python_callable=make_log_finish(tbl_name))

        t1 >> t2 >> t3

    globals()[dag_id] = dag