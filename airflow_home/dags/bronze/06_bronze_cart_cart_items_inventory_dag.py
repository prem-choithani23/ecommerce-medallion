"""
Bronze DAG FACTORY - timestamp-watermark tables (mutable, incremental).

v2 FIX: v1 used a strict `WHERE updated_at > last_timestamp` watermark.
Confirmed bug: this dataset's `updated_at` values are coarse (whole-day
granularity, tens of thousands of rows sharing the exact same timestamp -
e.g. 128,099 cart_items rows all stamped '2026-02-28'). With BATCH_SIZE=10,000,
the first batch of such a day advances the watermark to that day's timestamp,
and every subsequent query's strict `>` then PERMANENTLY excludes the rest of
that day's rows - they can never be picked up again. This is why
bronze.cart_items ended up with only 200,000 rows instead of millions.

FIX: compound watermark comparison - `(updated_at, pk) > (last_timestamp, last_id)`
instead of timestamp alone. This correctly handles ties: two rows with the
same updated_at are still ordered (and watermarked) by their pk, so nothing
with an equal timestamp gets silently skipped. Applies to `carts` and
`cart_items` (both have a single-column PK, so this reuses the existing
`last_id` column in etl_metadata - no schema change needed).

NOT applied to `inventory`: its PK is composite (warehouse_id, variant_id),
and etl_metadata only has one last_id slot - there's nowhere to store a
second ID column. inventory is empty today, so this is a documented but
currently-inert gap, not a live bug. If inventory gets populated later, this
needs a second watermark column added to etl_metadata first.

RESET REQUIRED before re-running: bronze.carts and bronze.cart_items were
loaded with the old buggy logic and are missing rows. TRUNCATE both tables
and reset their etl_metadata watermarks to NULL/0 before triggering this DAG,
so it does a full, correct backfill from scratch rather than resuming from a
wrong watermark. (SQL provided alongside this file.)
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "bronze"
BATCH_SIZE = 10_000

TABLES = {
    "carts": [
        "cart_id", "customer_id", "created_at", "updated_at", "channel",
        "cart_status", "abandoned_at", "converted_order_id",
    ],
    "cart_items": [
        "cart_item_id", "cart_id", "variant_id", "qty", "unit_price",
        "discount_amount", "added_at", "updated_at",
    ],
    "inventory": [
        "warehouse_id", "variant_id", "on_hand", "reserved", "updated_at",
    ],
}

PRIMARY_KEY = {
    "carts": ("cart_id",),
    "cart_items": ("cart_item_id",),
    "inventory": ("warehouse_id", "variant_id"),
}

# Tables where we can do the correct compound (timestamp, pk) watermark,
# because their PK is a single column that fits in etl_metadata.last_id.
COMPOUND_WATERMARK_TABLES = {"carts", "cart_items"}


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
    pk_cols = PRIMARY_KEY[table_name]
    col_list_sql = ", ".join(columns)
    insert_cols = col_list_sql + ", _ingested_at, _source_system, _batch_id"
    placeholders = ", ".join(["%s"] * len(columns)) + ", now(), 'mysql', %s"

    update_cols = [c for c in columns if c not in pk_cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    set_clause += ", _ingested_at = now(), _batch_id = EXCLUDED._batch_id"

    conflict_cols = ", ".join(pk_cols)
    return (
        f"INSERT INTO bronze.{table_name} ({insert_cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause};"
    )


def make_extract_and_load_batches(table_name: str, columns: list[str]):
    def extract_and_load_batches(**context):
        mysql = MySqlHook(mysql_conn_id="mysql_source")
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        run_id = context["run_id"]
        pk_cols = PRIMARY_KEY[table_name]
        col_list_sql = ", ".join(columns)
        upsert_sql = _build_upsert_sql(table_name, columns)
        order_by_sql = "updated_at, " + ", ".join(pk_cols)
        use_compound = table_name in COMPOUND_WATERMARK_TABLES

        # STEP 1: read the watermark.
        meta_row = pg.get_first(
            "SELECT last_timestamp, last_id FROM bronze.etl_metadata WHERE table_name = %s AND layer = %s;",
            parameters=(table_name, LAYER),
        )
        last_timestamp = meta_row[0] if meta_row and meta_row[0] is not None else datetime(1970, 1, 1)
        last_id = meta_row[1] if meta_row and len(meta_row) > 1 and meta_row[1] is not None else 0

        total_loaded = 0

        while True:
            if use_compound:
                # Compound comparison - correctly handles rows that tie on
                # updated_at by also comparing the single-column PK.
                pk_col = pk_cols[0]
                batch = mysql.get_records(
                    f"SELECT {col_list_sql} FROM {table_name} "
                    f"WHERE (updated_at, {pk_col}) > (%s, %s) "
                    f"ORDER BY {order_by_sql} LIMIT {BATCH_SIZE};",
                    parameters=(last_timestamp, last_id),
                )
            else:
                # inventory: composite PK, can't do compound watermark with
                # only one last_id slot available - falls back to the known-
                # limited timestamp-only comparison (harmless while empty).
                batch = mysql.get_records(
                    f"SELECT {col_list_sql} FROM {table_name} "
                    f"WHERE updated_at > %s ORDER BY {order_by_sql} LIMIT {BATCH_SIZE};",
                    parameters=(last_timestamp,),
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

            updated_at_idx = columns.index("updated_at")
            last_row = batch[-1]
            batch_max_ts = last_row[updated_at_idx]

            if use_compound:
                pk_idx = columns.index(pk_cols[0])
                batch_max_id = last_row[pk_idx]
                pg.run(
                    """
                    UPDATE bronze.etl_metadata
                    SET last_timestamp = %s, last_id = %s, last_run_at = now()
                    WHERE table_name = %s AND layer = %s;
                    """,
                    parameters=(batch_max_ts, batch_max_id, table_name, LAYER),
                )
                last_id = batch_max_id
            else:
                pg.run(
                    """
                    UPDATE bronze.etl_metadata
                    SET last_timestamp = %s, last_run_at = now()
                    WHERE table_name = %s AND layer = %s;
                    """,
                    parameters=(batch_max_ts, table_name, LAYER),
                )

            last_timestamp = batch_max_ts
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
        tags=["bronze", "timestamp_watermark", "incremental"],
    ) as dag:

        t1 = PythonOperator(task_id="log_start", python_callable=make_log_start(tbl_name))
        t2 = PythonOperator(
            task_id="extract_and_load_batches",
            python_callable=make_extract_and_load_batches(tbl_name, cols),
        )
        t3 = PythonOperator(task_id="log_finish", python_callable=make_log_finish(tbl_name))

        t1 >> t2 >> t3

    globals()[dag_id] = dag