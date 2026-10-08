"""
Silver DAG - dim_customer + dim_customer_address.

Unlike every Bronze DAG, this is a PURE SQL transform: Bronze and Silver both
live in the same Postgres database (ecommerce_warehouse), so there's no need
to pull rows into Python and loop - we can just run
    INSERT INTO silver.X SELECT ... FROM bronze.Y
directly inside Postgres, which is both simpler and much faster than row-by-row.

Strategy: FULL REBUILD (TRUNCATE + INSERT) every run, not incremental.
Why this is fine here, unlike Bronze's orders/payments full-reload:
  - Silver tables here are small (200K customers, ~400K addresses) - a full
    rebuild is seconds, not a real cost.
  - Silver has no watermark/upsert complexity to maintain - it always reflects
    "the current state of Bronze, transformed," which is exactly SCD Type 1
    semantics (overwrite on change, no history). A full rebuild every run
    trivially guarantees that, with no risk of stale rows lingering from a
    customer that no longer exists upstream (which an upsert-only approach
    could leave behind).
  - If Bronze volume grows large enough that a full Silver rebuild becomes
    expensive, the fix is incremental Silver (only reprocess customers whose
    Bronze row changed since last run) - not needed yet, so not built yet.

Task order matters: dim_customer_address must be rebuilt BEFORE dim_customer,
because dim_customer's "default address" columns are populated by looking up
into dim_customer_address.

Default-address selection: a customer could theoretically have zero or
multiple addresses flagged is_default = true (a Bronze-layer data-quality
question, not something Silver can fix). The LATERAL join below picks the
lowest address_id among any is_default=true rows for that customer (and NULL
if there are none) - deterministic either way, never fails or duplicates the
customer row.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "silver"

TRANSFORM_ADDRESS_SQL = """
TRUNCATE TABLE silver.dim_customer_address;

INSERT INTO silver.dim_customer_address (
    address_id, customer_id, address_type, line1, line2,
    city_id, pincode, is_default, created_at
)
SELECT
    address_id, customer_id, address_type, line1, line2,
    city_id, pincode, is_default, created_at
FROM bronze.customer_addresses;
"""

TRANSFORM_CUSTOMER_SQL = """
TRUNCATE TABLE silver.dim_customer;

INSERT INTO silver.dim_customer (
    customer_id, first_name, last_name, full_name, email, phone,
    pan, gstin, created_at,
    default_address_id, default_line1, default_line2,
    default_city_id, default_pincode
)
SELECT
    c.customer_id, c.first_name, c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email, c.phone, c.pan, c.gstin, c.created_at,
    da.address_id, da.line1, da.line2, da.city_id, da.pincode
FROM bronze.customers c
LEFT JOIN LATERAL (
    SELECT address_id, line1, line2, city_id, pincode
    FROM bronze.customer_addresses ca
    WHERE ca.customer_id = c.customer_id AND ca.is_default = true
    ORDER BY ca.address_id
    LIMIT 1
) da ON true;
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
            # row count of the INSERT (the statement right before it is TRUNCATE,
            # so cur.rowcount here reflects the INSERT, which runs last)
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
    dag_id="silver_dim_customer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "full_rebuild", "dim_customer"],
) as dag:

    # --- dim_customer_address (runs first - dim_customer depends on it) ---
    addr_log_start = PythonOperator(
        task_id="log_start_dim_customer_address",
        python_callable=make_log_start("dim_customer_address"),
    )
    addr_transform = PythonOperator(
        task_id="transform_dim_customer_address",
        python_callable=make_transform("dim_customer_address", TRANSFORM_ADDRESS_SQL),
    )
    addr_log_finish = PythonOperator(
        task_id="log_finish_dim_customer_address",
        python_callable=make_log_finish("dim_customer_address"),
    )

    # --- dim_customer (depends on dim_customer_address being fresh) ---
    cust_log_start = PythonOperator(
        task_id="log_start_dim_customer",
        python_callable=make_log_start("dim_customer"),
    )
    cust_transform = PythonOperator(
        task_id="transform_dim_customer",
        python_callable=make_transform("dim_customer", TRANSFORM_CUSTOMER_SQL),
    )
    cust_log_finish = PythonOperator(
        task_id="log_finish_dim_customer",
        python_callable=make_log_finish("dim_customer"),
    )

    addr_log_start >> addr_transform >> addr_log_finish >> cust_log_start >> cust_transform >> cust_log_finish