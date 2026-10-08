"""
Bronze DAG FACTORY - small full-reload reference tables.

Same pattern as bronze_states_dag.py (log_start -> extract_and_load -> log_finish),
generalized so one file produces 9 separate DAGs instead of 9 copy-pasted files.

Each table still gets its OWN DAG (own schedule, own run history, own logs) -
this is a code-generation trick, not one giant DAG. In the Airflow UI you'll see:
  bronze_cities, bronze_categories, bronze_brands, bronze_attributes,
  bronze_attribute_values, bronze_warehouses, bronze_seller_profiles,
  bronze_product_tax_profiles, bronze_variant_attribute_values
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "bronze"

# table_name -> list of source columns, in order (same order used for SELECT and INSERT)
SMALL_TABLES = {
    "cities": ["city_id", "state_code", "city_name"],
    "categories": ["category_id", "category_name"],
    "brands": ["brand_id", "brand_name"],
    "attributes": ["attribute_id", "attribute_name"],
    "attribute_values": ["attribute_value_id", "attribute_id", "value_text"],
    "warehouses": ["warehouse_id", "warehouse_name", "city_id"],
    "seller_profiles": ["seller_id", "seller_name", "origin_state_code", "gstin", "created_at"],
    "product_tax_profiles": ["product_id", "hsn_code", "gst_rate", "effective_from", "effective_to"],
    "variant_attribute_values": ["variant_id", "attribute_id", "attribute_value_id"],
}


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


def make_extract_and_load(table_name: str, columns: list[str]):
    def extract_and_load(**context):
        mysql = MySqlHook(mysql_conn_id="mysql_source")
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        run_id = context["run_id"]

        col_list_sql = ", ".join(columns)
        rows = mysql.get_records(f"SELECT {col_list_sql} FROM {table_name};")

        insert_cols = col_list_sql + ", _ingested_at, _source_system, _batch_id"
        placeholders = ", ".join(["%s"] * len(columns)) + ", now(), 'mysql', %s"

        conn = pg.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE bronze.{table_name};")
            for row in rows:
                cur.execute(
                    f"INSERT INTO bronze.{table_name} ({insert_cols}) VALUES ({placeholders});",
                    tuple(row) + (run_id,),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        context["ti"].xcom_push(key="rows_loaded", value=len(rows))

    return extract_and_load


def make_log_finish(table_name: str):
    def log_finish(**context):
        pg = PostgresHook(postgres_conn_id="postgres_warehouse")
        ti = context["ti"]
        log_id = ti.xcom_pull(task_ids="log_start", key="log_id")
        rows_loaded = ti.xcom_pull(task_ids="extract_and_load", key="rows_loaded")

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


# --- DAG factory: one DAG object per table, registered into this module's globals ---
# --- DAG factory: one DAG object per table, registered into this module's globals ---
for tbl_name, cols in SMALL_TABLES.items():
    dag_id = f"bronze_{tbl_name}"

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,  # trigger manually for now
        catchup=False,
        default_args={
            "owner": "admin"
        },
        tags=["bronze", "full_reload", "small_table"],
    ) as dag:

        t1 = PythonOperator(
            task_id="log_start",
            python_callable=make_log_start(tbl_name),
            provide_context=True  # <-- ADD THIS LINE
        )
        t2 = PythonOperator(
            task_id="extract_and_load",
            python_callable=make_extract_and_load(tbl_name, cols),
            provide_context=True  # <-- ADD THIS LINE
        )
        t3 = PythonOperator(
            task_id="log_finish",
            python_callable=make_log_finish(tbl_name),
            provide_context=True  # <-- ADD THIS LINE
        )

        t1 >> t2 >> t3

    globals()[dag_id] = dag
