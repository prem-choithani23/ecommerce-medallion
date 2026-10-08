"""
Bronze DAG - states table
Pattern: FULL RELOAD (small reference table, no watermark needed)

Flow: log_start -> extract_and_load -> log_finish
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLE_NAME = "states"
LAYER = "bronze"


def log_start(**context):
    """Insert a RUNNING row into etl_log, push its log_id downstream via XCom."""
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    run_id = context["run_id"]

    log_id = pg.get_first(
        """
        INSERT INTO bronze.etl_log (table_name, layer, run_id, started_at, status)
        VALUES (%s, %s, %s, now(), 'RUNNING')
        RETURNING log_id;
        """,
        parameters=(TABLE_NAME, LAYER, run_id),
    )[0]

    context["ti"].xcom_push(key="log_id", value=log_id)


def extract_and_load(**context):
    """Pull all rows from MySQL, truncate + reload into bronze.states."""
    mysql = MySqlHook(mysql_conn_id="mysql_source")
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    run_id = context["run_id"]

    rows = mysql.get_records(
        "SELECT state_code, state_name FROM states;"
    )

    # Full reload = wipe and reinsert, inside one transaction.
    # If this fails partway, the transaction rolls back -> table is never left half-empty.
    conn = pg.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE bronze.states;")
        for state_code, state_name in rows:
            cur.execute(
                """
                INSERT INTO bronze.states
                    (state_code, state_name, _ingested_at, _source_system, _batch_id)
                VALUES (%s, %s, now(), 'mysql', %s);
                """,
                (state_code, state_name, run_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    context["ti"].xcom_push(key="rows_loaded", value=len(rows))


def log_finish(**context):
    """Mark the etl_log row SUCCESS and update etl_metadata.last_run_at."""
    pg = PostgresHook(postgres_conn_id="postgres_warehouse")
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="log_start", key="log_id")
    rows_loaded = ti.xcom_pull(task_ids="extract_and_load", key="rows_loaded")

    pg.run(
        """
        UPDATE bronze.etl_log
        SET finished_at = now(),
            rows_extracted = %s,
            rows_upserted = %s,
            status = 'SUCCESS'
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
        parameters=(TABLE_NAME, LAYER),
    )


with DAG(
    dag_id="bronze_states",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually for now; add a schedule once tested
    catchup=False,
    tags=["bronze", "full_reload"],
) as dag:

    t1 = PythonOperator(task_id="log_start", python_callable=log_start)
    t2 = PythonOperator(task_id="extract_and_load", python_callable=extract_and_load)
    t3 = PythonOperator(task_id="log_finish", python_callable=log_finish)

    t1 >> t2 >> t3