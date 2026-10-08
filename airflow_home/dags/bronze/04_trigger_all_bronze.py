from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# List of the tables matching your factory keys
SMALL_TABLES = [
    "cities", "categories", "brands", "attributes", "attribute_values",
    "warehouses", "seller_profiles", "product_tax_profiles", "variant_attribute_values"
]

with DAG(
    dag_id="trigger_all_bronze_tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Trigger manually from the UI with ONE button
    catchup=False,
    tags=["utility", "orchestration"],
) as dag:

    # Dynamically generate trigger tasks that run in parallel
    for table in SMALL_TABLES:
        TriggerDagRunOperator(
            task_id=f"trigger_bronze_{table}",
            trigger_dag_id=f"bronze_{table}",
            wait_for_completion=False, # Set to True if you want this controller to wait for logs
        )
