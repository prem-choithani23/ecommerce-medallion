"""
Master orchestrator DAG - runs the full pipeline: all Bronze DAGs, then all
Silver DAGs, in one trigger.

Pattern: TriggerDagRunOperator ("push" cross-DAG orchestration) - this DAG
has no extraction/transform logic of its own, it only triggers the real
DAGs and waits for them.

Two phases, not one flat list:
  Phase 1 (Bronze) - all 20 Bronze DAGs run IN PARALLEL. They're safe to
    parallelize because each one reads its own MySQL table independently -
    none of them depend on another Bronze DAG's output.
  Phase 2 (Silver) - all 4 Silver DAGs run IN PARALLEL, but only after
    EVERY Bronze DAG has finished. Silver reads FROM Bronze (same Postgres
    database), so it must wait for the full Bronze set, not just "its own"
    source table - e.g. silver_dim_customer needs both bronze.customers AND
    bronze.customer_addresses, silver_fact_orders needs bronze.orders AND
    bronze.order_items, etc. Simplest correct rule: wait for ALL of Bronze,
    not a hand-picked subset per Silver DAG (less error-prone than trying to
    track exact per-table dependencies here).

wait_for_completion=True on every trigger means this DAG's own task doesn't
mark SUCCESS until the triggered DAG actually finishes (and FAILS if the
triggered DAG fails) - so a broken Bronze DAG correctly blocks Silver from
running on incomplete data, rather than silently racing ahead.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BRONZE_DAG_IDS = [
    # small full-reload reference tables
    "bronze_states", "bronze_cities", "bronze_categories", "bronze_brands",
    "bronze_attributes", "bronze_attribute_values", "bronze_warehouses",
    "bronze_seller_profiles", "bronze_product_tax_profiles",
    "bronze_variant_attribute_values",
    # full-reload, batched
    "bronze_orders", "bronze_payments",
    # ID-watermark
    "bronze_order_items", "bronze_customers", "bronze_customer_addresses",
    "bronze_products", "bronze_product_variants",
    # timestamp-watermark
    "bronze_carts", "bronze_cart_items", "bronze_inventory",
]

SILVER_DAG_IDS = [
    "silver_dim_customer",
    "silver_dim_product",
    "silver_fact_orders",
    "silver_fact_cart_items",
]

with DAG(
    dag_id="master_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestrator"],
) as dag:

    bronze_triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=10,
        )
        for dag_id in BRONZE_DAG_IDS
    ]

    silver_triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=10,
        )
        for dag_id in SILVER_DAG_IDS
    ]

    # every Bronze trigger must finish before any Silver trigger starts
    bronze_triggers >> silver_triggers