"""
Master orchestrator DAG - runs the full pipeline in one trigger:
all Bronze DAGs, then all Silver DAGs, then the Gold DAGs.

Pattern: TriggerDagRunOperator ("push" cross-DAG orchestration) - this DAG
has no extraction/transform logic of its own, it only triggers the real
DAGs and waits for them.

Three phases, not one flat list:
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
  Phase 3 (Gold) - two steps, only after EVERY Silver DAG has finished:
      3a. gold_dim_date runs ALONE first. It reads Silver to find the date
          range and extends the calendar. Every other Gold table has a
          foreign key on gold.dim_date(date_key), so the calendar must be
          complete before they load.
      3b. The 5 other Gold DAGs run IN PARALLEL. They only read Silver and
          write their own table, so none depends on another Gold DAG.

deferrable=True on every trigger lets the wait happen in the triggerer
process instead of holding an executor slot - required on SequentialExecutor
(one task at a time), where a trigger task that blocks while waiting would
starve the very child DAG it is waiting for. The triggerer must be running.

wait_for_completion=True on every trigger means this DAG's own task doesn't
mark SUCCESS until the triggered DAG actually finishes (and FAILS if the
triggered DAG fails) - so a broken Bronze DAG correctly blocks Silver from
running on incomplete data, and a broken Silver DAG blocks Gold, rather than
silently racing ahead.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
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

# Gold step 3a - must finish before the others (they have a FK on dim_date)
GOLD_CALENDAR_DAG_ID = "gold_dim_date"

# Gold step 3b - independent of each other, run in parallel
GOLD_DAG_IDS = [
    "gold_daily_sales_summary",
    "gold_product_performance",
    "gold_customer_summary",
    "gold_cart_funnel",
    "gold_payment_summary",
]


def _trigger(dag_id):
    return TriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        # deferrable: while waiting for the child DAG, the task hands the wait
        # to the triggerer process and RELEASES its executor slot. Without
        # this, SequentialExecutor (1 slot) deadlocks: the waiting trigger task
        # holds the only slot, so the child DAG it waits for can never start.
        # Requires `airflow triggerer` to be running.
        deferrable=True,
        poke_interval=10,
    )


with DAG(
    dag_id="master_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestrator"],
) as dag:

    bronze_triggers = [_trigger(dag_id) for dag_id in BRONZE_DAG_IDS]
    silver_triggers = [_trigger(dag_id) for dag_id in SILVER_DAG_IDS]
    gold_calendar_trigger = _trigger(GOLD_CALENDAR_DAG_ID)
    gold_triggers = [_trigger(dag_id) for dag_id in GOLD_DAG_IDS]

    # Airflow does NOT support `list >> list` (raises TypeError at import).
    # A no-op barrier task gives the same "all of phase N before any of
    # phase N+1" rule with 24 edges instead of 80: list >> task >> list.
    bronze_complete = EmptyOperator(task_id="bronze_complete")

    # every Bronze trigger must finish before any Silver trigger starts
    bronze_triggers >> bronze_complete >> silver_triggers

    # every Silver trigger must finish before the calendar step starts
    # (list >> task is supported)
    silver_triggers >> gold_calendar_trigger

    # the calendar must finish before any other Gold trigger starts
    # (task >> list is supported)
    gold_calendar_trigger >> gold_triggers