"""
Silver DAG - dim_product (grain = variant_id) + dim_variant_attribute (EAV).

Same shape as silver_dim_customer_dag.py: pure SQL Postgres-to-Postgres
transform, full TRUNCATE + rebuild every run (small volume: 319 variants,
29 products - rebuilding is instant, no watermark/incremental complexity
needed). See that file's docstring for the fuller reasoning; not repeated here.

Two independent outputs this time, not a dependency chain:
  - dim_product does NOT need dim_variant_attribute to exist first (unlike
    dim_customer, which needed dim_customer_address for the default-address
    lookup). Attributes are a separate EAV table, not a column on dim_product.
  - So the two transforms can run as two independent branches, not a strict
    sequence - reflects that they really are independent outputs.

dim_product grain decision: ONE ROW PER VARIANT, not per product. A variant
(specific SKU, with its own mrp/selling_price) is what actually gets sold and
what an order_item references - so that's the right join target for a future
fact_orders table. Product-level fields (name, category, brand, tax profile)
are flattened in as repeated columns on every variant row of that product -
normal, deliberate denormalization for a dimension table.

product_tax_profiles has effective_from/effective_to (a profile can change
over time, e.g. a GST rate revision) - the LATERAL join picks whichever row
is CURRENTLY effective (effective_from <= now() AND (effective_to IS NULL OR
effective_to > now())), same "pick exactly one, deterministically" pattern
used for the customer's default address.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LAYER = "silver"

TRANSFORM_PRODUCT_SQL = """
TRUNCATE TABLE silver.dim_product;

INSERT INTO silver.dim_product (
    variant_id, product_id, product_name, description,
    category_id, category_name, brand_id, brand_name,
    hsn_code, gst_rate, sku, mrp, selling_price,
    product_is_active, variant_is_active,
    product_created_at, variant_created_at
)
SELECT
    v.variant_id, v.product_id, p.product_name, p.description,
    p.category_id, c.category_name, p.brand_id, b.brand_name,
    tp.hsn_code, tp.gst_rate, v.sku, v.mrp, v.selling_price,
    p.is_active, v.is_active,
    p.created_at, v.created_at
FROM bronze.product_variants v
JOIN bronze.products p ON p.product_id = v.product_id
LEFT JOIN bronze.categories c ON c.category_id = p.category_id
LEFT JOIN bronze.brands b ON b.brand_id = p.brand_id
LEFT JOIN LATERAL (
    SELECT hsn_code, gst_rate
    FROM bronze.product_tax_profiles tp
    WHERE tp.product_id = p.product_id
      AND tp.effective_from <= now()
      AND (tp.effective_to IS NULL OR tp.effective_to > now())
    ORDER BY tp.effective_from DESC
    LIMIT 1
) tp ON true;
"""

TRANSFORM_ATTRIBUTE_SQL = """
TRUNCATE TABLE silver.dim_variant_attribute;

INSERT INTO silver.dim_variant_attribute (
    variant_id, attribute_id, attribute_name, attribute_value_id, value_text
)
SELECT
    vav.variant_id, vav.attribute_id, a.attribute_name,
    vav.attribute_value_id, av.value_text
FROM bronze.variant_attribute_values vav
JOIN bronze.attributes a ON a.attribute_id = vav.attribute_id
JOIN bronze.attribute_values av ON av.attribute_value_id = vav.attribute_value_id;
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
    dag_id="silver_dim_product",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "full_rebuild", "dim_product"],
) as dag:

    # --- branch 1: dim_product ---
    prod_log_start = PythonOperator(
        task_id="log_start_dim_product",
        python_callable=make_log_start("dim_product"),
    )
    prod_transform = PythonOperator(
        task_id="transform_dim_product",
        python_callable=make_transform("dim_product", TRANSFORM_PRODUCT_SQL),
    )
    prod_log_finish = PythonOperator(
        task_id="log_finish_dim_product",
        python_callable=make_log_finish("dim_product"),
    )
    prod_log_start >> prod_transform >> prod_log_finish

    # --- branch 2: dim_variant_attribute (independent of branch 1) ---
    attr_log_start = PythonOperator(
        task_id="log_start_dim_variant_attribute",
        python_callable=make_log_start("dim_variant_attribute"),
    )
    attr_transform = PythonOperator(
        task_id="transform_dim_variant_attribute",
        python_callable=make_transform("dim_variant_attribute", TRANSFORM_ATTRIBUTE_SQL),
    )
    attr_log_finish = PythonOperator(
        task_id="log_finish_dim_variant_attribute",
        python_callable=make_log_finish("dim_variant_attribute"),
    )
    attr_log_start >> attr_transform >> attr_log_finish