"""Load Silver tables into TimescaleDB (benchmark target)."""
import tempfile
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

TABLES = ["dim_customer", "dim_product", "fact_payments",
          "fact_order_items", "fact_cart_items"]

# airflow_home/dags/load_timescale.py -> project root is 3 levels above this file
SCHEMA_SQL = Path(__file__).resolve().parents[3] / "db-benchmark" / "sql-scripts" / "01_timescale_schema.sql"


def connect(conn_id):
    c = BaseHook.get_connection(conn_id)
    return psycopg2.connect(host=c.host, port=c.port, user=c.login,
                            password=c.password, dbname=c.schema)


@dag(dag_id="load_timescale", schedule=None, start_date=datetime(2026, 1, 1),
     catchup=False, tags=["benchmark", "timescale"])
def load_timescale():

    @task
    def setup_schema():
        with closing(connect("timescale_bench")) as dst, dst.cursor() as cur:
            cur.execute(SCHEMA_SQL.read_text())
            dst.commit()

    @task
    def load_table(table: str):
        with closing(connect("postgres_warehouse")) as src, \
             closing(connect("timescale_bench")) as dst:
            dcur, scur = dst.cursor(), src.cursor()
            dcur.execute("""SELECT column_name FROM information_schema.columns
                            WHERE table_schema='public' AND table_name=%s
                            ORDER BY ordinal_position""", (table,))
            cols = ", ".join(r[0] for r in dcur.fetchall())

            t0 = time.perf_counter()
            with tempfile.TemporaryFile() as f:
                scur.copy_expert(f"COPY (SELECT {cols} FROM silver.{table}) TO STDOUT", f)
                f.seek(0)
                dcur.copy_expert(f"COPY {table} ({cols}) FROM STDIN", f)
            dst.commit()
            seconds = round(time.perf_counter() - t0, 2)

            dcur.execute(f"SELECT count(*) FROM {table}")
            rows = dcur.fetchone()[0]
            dcur.execute(f"ANALYZE {table}")   # refresh planner statistics
            dst.commit()
        print(f"{table}: {rows} rows loaded in {seconds}s")
        return {"table": table, "rows": rows, "seconds": seconds}

    @task
    def verify(results):
        bad = []
        with closing(connect("postgres_warehouse")) as src:
            cur = src.cursor()
            for r in results:
                cur.execute(f"SELECT count(*) FROM silver.{r['table']}")
                expected = cur.fetchone()[0]
                ok = expected == r["rows"]
                print(f"{r['table']}: silver={expected} timescale={r['rows']} "
                      f"load={r['seconds']}s {'OK' if ok else 'MISMATCH'}")
                if not ok:
                    bad.append(r["table"])
        if bad:
            raise ValueError(f"Row count mismatch: {bad}")

    schema = setup_schema()
    loads = [load_table.override(task_id=f"load_{t}")(t) for t in TABLES]
    schema >> loads[0]
    for a, b in zip(loads, loads[1:]):
        a >> b
    verify(loads)


load_timescale()
