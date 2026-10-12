"""Load Silver tables into ClickHouse (benchmark target)."""
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

TABLES = ["dim_customer", "dim_product", "fact_payments",
          "fact_order_items", "fact_cart_items"]
BATCH = 200_000

# airflow_home/dags/db-test/load_clickhouse.py -> project root is parents[3]
SCHEMA_SQL = Path(__file__).resolve().parents[3] / "db-benchmark" / "clickhouse_schema.sql"


def ch_client():
    c = BaseHook.get_connection("clickhouse_bench")
    return clickhouse_connect.get_client(host=c.host, port=c.port, username=c.login,
                                         password=c.password, database=c.schema)


def pg_conn():
    c = BaseHook.get_connection("postgres_warehouse")
    return psycopg2.connect(host=c.host, port=c.port, user=c.login,
                            password=c.password, dbname=c.schema)


@dag(dag_id="load_clickhouse", schedule=None, start_date=datetime(2026, 1, 1),
     catchup=False, tags=["benchmark", "clickhouse"])
def load_clickhouse():

    @task
    def setup_schema():
        ch = ch_client()
        for stmt in SCHEMA_SQL.read_text().split(";"):
            if stmt.strip():
                ch.command(stmt)

    @task
    def load_table(table: str):
        ch = ch_client()
        cols = [r[0] for r in ch.query(
            "SELECT name FROM system.columns WHERE database='bench' AND table={t:String} "
            "ORDER BY position", parameters={"t": table}).result_rows]
        col_sql = ", ".join(cols)

        t0 = time.perf_counter()
        with closing(pg_conn()) as src:
            cur = src.cursor(name="silver_reader")   # server-side cursor: streams rows
            cur.itersize = BATCH
            cur.execute(f"SELECT {col_sql} FROM silver.{table}")
            while True:
                rows = cur.fetchmany(BATCH)
                if not rows:
                    break
                ch.insert(table, rows, column_names=cols,
                          settings={"max_partitions_per_insert_block": 1000})
        seconds = round(time.perf_counter() - t0, 2)

        rows_loaded = ch.query(f"SELECT count() FROM {table}").result_rows[0][0]
        ch.command(f"OPTIMIZE TABLE {table} FINAL")   # merge parts (not timed)
        print(f"{table}: {rows_loaded} rows loaded in {seconds}s")
        return {"table": table, "rows": rows_loaded, "seconds": seconds}

    @task
    def verify(results):
        bad = []
        with closing(pg_conn()) as src:
            cur = src.cursor()
            for r in results:
                cur.execute(f"SELECT count(*) FROM silver.{r['table']}")
                expected = cur.fetchone()[0]
                ok = expected == r["rows"]
                print(f"{r['table']}: silver={expected} clickhouse={r['rows']} "
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


load_clickhouse()
