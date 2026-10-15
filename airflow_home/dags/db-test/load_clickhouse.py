"""Load Silver tables into ClickHouse (benchmark target).

Timestamp note: Silver timestamps carry no time zone. The ClickHouse client
converts such values using the *local* time zone of the machine running this
code, which shifted every timestamp (by 5h30m on an India-time machine).
We tag every datetime as UTC before inserting, so the stored wall-clock value
is identical to Silver on any machine. verify() checks this explicitly.
"""
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

TABLES = ["dim_customer", "dim_product", "fact_payments",
          "fact_order_items", "fact_cart_items"]
BATCH = 200_000

# latest timestamp per fact table, compared between Silver and ClickHouse
TIME_COLS = {"fact_order_items": "order_datetime",
             "fact_cart_items": "cart_created_at"}

# airflow_home/dags/db-test/load_clickhouse.py -> project root is parents[3]
SCHEMA_SQL = Path(__file__).resolve().parents[3] / "db-benchmark" / "sql-scripts" / "02_clickhouse_schema.sql"


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
        meta = ch.query(
            "SELECT name, type FROM system.columns WHERE database='bench' AND table={t:String} "
            "ORDER BY position", parameters={"t": table}).result_rows
        cols = [name for name, _ in meta]
        # positions of DateTime columns, so we can tag them as UTC
        dt_idx = [i for i, (_, typ) in enumerate(meta) if "DateTime" in typ]
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
                if dt_idx:
                    fixed = []
                    for row in rows:
                        row = list(row)
                        for i in dt_idx:
                            if row[i] is not None:
                                row[i] = row[i].replace(tzinfo=timezone.utc)
                        fixed.append(row)
                    rows = fixed
                ch.insert(table, rows, column_names=cols,
                          settings={"max_partitions_per_insert_block": 1000})
        seconds = round(time.perf_counter() - t0, 2)

        rows_loaded = ch.query(f"SELECT count() FROM {table}").result_rows[0][0]
        ch.command(f"OPTIMIZE TABLE {table} FINAL")   # merge parts (not timed)
        print(f"{table}: {rows_loaded} rows loaded in {seconds}s")
        return {"table": table, "rows": rows_loaded, "seconds": seconds}

    @task
    def verify(results):
        ch = ch_client()
        bad = []
        with closing(pg_conn()) as src:
            cur = src.cursor()
            for r in results:
                t = r["table"]
                cur.execute(f"SELECT count(*) FROM silver.{t}")
                expected = cur.fetchone()[0]
                ok = expected == r["rows"]
                note = ""
                if t in TIME_COLS:
                    col = TIME_COLS[t]
                    cur.execute(f"SELECT max({col}) FROM silver.{t}")
                    silver_max = cur.fetchone()[0].replace(tzinfo=None)
                    ch_max = ch.query(f"SELECT max({col}) FROM {t}").result_rows[0][0]
                    ch_max = ch_max.replace(tzinfo=None)
                    time_ok = silver_max == ch_max
                    note = f" max({col}): silver={silver_max} clickhouse={ch_max} " \
                           f"{'OK' if time_ok else 'TIME SHIFT'}"
                    ok = ok and time_ok
                print(f"{t}: silver={expected} clickhouse={r['rows']} "
                      f"load={r['seconds']}s {'OK' if ok else 'MISMATCH'}{note}")
                if not ok:
                    bad.append(t)
        if bad:
            raise ValueError(f"Verification failed: {bad}")

    schema = setup_schema()
    loads = [load_table.override(task_id=f"load_{t}")(t) for t in TABLES]
    schema >> loads[0]
    for a, b in zip(loads, loads[1:]):
        a >> b
    verify(loads)


load_clickhouse()