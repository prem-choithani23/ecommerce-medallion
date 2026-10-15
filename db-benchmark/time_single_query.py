"""Time one analytical query on TimescaleDB and ClickHouse.

Run from the project root with the virtual environment active and
AIRFLOW_HOME set:
    python db-benchmark/time_single_query.py
"""
import statistics
import time

import clickhouse_connect
import psycopg2
from airflow.hooks.base import BaseHook

SQL = """
SELECT channel,
       COUNT(DISTINCT order_id) AS orders,
       SUM(qty)                 AS units,
       SUM(line_total)          AS net_revenue
FROM fact_order_items
WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY channel
ORDER BY channel
"""
RUNS = 5

# --- connect using the Airflow connections we created earlier ---------------
c = BaseHook.get_connection("timescale_bench")
pg = psycopg2.connect(host=c.host, port=c.port, user=c.login,
                      password=c.password, dbname=c.schema)
cur = pg.cursor()

c = BaseHook.get_connection("clickhouse_bench")
ch = clickhouse_connect.get_client(host=c.host, port=c.port, username=c.login,
                                   password=c.password, database=c.schema)


def run_pg():
    t = time.perf_counter()
    cur.execute(SQL)
    rows = cur.fetchall()
    return time.perf_counter() - t, rows


def run_ch():
    t = time.perf_counter()
    rows = ch.query(SQL).result_rows
    return time.perf_counter() - t, rows


for name, fn in [("TimescaleDB", run_pg), ("ClickHouse", run_ch)]:
    times = []
    for _ in range(RUNS):
        secs, rows = fn()
        times.append(secs)
    print(f"\n{name}")
    print("  runs (s):", ", ".join(f"{x:.3f}" for x in times))
    print(f"  first run: {times[0]:.3f}s   median of all: {statistics.median(times):.3f}s")
    print("  result:", rows)