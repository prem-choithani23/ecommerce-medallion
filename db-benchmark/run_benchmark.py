"""Run every query in queries.py on TimescaleDB and ClickHouse and compare.

Usage (project root, virtualenv active, AIRFLOW_HOME set):
    python db-benchmark/run_benchmark.py baseline
    python db-benchmark/run_benchmark.py timescale_indexed   # later rounds

The argument is a label for this round. Results are written to
db-benchmark/results/<label>.csv and printed as a table.

For each query and each engine:
  * the query runs RUNS times
  * "first" = the first run (data may not be cached yet)
  * "warm"  = median of the remaining runs
  * the result rows are normalised and hashed, so we can check that both
    engines returned the SAME answer (a fast wrong answer does not count)
"""
import csv
import hashlib
import statistics
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import clickhouse_connect
import psycopg2
from airflow.hooks.base import BaseHook

from queries import QUERIES

RUNS = 7
LABEL = sys.argv[1] if len(sys.argv) > 1 else "baseline"
OUT = Path(__file__).resolve().parent / "results" / f"{LABEL}.csv"


# --- connections (the Airflow connections created earlier) -------------------
def connect_pg():
    c = BaseHook.get_connection("timescale_bench")
    conn = psycopg2.connect(host=c.host, port=c.port, user=c.login,
                            password=c.password, dbname=c.schema)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '300s'")
    return cur


def connect_ch():
    c = BaseHook.get_connection("clickhouse_bench")
    return clickhouse_connect.get_client(
        host=c.host, port=c.port, username=c.login,
        password=c.password, database=c.schema)


# --- result fingerprint -------------------------------------------------------
def norm(v):
    """Make values comparable across the two engines."""
    if v is None:
        return "NULL"
    if isinstance(v, Decimal):
        return str(v.quantize(Decimal("0.01")))
    if isinstance(v, float):
        return f"{v:.2f}"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def fingerprint(rows):
    text = "\n".join("|".join(norm(v) for v in row) for row in rows)
    return hashlib.md5(text.encode()).hexdigest()[:10]


# --- timing -------------------------------------------------------------------
def time_query(run_once):
    times, rows = [], None
    for _ in range(RUNS):
        t = time.perf_counter()
        rows = run_once()
        times.append(time.perf_counter() - t)
    return times, rows


def main():
    cur = connect_pg()
    ch = connect_ch()

    # a real order id for the point-lookup query (Q10)
    cur.execute("SELECT order_id FROM fact_order_items ORDER BY order_id OFFSET 500000 LIMIT 1")
    sample_order_id = cur.fetchone()[0]
    print(f"Round '{LABEL}': {RUNS} runs per query. Sample order id for Q10: {sample_order_id}\n")

    def pg_runner(sql):
        def go():
            cur.execute(sql)
            return cur.fetchall()
        return go

    def ch_runner(sql):
        def go():
            return ch.query(sql, settings={"max_execution_time": 300}).result_rows
        return go

    records = []
    for q in QUERIES:
        sql = {d: q[d] for d in ("pg", "ch")}
        if q.get("needs_sample_order_id"):
            sql = {d: s.format(sample_order_id=sample_order_id) for d, s in sql.items()}

        rec = {"id": q["id"], "kind": q["kind"], "name": q["name"]}
        for engine, runner in (("pg", pg_runner(sql["pg"])), ("ch", ch_runner(sql["ch"]))):
            try:
                times, rows = time_query(runner)
                rec[f"{engine}_first"] = round(times[0], 4)
                rec[f"{engine}_warm"] = round(statistics.median(times[1:]), 4)
                rec[f"{engine}_rows"] = len(rows)
                rec[f"{engine}_hash"] = fingerprint(rows)
                rec[f"{engine}_error"] = ""
            except Exception as exc:  # keep going, record the problem
                rec.update({f"{engine}_first": "", f"{engine}_warm": "",
                            f"{engine}_rows": "", f"{engine}_hash": "",
                            f"{engine}_error": str(exc).splitlines()[0][:150]})
        records.append(rec)
        print(f"  {q['id']} done", flush=True)

    # --- summary table --------------------------------------------------------
    print(f"\n{'Q':<4}{'kind':<9}{'Timescale(s)':>13}{'ClickHouse(s)':>15}  {'faster':<22}{'same result?':<12}")
    print("-" * 78)
    for r in records:
        if r["pg_error"] or r["ch_error"]:
            r["winner"], r["ratio"], r["match"] = "ERROR", "", "n/a"
            print(f"{r['id']:<4}{r['kind']:<9}  ERROR  pg: {r['pg_error'] or 'ok'} | ch: {r['ch_error'] or 'ok'}")
            continue
        pg_t, ch_t = r["pg_warm"], r["ch_warm"]
        if pg_t <= ch_t:
            r["winner"], r["ratio"] = "TimescaleDB", round(ch_t / max(pg_t, 1e-9), 1)
        else:
            r["winner"], r["ratio"] = "ClickHouse", round(pg_t / max(ch_t, 1e-9), 1)
        r["match"] = "MATCH" if r["pg_hash"] == r["ch_hash"] else "DIFFERENT"
        print(f"{r['id']:<4}{r['kind']:<9}{pg_t:>13.4f}{ch_t:>15.4f}  "
              f"{r['winner'] + ' x' + str(r['ratio']):<22}{r['match']:<12}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fields = ["id", "kind", "name", "pg_first", "pg_warm", "ch_first", "ch_warm",
              "winner", "ratio", "match", "pg_rows", "ch_rows", "pg_hash", "ch_hash",
              "pg_error", "ch_error"]
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)
    print(f"\nSaved: {OUT}")


if __name__ == "__main__":
    main()