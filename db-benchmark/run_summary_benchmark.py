"""Round 4: how much faster is a precomputed summary than the raw table?

Times the queries in precomputed_summary_queries.py on TimescaleDB and
ClickHouse, then reports:
  * which engine is faster on each query
  * whether both engines return the same answer
  * whether each summary query returns exactly the same rows as its raw-table twin
  * how many times faster each engine gets when it reads the summary

Usage (project root, virtualenv active, AIRFLOW_HOME set):
    python db-benchmark/run_summary_benchmark.py
    python db-benchmark/run_summary_benchmark.py my_label     # custom result file name

Results go to db-benchmark/results/<label>.csv (default round4_summaries).
Connection and timing helpers are reused from run_query_benchmark.py.
"""
import csv
import statistics
import sys
from pathlib import Path

from precomputed_summary_queries import SUMMARY_PAIRS, SUMMARY_QUERIES
from run_query_benchmark import RUNS, connect_ch, connect_pg, fingerprint, time_query

LABEL = sys.argv[1] if len(sys.argv) > 1 else "round4_summaries"
OUT = Path(__file__).resolve().parent / "results" / f"{LABEL}.csv"


def main():
    cur = connect_pg()
    ch = connect_ch()
    print(f"Round '{LABEL}': {RUNS} runs per query.\n")

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
    for q in SUMMARY_QUERIES:
        rec = {"id": q["id"], "kind": q["kind"], "name": q["name"]}
        for engine, runner in (("pg", pg_runner(q["pg"])), ("ch", ch_runner(q["ch"]))):
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

    # --- table: engine against engine ----------------------------------------
    print(f"\n{'Q':<6}{'what':<44}{'Timescale(s)':>13}{'ClickHouse(s)':>15}  {'faster':<22}{'same result?'}")
    print("-" * 112)
    for r in records:
        if r["pg_error"] or r["ch_error"]:
            r["winner"], r["ratio"], r["match"] = "ERROR", "", "n/a"
            print(f"{r['id']:<6}ERROR  pg: {r['pg_error'] or 'ok'} | ch: {r['ch_error'] or 'ok'}")
            continue
        pg_t, ch_t = r["pg_warm"], r["ch_warm"]
        if pg_t <= ch_t:
            r["winner"], r["ratio"] = "TimescaleDB", round(ch_t / max(pg_t, 1e-9), 1)
        else:
            r["winner"], r["ratio"] = "ClickHouse", round(pg_t / max(ch_t, 1e-9), 1)
        r["match"] = "MATCH" if r["pg_hash"] == r["ch_hash"] else "DIFFERENT"
        print(f"{r['id']:<6}{r['name']:<44}{pg_t:>13.4f}{ch_t:>15.4f}  "
              f"{r['winner'] + ' x' + str(r['ratio']):<22}{r['match']}")

    # --- table: raw table against summary, per engine ------------------------
    by_id = {r["id"]: r for r in records}
    print("\nSummary vs raw table (does the summary give the same rows, and how much faster is it?)")
    print("-" * 78)
    for raw_id, sum_id in SUMMARY_PAIRS:
        a, b = by_id.get(raw_id), by_id.get(sum_id)
        if not a or not b:
            continue
        for eng, name in (("pg", "TimescaleDB"), ("ch", "ClickHouse")):
            if a[f"{eng}_error"] or b[f"{eng}_error"]:
                print(f"{name:<12}{sum_id} vs {raw_id}: ERROR")
                continue
            same = a[f"{eng}_hash"] == b[f"{eng}_hash"]
            raw_t, sum_t = a[f"{eng}_warm"], b[f"{eng}_warm"]
            speedup = raw_t / max(sum_t, 1e-9)
            print(f"{name:<12}{sum_id} vs {raw_id}: {'IDENTICAL rows' if same else 'DIFFERENT rows'}   "
                  f"raw {raw_t:.4f}s -> summary {sum_t:.4f}s  ({speedup:.1f}x faster)")

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