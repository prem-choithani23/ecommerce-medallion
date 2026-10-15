# db-benchmark: TimescaleDB vs ClickHouse

Loads the Silver layer of the e-commerce warehouse into both engines with two Airflow DAGs, then times
analytical queries on each. Results and conclusions: [FINDINGS.md](FINDINGS.md).

## Layout
| Path | Purpose |
|---|---|
| `docker-compose.yml` | TimescaleDB (port 5433) and ClickHouse (ports 8123 / 9000) containers |
| `sql-scripts/01_timescale_schema.sql`, `02_clickhouse_schema.sql` | Table definitions (run by the DAGs) |
| `sql-scripts/03_timescaledb_order_id_index.sql` | Round 2: index on `order_id` |
| `sql-scripts/04_timescaledb_columnstore.sql` | Round 3: columnstore compression |
| `sql-scripts/05_clickhouse_materialized_view.sql` | Round 4: ClickHouse daily summary |
| `sql-scripts/06_timescaledb_continuous_aggregate.sql` | Round 4: TimescaleDB daily summary |
| `sql-scripts/timescaledb_q02_query_plan.sql` | EXPLAIN ANALYZE for Q02 |
| `analytical_queries.py` | The 13 queries, in both SQL dialects |
| `precomputed_summary_queries.py` | Round 4 queries (raw table vs summary) |
| `run_query_benchmark.py` | Times the 13 queries, checks both engines agree |
| `run_summary_benchmark.py` | Times the round 4 queries |
| `time_single_query.py` | Small demo: times one query |
| `results/` | One CSV per round, load-time logs, Q02 query plan |
| `../airflow_home/dags/db-test/` | `load_timescale.py` and `load_clickhouse.py` DAGs |

## One-time setup
Run from the project root with the virtualenv active and `AIRFLOW_HOME` set.

1. Start the containers:
   ```bash
   docker compose -f db-benchmark/docker-compose.yml up -d
   ```
2. Create the TimescaleDB database and extension (the database is not created by compose):
   ```bash
   docker exec -it bench_timescale psql -U postgres -c "CREATE DATABASE bench;"
   docker exec -it bench_timescale psql -U postgres -d bench -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
   ```
3. Add the Airflow connections (throwaway local credentials):
   ```bash
   airflow connections add timescale_bench --conn-type postgres --conn-host localhost \
       --conn-port 5433 --conn-login postgres --conn-password bench --conn-schema bench
   airflow connections add clickhouse_bench --conn-type generic --conn-host localhost \
       --conn-port 8123 --conn-login bench --conn-password bench --conn-schema bench
   ```
   The existing `postgres_warehouse` connection (Silver source) must also exist.

## Run order
```bash
# 1. Load (drops and recreates the tables each time)
airflow dags test load_timescale
airflow dags test load_clickhouse

# 2. Tuning scripts, in this order
docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/03_timescaledb_order_id_index.sql
docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/04_timescaledb_columnstore.sql   # optional, round 3
docker exec -i bench_timescale psql -U postgres -d bench < db-benchmark/sql-scripts/06_timescaledb_continuous_aggregate.sql
docker exec -i bench_clickhouse clickhouse-client --user bench --password bench --database bench --multiquery < db-benchmark/sql-scripts/05_clickhouse_materialized_view.sql

# 3. Benchmark (the label names the results CSV)
python db-benchmark/run_query_benchmark.py my_run
python db-benchmark/run_summary_benchmark.py
```

## Reload gotcha
Reloading with the DAGs drops and recreates the tables, which **wipes** the TimescaleDB index, the
columnstore settings, the continuous aggregate and the ClickHouse materialized view. After any reload,
re-run `03`, `04`, `06` (TimescaleDB) and `05` (ClickHouse) before benchmarking. The ClickHouse schema
script drops the old view first, so a reload never double-counts.

## Reading the output
- `MATCH` means both engines returned identical rows; `DIFFERENT` means do not trust the timing.
- "first" is the first run, "warm" the median of the other six.
- Differences under about 10 ms are connection overhead, not engine speed.
