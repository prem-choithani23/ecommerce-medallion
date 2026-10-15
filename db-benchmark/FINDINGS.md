# TimescaleDB vs ClickHouse: which database for which query

Same data in both engines (loaded from the Silver layer), 13 analytical queries plus 2 summary queries,
7 runs each. "Warm" = median of runs 2-7. Every result below was cross-checked: both engines return
identical rows (MATCH) for all 13 queries, so none of the speed differences come from wrong answers.

## Data and setup
- 1,310,429 order lines (`fact_order_items`), 5,963,986 cart lines (`fact_cart_items`), 439,248 payments,
  200,000 customers, 319 products. About 6 years of orders (Feb 2020 to Feb 2026), 73 monthly chunks/partitions.
- TimescaleDB 2.x on PostgreSQL 16 (hypertables, 1-month chunks). ClickHouse (MergeTree, partitioned by month).
- Both run in Docker on one machine, default settings, so the comparison is relative, not absolute.

## Result per query
| Query | Question | Type | TimescaleDB, index only | TimescaleDB, best of 3 rounds | ClickHouse | ClickHouse faster by | Verdict |
|---|---|---|---|---|---|---|---|
| Q01 | Revenue by channel | scan + group | 1.03 s | 926 ms | 27 ms | 34x | **ClickHouse** |
| Q02 | Monthly revenue trend | time bucket | 255 ms | 255 ms | 26 ms | 10x | **ClickHouse** |
| Q03 | Top 10 products by revenue | join, small dimension | 61 ms | 61 ms | 26 ms | 2x | **ClickHouse** |
| Q04 | Category revenue by month | join + time bucket | 247 ms | 247 ms | 42 ms | 6x | **ClickHouse** |
| Q05 | Cart funnel by channel (6M cart lines) | scan + group | 5.35 s | 4.95 s | 96 ms | 51x | **ClickHouse** |
| Q06 | Abandoned cart value by month | scan + time bucket | 5.15 s | 4.73 s | 83 ms | 57x | **ClickHouse** |
| Q07 | Payment status by method | scan, small table | 37 ms | 36 ms | 5.7 ms | 6x | **ClickHouse** |
| Q08 | Active customers per month | COUNT DISTINCT | 170 ms | 170 ms | 16 ms | 11x | **ClickHouse** |
| Q09 | Top 100 customers by revenue | high-cardinality group | 556 ms | 509 ms | 29 ms | 18x | **ClickHouse** |
| Q10 | Look up one order by id | point lookup | 3.2 ms | 3.2 ms | 2.9 ms | about equal | **Tie** |
| Q11 | Last 7 days by channel | recent time range | 77 ms | 75 ms | 7.6 ms | 10x | **ClickHouse** |
| Q12 | Daily revenue rollup | daily time bucket | 230 ms | 230 ms | 21 ms | 11x | **ClickHouse** |
| Q13 | Cart lines by status (6M rows) | full scan | 248 ms | 214 ms | 22 ms | 10x | **ClickHouse** |

TimescaleDB column "best of 3 rounds" is the fastest of: no tuning (round 1), `order_id` index (round 2),
columnstore compression (round 3). ClickHouse is shown untuned (round 1); it was not tuned at all.
A ratio between 0.8x and 1.25x counts as a tie. Differences under about 10 ms are mostly connection
overhead (HTTP vs native protocol), so Q07, Q10 and Q11 should not be used to rank the engines.

## Which database for which query
| Query shape | Better fit | Why |
|---|---|---|
| Scans and aggregations over millions of rows (Q01, Q05, Q06, Q13) | ClickHouse | Column storage reads only the needed columns and aggregates in parallel. Gap is 10x to 57x. |
| Time-bucket trends (Q02, Q04, Q11, Q12) | ClickHouse | About 10x faster. Partition pruning helps both, but ClickHouse scans far less data per row. |
| COUNT(DISTINCT) (Q08) | ClickHouse | PostgreSQL cannot parallelise COUNT(DISTINCT); it sorts per chunk in a serial plan. |
| Large GROUP BY with many groups (Q09) | ClickHouse | About 18x faster. |
| Joins to a small dimension (Q03) | ClickHouse (smaller gap, about 2x) | Both handle small joins well. |
| Look up one order by id (Q10) | Tie (use TimescaleDB if you need it) | With the `order_id` index TimescaleDB takes 3.2 ms against ClickHouse 2.9 ms. This is the one workload PostgreSQL is built for, and it is also where updates, constraints and transactions matter. |
| Known dashboard questions answered from a precomputed summary (Q14s, Q15s) | Tie | See round 4 below. |

Scoreboard: ClickHouse is clearly faster on 12 of 13 queries under every TimescaleDB configuration we
tried; 1 is a tie. TimescaleDB won none outright.

## What each tuning round changed (TimescaleDB)
| Round | Change | Effect |
|---|---|---|
| 1 | Baseline, no tuning | ClickHouse faster on all 13 (2.6x to 64.7x). |
| 2 | Index on `order_id` | Q10 dropped from 21.5 ms to 3.2 ms (about 7x). Everything else about 3-11% faster. |
| 2b | Index + `work_mem` 256MB | **Rejected.** Q02, Q04, Q08 and Q12 got about 2-3x slower. We do not know why for certain (plan not inspected after the change). Reset to default. |
| 3 | Columnstore compression | Storage dropped sharply (below), Q01/Q05/Q06/Q09 about 5-15% faster, but Q02, Q08, Q12 became 2-3x slower and Q10 lost its edge (12.8 ms). Repeat run varied by 1-4% only, so these are real, not noise. |
| 4 | Precomputed daily summary (continuous aggregate / materialized view) | Closes the gap for the questions the summary covers (below). |

## Round 4: precomputed summaries
| Query | TimescaleDB | ClickHouse |
|---|---|---|
| Q14 daily revenue by channel, raw table | 271 ms | 37 ms |
| Q14s same, from summary | 7.2 ms | 10.9 ms |
| Q15 monthly revenue, raw table | 253 ms | 16 ms |
| Q15s same, from summary | 1.9 ms | 1.9 ms |

Summary and raw queries return identical rows in both engines. A summary makes TimescaleDB 38x to 133x
faster and ClickHouse 3x to 8x faster, so the engines end up level for questions you know in advance.
Limits: the summary only answers questions at its own grain (day and channel); a new question still hits
the raw table, where ClickHouse wins. A continuous aggregate generally cannot hold COUNT(DISTINCT);
this was not tested here.

## Load time and storage
| Table | Rows | TimescaleDB load | ClickHouse load |
|---|---|---|---|
| dim_customer | 200,000 | 0.68 s | 0.94 s |
| dim_product | 319 | 0.00 s | 0.07 s |
| fact_payments | 439,248 | 0.72 s | 1.60 s |
| fact_order_items | 1,310,429 | 7.38 s | 12.24 s |
| fact_cart_items | 5,963,986 | 56.90 s | 52.97 s |
| **Total** | | **65.7 s** | **67.8 s** |

Loading is a tie: both are limited by reading Silver out of PostgreSQL, not by the target engine.
(The ClickHouse loader has extra Python work to tag timestamps as UTC.)

| Table | TimescaleDB plain | TimescaleDB columnstore | ClickHouse |
|---|---|---|---|
| fact_order_items | 252 MB | 81 MB | 74 MiB |
| fact_cart_items | 940 MB | 354 MB | 277 MiB |
| fact_payments | 38 MB | not converted | 13.7 MiB |
| dim_customer | 38 MB | not converted | 17.8 MiB |

ClickHouse uses about a third of TimescaleDB's plain storage. Columnstore closes most of the gap
(orders within about 10%, carts about 28% larger) but costs speed on some queries (round 3).

## Conclusions
1. For analytical, read-heavy workloads on this data, ClickHouse is the better engine: faster on 12 of 13
   queries, usually by 10x or more, with no tuning, and smaller on disk.
2. TimescaleDB is competitive only for point lookups (with an index) and for fixed dashboard questions
   served from a continuous aggregate. It also keeps PostgreSQL features (transactions, updates, joins
   to existing Postgres tables), which ClickHouse does not offer in the same way.
3. Tuning TimescaleDB is a trade-off, not a free win: `work_mem` and columnstore each sped some queries
   and slowed others. Measure per workload before applying.
4. The two are not exclusive: ClickHouse for exploratory analytics, TimescaleDB where the same store must
   also serve lookups and updates.

## Caveats
- One machine, one dataset of about 1.3M order lines. Larger data usually widens the gap further; we did not test it.
- Single-user timing only. Concurrency, cold-start (empty cache) and write-while-read were not tested.
- TimescaleDB is shown two ways because tuning is query-specific: "index only" is one fixed setup;
  "best of 3 rounds" picks the best configuration for each query (an upper bound no single setup achieves).
- ClickHouse numbers are from round 1 only. Round 2 and 3 values were within roughly the same range
  (about 14-40 ms for the 13 queries) but are not used in the table.
- The TimescaleDB plan for Q02 was inspected (serial aggregate over 73 per-chunk sorts); the `work_mem`
  slowdown explanation is a hypothesis, not confirmed.
- A timezone bug in the first ClickHouse load shifted timestamps by 5h30m and made 5 queries return
  different answers. It was fixed (UTC tagging) and a `max(timestamp)` check was added to the loader;
  all results above are from the corrected data.

## Reproducing
See `../README.md` in this folder. Raw timings are in `results/`, one CSV per round.
