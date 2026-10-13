# Local measurements (blueprint §33)

All numbers come from `evidence/benchmark/olist_local_benchmark.json`, produced by
`commerce benchmark` on **one machine**: a cloud sandbox VM, 2 vCPU x86_64, 7.8 GB RAM, Linux 6.18,
PostgreSQL 16.15 and the application on the same host, nothing tuned. Dataset: the Olist archive
(sha256 `bc76c894…`), 27.6 MB of raw store, 451,535 source events in 217 files over 31 daily intervals.
They are not production figures and not a comparison with any other system. Re-run on your own machine before
quoting a number as yours.

| Experiment | Result |
|---|---|
| Baseline load: clean database, all 31 intervals, 5 serial runs of up to 7 intervals, each ending in a validated, published release | 137 s wall clock; 3,290 source rows/s end to end. Per run: ingestion 11 to 23 s, dbt build 12 to 19 s, manifest verification about 1.6 s |
| Daily replay: 30 one-day runs | cold start 11.8 s (a one-day database, so small); warm runs median 18.5 s, p95 24.1 s, range 11.0 to 24.4 s. The dbt build dominates (median 15.0 s, p95 19.4 s) and grows with the data already loaded because every candidate is a complete rebuild (D-013); ingestion median 3.4 s for about 13.9k rows a day |
| Duplicate replay | the last interval delivered again as a new run: 0 new events, 0 new receipts, run ends `NO_CHANGE` in 1.4 s, serving checksum unchanged |
| Recovery | publication aborted before commit on the last day: failed run 26.4 s, serving unchanged while failed, same run resumed in 1.6 s; 28.2 s from injected fault to restored, validated serving |
| Daily equals bulk | canonical hash over 11 business tables identical for the day-by-day database and the bulk database (A07 on the real data) |
| Query tuning | dashboard reads, 5 `EXPLAIN ANALYZE` runs each: sales 0.13 ms (634 rows), category view 3.1 ms (18,943 rows), one-category aggregate 2.0 ms, delivery 0.10 ms. All sequential scans. **No index was added**: nothing is slow enough for one to matter, and an index would add storage and write cost for no measured gain |
| Memory | application process tree peak 234 MB, PostgreSQL peak 395 MB (sum of proportional set size, sampled every 0.25 s). The Airflow services were not running here, so this is not the memory of the full Compose stack |
| Size | bulk database 0.89 GB (bronze events 335 MB, one served candidate 162 MB); the daily database reached 3.04 GB because it retained 31 candidate schemas, which is what the retention command (D-063) is for |
| Backup and restore (separate run, archive release) | dump 97.5 MB in 23 s (4 old candidate schemas excluded); restore into a separate database 14.9 s; serving checksum, current release, row counts, guard and role boundaries all equal; a damaged dump and a wrong manifest are refused (`evidence/recovery/olist_backup_restore.json`) |

## What this does not show

* A normal-sized production load: the replay is a historical snapshot cut into 31 days.
* Variance across machines, concurrent users, or a warm versus cold operating system cache beyond the one
  cold start noted above.
* Optimisation: the next measured candidate would be incremental marts, which the blueprint tells us to defer
  until the complete rebuild costs too much. At 18.5 s median it does not.
