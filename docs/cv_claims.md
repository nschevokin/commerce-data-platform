# CV and interview claims

Only capabilities that are implemented **and** backed by a test or a measured file. If you cannot point to the
evidence column in an interview, do not say it. Numbers come from one 2-vCPU VM unless stated.

## Safe to claim

| Claim (wording you can use) | Evidence |
|---|---|
| Built an idempotent, checksummed ingestion pipeline into PostgreSQL: one transaction per file, receipts make redelivery a no-op (0 new events on duplicate replay) | `tests/integration/test_ingestion.py`, `evidence/benchmark/olist_local_benchmark.json` |
| Modelled order money with exact `Decimal`/`numeric` and independent item/payment aggregation; reconciled 99,441 Olist orders to an independent reference with 0 differences | `evidence/bronze/olist_candidate_baseline_v1.json` |
| Isolated-candidate releases with 9 quality gates and atomic, writer-fenced publication; a dashboard role that reads exactly one release per refresh | `tests/integration/test_publication.py`, A09–A11 |
| Fault-injection matrix of 10 failure cases (killed ingestion/publication, stale writer, lost acknowledgement …) all recover; a mutant without the fencing check is caught | `evidence/recovery/fault_matrix.json` |
| Showed day-by-day processing equals a clean bulk load (equal canonical hash over 11 tables, on the real archive) | A07, `evidence/benchmark/` |
| Orchestrated with Airflow 3 (thin DAG, pools, retries); verified under a real scheduler, dag-processor and api-server with LocalExecutor + PostgreSQL metadata: pool limits held, a transient fault was retried | `evidence/orchestration/scheduler_check.json` |
| Airflow stack in Docker Compose starts healthy on macOS (api-server, scheduler, dag-processor; DAG and pools present); idle memory about 0.8 GB for the three services | D-068 |
| Backup/restore drill on a 97.5 MB dump restored in 14.9 s with checksum, release and role boundary verified | `evidence/recovery/olist_backup_restore.json` |
| Measured: 137 s bulk load of 451k events; daily run median 18.5 s (p95 24.1 s); peak 234 MB app + 395 MB PostgreSQL | `docs/benchmark.md` |
| Reproducibility: golden fixture produces identical serving checksums on Linux x86_64 and macOS arm64 (and CI once pushed) | `make parity`, `evidence/parity/` |
| Documented 68 design decisions, including corrections to the original specification | `docs/decisions.md` |

Stack to list: Python 3.12, PostgreSQL 16, dbt Core, Airflow 3, Docker Compose, Streamlit, pytest, ruff, GitHub Actions
(only after the CI workflow has run green).

## Do not claim

* Any AWS/cloud deployment, S3, EC2, "production" or "at scale". None exists (D-066).
* Spark, streaming/CDC, Kafka, real-time. The stream is a replayed historical snapshot.
* Throughput or latency figures without saying "one 2-vCPU VM, ~450k events".
* That a pipeline run executed inside the Airflow containers, or that the **dashboard** Docker image was built and run (neither verified). The Airflow stack itself starting healthy is verified (D-068).
* That CI passes (until the workflow has run on GitHub).
* Sensor reschedule behaviour under a real scheduler (covered by `dag.test` only).

## Questions you should be ready for

* *Why not incremental models?* Complete rebuild is 18.5 s median; incremental marts add late-data complexity
  that nothing measured justifies (D-013).
* *Why one PostgreSQL?* The correctness mechanisms (atomic publication, privileges, fencing) are database
  guarantees; a second engine would only add a consistency boundary. State plainly that this is a small-data design.
* *What would change at 100x data?* Partitioned bronze, incremental marts, object-store raw layer, a warehouse
  that separates storage from compute — none of which is built or measured here.
* *What was wrong in the original specification?* See D-028 and the other "corrects the blueprint" decisions.
