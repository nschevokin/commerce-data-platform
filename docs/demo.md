# Demonstration script (about 10 minutes)

Everything runs locally with the golden fixture (no dataset download). Replace the fixture with the Olist archive
(`make import-data`, `make replay`) to demo the real data; the steps are the same, timings longer.

Prerequisites: `make setup && make bootstrap`; `COMMERCE_TEST_SERVER_DSN` pointing at a PostgreSQL 16 server you
may create databases on (for the throwaway-database steps).

## 1. The pipeline publishes a release (2 min)

```bash
make fixture-slice
```
Expected headline: eligible orders 2, merchandise BRL 130.00, freight 13.00, recorded payments 143.00, AOV 65.00,
on-time rate 50%. Say: these numbers are hand-calculated in `docs/golden_fixture.md`; dbt SQL and an independent
Python `Decimal` implementation both reproduce them.

## 2. The dashboard reads one release (1 min)

```bash
make dashboard        # then open http://127.0.0.1:8501
```
Show the Pipeline-evidence tab: the release id on every page is the same one. Say: the page uses a role that can
read `serving` only, inside a `READ ONLY REPEATABLE READ` transaction.

## 3. Redelivery changes nothing, a failure does not reach the dashboard (4 min)

```bash
commerce fault-matrix --server-dsn "$COMMERCE_TEST_SERVER_DSN" --only C03 C07 C08
```
Say: the matrix runs in a throwaway `commerce_demo_*` database and refuses any other name. The report shows
C03 (same file twice: no business effect), C07 (publication killed before commit: whole transaction rolls back,
views still show the prior release) and C08 (acknowledgement lost after commit: retry is a no-op, exactly one
publication). All 10 cases always run (they share state, about 1.5 minutes); `--only` limits the report. Full report: omit `--only`.

## 4. Backup, restore, parity (2 min)

```bash
make backup
make restore-drill DUMP=... MANIFEST=...
make parity
```
`parity` must print `"matches_committed": true`: the same raw files produce the same serving checksum on this
machine as on the reference machines.

## 5. Scheduler path (optional, 5 min)

```bash
make airflow-env
envs/airflow/.venv/bin/python scripts/airflow_scheduler_check.py
```
Starts the real Airflow processes, runs the DAG, forces a connection refusal and shows the scheduler retry it.

## What to say it is not

A local, small-data system; the stream is replayed history; there is no cloud deployment (see docs/operations_and_limits.md).
