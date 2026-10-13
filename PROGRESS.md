# Progress record

_Last updated: 2026-10-12 (session 1, T10 local)._ Baseline spec: `Ecommerce_Data_Platform_Blueprint.pdf` v1.0
(kept outside the repo; it is addressed to a different person, see D-017).

## Where we are

| Ticket | Status |
|--------|--------|
| T01 repo, locks, Compose DBs, migrations, roles | **Done**: Compose boot unverified (Docker Hub blocked in build sandbox) |
| T02 golden fixture, metric definitions, source profile | **Done**: archive profile waits for the dataset (A02) |
| T03 archive inventory, replay generator, manifests | **Done on the fixture**: real archive run waits for the dataset (A02) |
| T04 transactional ingestion + receipts | **Done** (sandbox): full Olist baseline ingested; Mac run pending |
| T05 bronze to staging, dimensions, facts, marts | **Done** (sandbox): archive candidate equals an independent reference |
| T06 isolated release, quality gates, atomic publication | **Done** (sandbox): 154 tests pass; archive published and read via the dashboard role; Mac run pending |
| T07 Airflow task graph, stage ledger, retries, guard | **Done in the sandbox**: DAG executed by Airflow 3.3.2 (`dag.test`); Docker image and services written, NOT run |
| T08 backfills, corrections, recovery, clean-vs-daily equality | **Done in the sandbox** (A07 PASS; fault matrix 10/10); Mac: 181 passed, 1 skipped |
| T09 Streamlit dashboard | **Done in the sandbox** (headless page tests, archive screenshots); Docker image not run |
| T10 benchmark, backup/restore, parity | **Done in the sandbox and (benchmark runner, backup) on the Mac**; AWS dropped (D-066), replaced by environment parity |
| T11 README, diagram, runbooks, demo | Not started |

Acceptance criteria: A02, A03, A04, A05, A06, A08 (ingestion), A09, A10, A11 PASS in the sandbox. A01 PARTIAL. A07, A12, A13, A14 PENDING. Detail in
`docs/traceability.md`.

## Verification (session 1)

Environment: Ubuntu 24.04, 2 vCPU, 7 GiB RAM. PostgreSQL 16.15 (native, `trust` auth, port 5433).
Python 3.12.13 via uv 0.11.32. dbt-core 1.12.5 + dbt-postgres 1.11.0. psycopg 3.3.5.

```bash
export COMMERCE_TEST_SERVER_DSN="host=127.0.0.1 port=5433 user=postgres"
make lint        # ruff: all checks passed; dbt parse OK
make test        # see results below
```

Results:
- **Clean-checkout run:** `git clone` → `make setup` → `make lint` → `make test`: **52 passed** (45 s).
  This covers 18 unit tests (money/status contracts, Decimal oracle) and 34 integration tests on a
  throwaway database: migrations, role boundaries, golden fixture, fan-out mutant, 5 broken inputs.
- `commerce fixture-slice`: dbt 68/68 nodes executed, 0 failures, 1 expected warning (unmapped categories).
  SQL == oracle, Python == oracle. Evidence: `evidence/fixture/golden_v1_latest.json`.
- Reader-lock experiment (D-004): `evidence/experiments/2026-10-10_rr_reader_lock.out`.

**Owner's Mac (macOS, Docker Desktop, Compose PostgreSQL 16, SCRAM password auth), session 2:**
`make setup`, `make bootstrap` (image build, healthy DB, migrations, role logins), `make lint`, `make test`
(**52 passed**, 34 s) and `make fixture-slice` (passed: dbt 68/68, SQL == oracle, Python == oracle, eligible
2 / 130.00 / 13.00 / 143.00 / AOV 65.00) all worked. The first run exposed D-020 (test fixture overwrote real
role passwords); after the fix, `make test` followed by `make fixture-slice` passes on the Mac.
Setup lesson: keep the project outside `~/Downloads`; a moved folder breaks the venv and the Compose bind mounts.

**Not verified:** the CI workflow; the Airflow image (T07); the Apple-Silicon vs Intel architecture of the Mac was not recorded.

## Open issues

1. **D-005**: adopted by default; owner may still change it before T04.
2. **D-009**: pin image digests on first pull with registry access.
3. **Dataset access**: the Olist archive must come from the owner (Kaggle login). Needed for A02 and T03 profiling.
   It does not block T03 code or the fixture path.
4. **D-017**: no references to the owner's background anywhere in the repository (decided).

## Estimate

Spent this session: roughly the "design and contracts" package plus part of "SQL modelling". The 40–60 h core
estimate stands. Risk added: container registry access must be confirmed on the owner's machine before T07.

## T03 results (sandbox)

75 unit tests pass (`pytest tests/unit`), ruff clean. Fixture replay: 31 intervals, 21 rows. Golden content
digest pinned in tests. Mac: `make test` 109 passed (75 unit + 34 integration), pinned digest matches, uncompressed file hashes identical; gzip/descriptor hashes differ by zlib build (D-021, see `docs/replay_and_manifests.md`).
Not measured: performance on the full archive.

## T04 results (sandbox)

135 tests pass (75 unit + 60 integration), ruff clean. Full Olist baseline ingested through the `commerce_ingest`
role: 451,535 events, 217 receipts (all reconcile), 31 intervals COMPLETE, bronze 322 MB. First load 57 s and a
no-op rerun 14 s on 2 vCPU: one run, labelled as such, not a benchmark. Evidence:
`evidence/ingestion/olist_ingest_baseline_v1.json`. Three mutants (no insert-count check, no conflict check, no fence
check) each break a test. Not verified: ingestion on the owner's Mac; Airflow calling these functions (T07).

## T05 results (sandbox)

143 tests pass (75 unit + 68 integration), ruff clean. Golden A/B/C totals now come out of
replay files, ingestion, bronze and dbt (not only the CSV harness). Duplicate delivery gives the same business
checksum; late B changes 2017-03-11 only when it arrives; the correction moves AOV 65.00 to 67.50 only after it
arrives. Full Olist archive: candidate built in 17 s (76 dbt nodes, 0 failures, 2 expected warnings), compared
with an independent Decimal reference computed from the original CSVs: 99,441 orders, 634 days, 74 categories,
0 differences. Evidence: `evidence/bronze/olist_candidate_baseline_v1.json`. Two mutants (wrong revision order,
no frontier filter) each fail tests. Not verified on the owner's Mac; no performance claim beyond these runs.

Archive facts that matter next: eligible orders 98,207 of 99,441 (1,234 canceled or unavailable); merchandise
13,494,400.74; AOV 137.42; on-time 89,936 of 96,470 delivered with both dates (93.2%). 576 orders have a
payment-vs-items gap (warn); 623 products have no mapped category (warn).

## T06 result (2026-10-12)

`commerce release` runs one fenced run: frontier check, NO_CHANGE if the input digest equals the serving
release, candidate build, 9 quality checks, publication transaction, dashboard-role verification.
`commerce rollback` and `commerce serving-status` complete the operator surface. Tests: 11 new integration
tests (real PostgreSQL, real dbt builds, 41 s); full suite 154 passed. A mutant without the contract and
frontier checks fails 2 tests. Real archive: release 3454c526 published as generation 1 (dbt 78 nodes, all
blocking checks passed, 2 warnings: 576 payment gaps, 623 unmapped categories); rerun gives NO_CHANGE; the
dashboard role reads 634 sales days, 18,943 category-days, 556 delivery cohorts, 7 health rows. Evidence:
`evidence/release/olist_release_baseline_v1.json`. A race I found and fixed (D-038): idle-in-transaction
connection blocking the publisher.

Known gaps: candidate schema retention/GC (§23) not implemented; ingest and release still use separate runs
(T07 must make one run id hold the guard across ingest, build and publish); not run on the owner's Mac yet.

## T07 result (2026-10-12)

`dags/commerce_pipeline.py` is a thin DAG (14 task ids, ingestion mapped over the 7 sources, pools
`commerce_ingest` 2 / `commerce_transform` 1, 2 retries with backoff, `max_active_runs=1`, paused by default,
`pipeline_result` as the only leaf with `all_done`). Everything it calls is in `commerce.orchestration`
(decisions D-039 to D-046). `commerce pipeline run` drives the same stages for serial ranges of at most 7
intervals.

Verified in the sandbox: 17 integration tests of the stages (resume after a kill, rival run refused, no-change
reruns, rejected data, terminal stage raises on missing evidence; a mutant that makes the terminal task always
succeed fails 7 of them) and 6 Airflow tests that run the real DAG with Airflow 3.3.2 (`make airflow-env`,
`make test-airflow`): happy path, mapped ingestion, bad window, rejected source file. Full Olist replay: 31
intervals in 5 serial runs, 131 s wall clock (one run, 2 vCPU sandbox), final release equals the independent
reference (0 differences). Evidence: `evidence/orchestration/olist_serial_ranges.json`.

NOT verified: the scheduler/executor path (pools, sensor rescheduling, retry timers, a real `@daily` trigger),
the Airflow Docker image and Compose services (`docker compose config` parses; nothing built or started), and
all of it on the owner's Mac. Run `make bootstrap`, then `make airflow-up` to check the stack.

Owner's Mac (2026-10-12): `make lint` clean, `make test` 171 passed, 1 skipped (3:45). A compose bug (a required variable of the airflow profile broke `make bootstrap`) was found there and fixed.

## T08 result (2026-10-12)

Added `audit/equality.py`, `orchestration/backfill.py` (`commerce backfill --dry-run|--mode audit|publish`),
`recovery/{demo_db,matrix}.py` (`commerce fault-matrix`). Decisions D-047 to D-053. A07 is PASS on the golden
fixture (two databases, equal canonical hash; negative control differs only where a correction should).
The §26 matrix passes 10/10 and a mutant without the write-fence check fails C09. Owner's Mac (2026-10-12): `make test` 181 passed, 1 skipped (6:39), migrations already applied. Not verified: the matrix on
the Olist archive, real process kills.

## T09 result (2026-10-12)

`dashboard/app.py` (four tabs: Overview, Categories, Delivery, Pipeline evidence) over `commerce.dashboard`
(`data.py` one-release bundle, STALE/UNAVAILABLE handling; `metrics.py` exact Decimal aggregation). Decisions
D-054 to D-059; own environment `envs/dashboard` (`make dashboard-env`, `make test-dashboard`, `make dashboard`
to serve on 127.0.0.1:8501). Verified in the sandbox: 8 unit tests, 3 PostgreSQL tests against a real published
release (golden totals equal), 4 headless Streamlit tests; breaking the AOV formula fails both the unit and the
page test. The page on the Olist archive release shows 98,207 eligible orders and BRL 13,494,400.74 (equal to
the independent reference); looking at the screenshots found the category chart unsorted, now fixed.
Bundle read on the archive: median 0.061 s, p95 0.087 s over 30 reads (one sandbox, not a benchmark).

Owner's Mac (2026-10-12): `make test` 192 passed, 2 skipped (6:10); `make dashboard-env` + `make test-dashboard` 4 passed.

NOT verified: the dashboard Docker image and compose service (`make dashboard-up`), the page opened in a browser on the owner's Mac,
any accessibility audit, a browser CSV download.

## T10 local result (2026-10-12)

`commerce benchmark` (Olist archive, one sandbox VM): bulk load 137 s; 30 daily runs warm median 18.5 s, p95 24.1 s
(the full dbt rebuild dominates); duplicate delivery adds nothing; recovery after a killed publication 28.2 s;
daily and bulk databases have identical canonical hashes (A07 on real data); dashboard queries all under 4 ms, no
index added; peak 234 MB application, 395 MB PostgreSQL. Details and limits: `docs/benchmark.md`.
`commerce backup` / `restore-drill` (decisions D-062, D-064, D-065): archive release dump 97.5 MB, restore 14.9 s,
all checks equal. `commerce candidates-gc` (D-063) fixes the growth the benchmark exposed (31 retained
candidates = 3.0 GB). New tests 11 (unit 3, integration 8). Owner's Mac (2026-10-12): `make test` 196 passed, 9 skipped with no `pg_dump` on the PATH; after `brew install postgresql@16` (pg_dump 16.15) the 7 backup/retention tests pass. Not verified: the
memory of the full Compose stack with Airflow, anything on AWS.

## AWS dropped, parity instead (2026-10-12)

The owner has no AWS account, so A12 as written (EC2 + S3) is not met and will not be claimed (D-066). Substitute:
`commerce parity` (also run by `make test`, `make parity`) replays the golden fixture from raw files into a
throwaway database and compares the serving checksum, canonical hash and dataset digest with
`evidence/parity/golden_v1_checksums.json`, which was written by the sandbox reference run. Sandbox: PASS. Owner's Mac (arm64, macOS, PostgreSQL 16.15 Debian build in Docker, 35.9 s): PASS, all three values identical,
restore drill PASS. Two independent environments agree; a CI runner would be the third.

## Scheduler path verified; T11 packaging (2026-10-12)

`make airflow-scheduler-check` (D-067) ran the DAG under the real Airflow scheduler: success, pool slots respected,
transient fault retried, one extra publication, out-of-calendar scheduled run fails fast. The sandbox Docker daemon
starts but cannot pull images (registry 403), so the Airflow and dashboard images remain unbuilt here.
Written: README (abstract architecture only), `docs/operations_and_limits.md`, `docs/runbooks.md`, `docs/demo.md`, `docs/cv_claims.md`.

Owner's Mac (2026-10-12, snapshot -12/-13): `make bootstrap && make test` 205 passed, 2 skipped (9:57). Sandbox fresh
clone: 205 passed, 2 skipped (11:27); the fresh-clone run of the demo script found and fixed `fault-matrix --only`.

## Airflow Docker stack (2026-10-13, owner's Mac)

First real `make airflow-up`: two bugs found and fixed (uv pin vs Airflow constraints; trailing newline in the
database-name secret, D-068). Now: all three Airflow services Healthy, DAG listed (paused), pools present, idle
memory 808 MiB for the three services. Not run: a DAG run inside the containers; the dashboard image.

## Next action

Owner: (1) done: `make airflow-up`; (2) push the repository to GitHub so CI runs (the third parity environment).
Then: tag `v1.0` after CI is green and the Docker stack has started.
