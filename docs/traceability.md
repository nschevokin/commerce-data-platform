# Requirement → implementation → verification → evidence

A criterion is **PASS** only after its check has actually run successfully. "Where verified" names the
environment. Everything else is **PENDING** or **PARTIAL**, with what is missing.

| ID | Requirement | Implementation | Verification | Status | Evidence |
|----|-------------|----------------|--------------|--------|----------|
| A01 | Clean checkout runs the synthetic demonstration | Makefile, `commerce fixture-slice` | Fresh `git clone` → `make setup` → tests + slice | PARTIAL: verified on native PG 16.15 (sandbox) and on the owner's Mac via Docker Compose (bootstrap, tests, fixture slice); no dashboard yet; Airflow stack written but not started (T07) | `PROGRESS.md` §Verification |
| A02 | Full source input identified and profiled | — | Archive checksum, licence, profiles | PASS: inventory (zip sha256 bc76c894…7de7, 9 files with checksums and row counts, 2 deferred), licence and retrieval date (2026-10-11, assumed = upload day; owner to confirm), profile; original bytes verified identical | — |
| A03 | Record grains and money metrics are correct | `dbt/models/facts/fct_orders.sql` (separate aggregation), grain tests, 4 reconciliation tests | Golden fixture: oracle == SQL == Python Decimal, via the T02 harness AND the bronze path; fan-out mutant caught. Archive: 99,441 orders, 634 days, 74 categories compared with an independent Python Decimal reference from the original CSVs: 0 differences | PASS (sandbox PG 16.15); Mac run pending | `evidence/fixture/golden_v1_latest.json`, `evidence/bronze/olist_candidate_baseline_v1.json` |
| A04 | Duplicate file/revision delivery has no business effect | `ingestion/ledger.py`, D-005, `build/candidate.py` | Ledger: rerun is a no-op, redelivery counted. Business: candidate built from the duplicate replay has the same business checksum as the baseline | PASS on the golden fixture (sandbox PG 16.15) | `tests/integration/test_ingestion.py`, `tests/integration/test_bronze_pipeline.py` |
| A05 | Invalid required inputs cannot publish | Fail-closed staging, blocking tests, 9 quality checks, `release/publisher.py` status gate | 5 broken-input variants block the dbt build; orphan-item replay is REJECTED and `publish()` refuses it; serving stays on the previous release | PASS on native PG 16.15 (golden fixture) | `tests/integration/test_publication.py`, pytest `test_broken_input_blocks` |
| A06 | Late records and corrections update older business dates | Frontier + latest-revision selection (D-030) | Late B changes the 2017-03-11 date when it arrives; correction moves AOV 65.00 to 67.50 only after it arrives; highest revision wins | PASS on the golden fixture (sandbox PG 16.15); archive-scale scenarios wait for T08 | `tests/integration/test_bronze_pipeline.py` |
| A07 | Daily and clean full replay agree | `audit/equality.py`, `benchmark/run.py` | Five sequential windows (0-7 … 28-31) in one database vs clean full ingest + one window in another: equal `overall_sha256` over 11 business tables; negative control differs only in the 5 tables a correction touches and `differing_rows` names order B only | PASS (sandbox, golden fixture, native PG 16.15); also PASS on the Olist archive: 30 daily runs vs 5 bulk ranges, equal canonical hash over 11 tables | `tests/integration/test_replay_equality_and_backfill.py`, `evidence/benchmark/olist_local_benchmark.json` |
| A08 | Interrupted ingestion recovers without partial commit | `ingestion/ledger.py`, fencing functions (migration 0005) | Kill before commit, lost acknowledgement after commit, two concurrent writers, stale writer after takeover; three mutants (no count check, no conflict check, no fence check) each make a test fail | PASS for ingestion on native PG 16.15; publication-side faults wait for T06 | `tests/integration/test_ingestion.py`, `evidence/ingestion/` |
| A09 | Failed build/publication retains coherent serving release | `release/publisher.py`, migration 0007 | Kill before DDL, after DDL, before commit: old release still serves, no receipt; lost acknowledgement after commit: one publication; stale publisher and moved generation refused; frontier regression refused; contract tamper refused. Mutant (no contract/frontier check) makes 2 tests fail | PASS on native PG 16.15 | `tests/integration/test_publication.py` |
| A10 | Dashboard refresh cannot mix releases | `serving/reader.py` (D-004), `dashboard/data.py` (D-055) | A publication started while a refresh is half-way waits; the refresh sees one release in release_info and every view; the next refresh sees the new one | PASS on native PG 16.15 (reader-level; the page uses the same reader, one bundle per refresh) | `tests/integration/test_publication.py`, `evidence/experiments/2026-10-10_rr_reader_lock.*` |
| A11 | Reader role respects its access boundary | `migrations/0001`, `0004` | Positive `serving.release_info` read; 9 dashboard denials; 10 writer-role denials | PASS on native PG 16.15 (SCRAM) and on Compose PostgreSQL 16 with password auth (owner's Mac, 52 passed) | `tests/integration/test_migrations_and_roles.py` |
| A12 | Cloud fixture == local fixture, restore works | — (T10) | | PENDING (no cloud resources without approved plan) | — |
| A13 | Performance statements measured and labelled | — | | PENDING; no performance claims made | — |
| A14 | Reviewer understands decisions and limitations | README, `docs/decisions.md` | Review | PARTIAL: README with diagram, `docs/runbooks.md`, `docs/demo.md`, `docs/cv_claims.md` written; no external reviewer yet | — |

## Ticket status

| Ticket | Status | Acceptance check | Result |
|--------|--------|------------------|--------|
| T01 | Done | Clean boot + isolated roles | Migrations apply to empty DB; re-run no-op; tampering refused; role tests pass (PG 16.15). Compose boot verified on the owner's Mac. |
| T02 | Done except archive profiling | Hand-calculated fixture totals documented | `expected.json` hand-written; Python Decimal and dbt SQL both equal it. Archive source profile waits for A02. |
| T03 | Done on the fixture; archive run waits for the dataset | Replay + manifest contract in `docs/replay_and_manifests.md` | 75 unit tests: determinism (two stores byte-identical), zip == dir content, pinned digest, tamper/crash/missing-manifest, orphan/duplicate/ragged failures, scenario exactness, fault variants. A02 stays PENDING. |
| T04 | Done | Per-file transactional ingestion with receipts, conflict checks, fencing | 26 integration tests on real PostgreSQL; full archive loaded: 451,535 events, 217 receipts, 31 intervals COMPLETE; rerun is a pure no-op. Found and fixed a blueprint flaw (D-028). | 
| T05 | Done | Golden answers through the real path; duplicate/late/correction exact; archive candidate equals independent reference | 8 new integration tests; two mutants (revision order, frontier filter) each fail tests; archive: dbt 76/76 nodes, 0 failures, 2 expected warnings (576 payment-vs-items gaps, 623 products with unmapped category), crosscheck 0 differences. |
| T06–T07 | Done | see sections below | |
| T08 | Done in the sandbox | Backfill, audit mode, equality, §26 fault matrix | 8 equality/backfill tests + fault matrix 10/10 cases PASS; removing the write-fence check makes C09 fail | `evidence/recovery/fault_matrix.json` |
| T09 | Done in the sandbox | Read-only Streamlit page on one release (§36) | 8 unit + 3 PostgreSQL + 4 headless-page tests; screenshots on the archive release | `evidence/dashboard/` |
| T10 (local) | Done in the sandbox; AWS part waits for the owner's approval of `docs/cost_plan.md` | Measured experiments, backup/restore drill, candidate retention | benchmark on the Olist archive; drill on the archive release; 11 new tests | `evidence/benchmark/`, `evidence/recovery/olist_backup_restore.json` |
| T11 | In progress | README + diagram, runbooks RB-01..07, demo script, CV claims, scheduler-path check | Scheduler check PASS (D-067); packaging written; release tag and GitHub CI run pending |

## T07 additions (orchestration)

| Requirement | Implementation | Verification | Status | Evidence |
|---|---|---|---|---|
| Scheduled and manual runs share one path and one interval rule | `orchestration/window.py`, `stages.resolve_run` | Same (start, end) from a data interval and from explicit params; 7 malformed windows rejected; misaligned or >7 interval windows rejected | PASS at function level; scheduler-triggered run NOT exercised | `tests/integration/test_orchestration.py` |
| Failures remain failed | `stages.pipeline_result`, `all_done`, single leaf | Real DAG: bad window, rejected source file, missing evidence all end DAG state `failed`, downstream `upstream_failed`, serving unchanged, guard freed; mutant (terminal task always succeeds) fails 7 tests | PASS (Airflow 3.3.2 `dag.test` + sandbox PG 16.15) | `tests/dag/test_commerce_dag.py`, `tests/integration/test_orchestration.py` |
| Resume after kill, competing run, idempotent retry | `ledger.ensure_writer`, stage re-entry (D-040), D-044 | Killed after claim: rival refused with the guard untouched, owner resumes (attempt 2); killed before publication: resume reuses the validated candidate, one release; ingest rerun adds 0 rows | PASS (sandbox) | same |
| Full Olist replay through the stages | `commerce pipeline run` | 31 intervals in 5 serial runs, 131 s wall clock, final release equals the independent reference (0 differences over 99,441 orders, 634 days, 74 categories) | PASS (one sandbox run; not a benchmark) | `evidence/orchestration/olist_serial_ranges.json` |
| Airflow services on Compose | `docker/airflow/Dockerfile`, `docker-compose.yml` profile `airflow` | `docker compose config` parses | NOT RUN: image build and services unverified | — |


## T08 additions (backfill, equality, recovery)

| Requirement | Implementation | Verification | Status | Evidence |
|---|---|---|---|---|
| Bounded backfill with dry run | `orchestration/backfill.py`, `commerce backfill` | Inventory per interval (manifest, bytes, rows, receipts); serial ranges <= 7 intervals; stops at first failure | PASS (sandbox) | `test_replay_equality_and_backfill.py` |
| Gap does not advance frontier (§21) | `FrontierNotReady` -> `INPUT_NOT_READY` (D-049) | Clean DB, replay with interval 0 missing: run ends INPUT_NOT_READY, serving empty | PASS | same |
| Audit never publishes | mode `audit` (D-048) | No publication row, serving unchanged, publish stages SKIPPED | PASS | same |
| Code fix gives a new release | code SHA in input digest (D-051) | Same data, new SHA: new release, equal canonical answer | PASS | same |
| §26 fault matrix | `recovery/matrix.py`, `commerce fault-matrix` | C01 kill before commit; C02 drop after commit; C03 duplicate file; C04 altered payload; C05 missing source; C06 failing quality test; C07 publication aborted; C08 lost ack; C09 stale writer after takeover; C10 publish during dashboard read | 10/10 PASS on fixture data in a throwaway DB; mutant without fence check fails C09 | `evidence/recovery/fault_matrix.json`, `tests/integration/test_fault_matrix.py` |

Limits: the matrix runs on the 31-interval golden fixture, not the Olist archive; timings in the evidence are
sandbox timings (2 vCPU), not a benchmark; faults are injected in-process, not by killing real OS processes
or containers.

## T09 additions (dashboard)

| Requirement | Implementation | Verification | Status | Evidence |
|---|---|---|---|---|
| Numbers on screen equal the independent totals | `dashboard/metrics.py` over the bundle | Golden fixture release: eligible 2, excluded 1, merchandise 130.00, freight 13.00, payments 143.00, AOV 65.00, category total 130.00; Olist archive release shows 98,207 orders, BRL 13,494,400.74 | PASS (sandbox) | `tests/integration/test_dashboard_data.py`, `evidence/dashboard/olist_overview.png` |
| AOV, on-time rate, category counts aggregate correctly | `metrics.py` (D-057) | Unit cases where an average of averages would differ; mutant dividing by all eligible orders fails a unit test and the page test | PASS | `tests/unit/test_dashboard_logic.py`, `tests/dashboard/test_app.py` |
| Totals disclose coverage; labels prevent misreading | `dashboard/app.py` | Page test asserts "historical replay", "not live", BRL, missing-items/payments and excluded counts, non-additivity note | PASS (text only; layout checked by screenshots, not automated) | same |
| Outage is explicit | `Refresher` (D-056) | Real PostgreSQL: good read then port 1 → STALE with the same bundle; never-read → UNAVAILABLE; page shows an error and no metrics | PASS | same |
| No release yet | page | Empty database → warning, no tabs | PASS | `tests/dashboard/test_app.py` |
| Aggregate table for the current filters | `metrics.to_csv` | Exact decimal text, blank nulls | PASS (unit); browser download not clicked | `tests/unit/test_dashboard_logic.py` |
| Query latency (§30) | `Bundle.read_seconds` | Archive release, 30 fresh reads of all four views: median 0.061 s, p95 0.087 s (sandbox, local socket) | MEASURED, one environment | `evidence/dashboard/olist_bundle_latency.json` |
| Dashboard container | `docker/dashboard/Dockerfile`, compose profile `dashboard` | `docker compose config` parses | NOT RUN: image build unverified | — |

Limits: the page was rendered in headless Chromium in the sandbox only; no accessibility audit (contrast,
keyboard, screen reader) has been run, although status is always given in text as well as colour.

## T10 additions (local measurements, backup, retention)

| Requirement | Implementation | Verification | Status | Evidence |
|---|---|---|---|---|
| Baseline, daily replay (>= 10 intervals), duplicate replay, recovery, query tuning (§33) | `benchmark/run.py`, `commerce benchmark` | Full run on the Olist archive; runner smoke test on the fixture checks every section and invariant | MEASURED on one machine; see `docs/benchmark.md` for limits | `evidence/benchmark/olist_local_benchmark.json`, `tests/integration/test_benchmark_smoke.py` |
| Restore works and is believed only with evidence (RB-07, §31) | `recovery/backup.py`, `commerce backup`, `commerce restore-drill` | Fixture release and Olist release: dump, restore into a separate database, equal checksum / release / counts, guard free, roles intact; damaged dump and wrong manifest refused; backup refused while a writer holds the guard | PASS (sandbox PostgreSQL 16.15) | `tests/integration/test_backup_restore.py`, `evidence/recovery/olist_backup_restore.json` |
| Candidate retention (§23) | `release/retention.py`, `commerce candidates-gc` | Pure plan unit tests (mutant fails); on a real database dry run changes nothing, apply keeps current + 2, serving checksum unchanged, second run no-op, refused while a run owns the guard | PASS | `tests/unit/test_retention_plan.py`, `tests/integration/test_backup_restore.py` |
| A12 (substitute, D-066): independent environments agree on the fixture, restore works | `audit/parity.py`, `commerce parity`, `audit/serving_checksum.py` | Replay from raw files on each host; serving checksum, canonical hash and dataset digest equal the committed reference; restore drill inside the check | PASS on two independent environments (sandbox x86_64 Ubuntu; owner's Mac arm64, Docker PostgreSQL 16.15), CI runner pending. AS WRITTEN (cloud, S3) NOT MET and not claimed | `evidence/parity/golden_v1_checksums.json`, `tests/integration/test_parity.py` |

## T11 additions (orchestration under a real scheduler, packaging)

| Requirement | Implementation | Verification | Status | Evidence |
|---|---|---|---|---|
| DAG runs under scheduler/executor with pools and retries (closes the D-046 gap) | `scripts/airflow_scheduler_check.py`, `make airflow-scheduler-check` | Real dag-processor + scheduler + api-server (Airflow 3.3.2, LocalExecutor, PostgreSQL metadata): clean run success, pool overlap ≤ slots, injected connection refusal retried, one extra publication; out-of-calendar scheduled run fails once without retry | PASS (sandbox, native, not Docker; one run) | `evidence/orchestration/scheduler_check.json`, D-067 |
| Failure classes have operator procedures | `docs/runbooks.md` (RB-01..07) | Commands cross-checked against the CLI; procedures rest on tests already listed above | Written; never exercised by someone other than the author | `docs/runbooks.md` |

