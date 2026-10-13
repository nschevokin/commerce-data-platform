# Decision log

Format: **ID — decision** · why · status. "Blueprint" = `Ecommerce_Data_Platform_Blueprint.pdf` v1.0.
Statuses: **Accepted** (implemented and verified), **Accepted-pending** (decided, implemented in a later
ticket), **Proposed** (needs owner review), **Open**.

## Corrections to the blueprint

**D-004 — Dashboard refresh must `LOCK TABLE serving.release_info IN ACCESS SHARE MODE` before its first
SELECT.** · Blueprint §22 says "read `release_info` first, holding its lock". In PostgreSQL a REPEATABLE
READ transaction's snapshot is fixed at the start of its first query, *before* that query acquires
relation locks. A reader that blocks on `release_info` during publication therefore wakes up with the
**old** data snapshot but resolves the **new** view definitions. Experiment on PG 16.15
(`evidence/experiments/2026-10-10_rr_reader_lock.*`): without the lock the reader saw release `B`'s views
and data, but `health='A'` for a row changed *inside* the publish transaction, which is a mixed read.
`LOCK TABLE` does not freeze the snapshot, so taking it first gives a post-commit snapshot: all `B`.
Minimal fix; the guarantee is unchanged. Also: `release_info` is published as a constant-literal view
(identity in the catalog, not in a mutable table). · Accepted-pending: concurrency test in T06/T09 (A10).

**D-005 — Redelivery of an already-accepted event in a later interval.** · §09 requires equal event keys to
have identical `available_at` *and* requires `available_at` to lie inside the manifest interval. A
duplicate redelivered in a later batch cannot satisfy both, so the duplicate-delivery scenario would
become a blocking conflict instead of a no-op (it breaks A04). Proposed rule: an event with
`available_at < interval_start` is accepted only as an *exact* redelivery of an event already in bronze
(same key, payload hash and `available_at`). It counts as `exact_duplicate_rows`. Any other
out-of-interval event is a contract error. · Proposed; decide in T03/T04.

**D-006 — Fencing lock modes and mechanism.** · §10 says every writer transaction checks the guard
"under a row lock". With `FOR UPDATE` the two parallel ingestion slots would serialise on the singleton.
Writers take `FOR SHARE`; claim, takeover and publication take `FOR UPDATE`. A takeover still waits
for in-flight writer transactions and bumps the generation, so stale writers fail afterwards.
Checks run inside `SECURITY DEFINER` functions, so writer roles never need `UPDATE` on `run_guard`
(row locks would otherwise require that privilege). · Accepted for ingestion (T04): writers call `assert_writer` (FOR SHARE) inside every file transaction;
`acquire`, `release`, `takeover` use FOR UPDATE; a stale writer is rejected (tested, with a mutant that removes the
check). Publication fencing follows in T06.

**D-007 — "Delivered" population for the on-time rate.** · §14 does not define it. Rule:
`order_status = 'delivered' AND is_eligible`. Denominator: those with both actual and estimated dates.
Comparison: source-local calendar dates. Orders with a delivery date but another status are not counted.
That share is to be profiled on the archive. · Accepted (T02).

**D-008 — Item key keeps the source name `order_item_id`.** · §13 writes `item_id`, §05 `order_item_id`.
Keeping the source name avoids a rename layer. · Accepted.

**D-016 — Dirty working tree cannot publish.** · The release digest (§21) includes the code SHA. An
uncommitted change would publish under a SHA that doesn't describe the code. Audit-only builds may
run dirty with `code_sha = <sha>-dirty-<diffhash>`. · Proposed (T06).

## Environment and dependencies

**D-001 — Verification server when Docker images are unavailable.** · Docker Hub is denied by the build
sandbox's egress policy (`registry-1.docker.io` → 403), so Compose cannot boot there. Checks run against
a native PostgreSQL 16.15 server instead (same major as the Compose image). Compose, the Dockerfile and CI
are written but **unverified** until run on a machine with registry access. · Accepted.

**D-002 — PostgreSQL 16** for both the warehouse and the Airflow metadata DB. Separate instances and volumes
(§30). · Accepted.

**D-003 — Locked toolchain.** Python 3.12.13; Airflow 3.3.2 resolved against the official
`constraints-3.3.2/constraints-3.12.txt`; dbt-core 1.12.5 + dbt-postgres 1.11.0 in an isolated venv
(`envs/dbt`). Finding: Airflow's constraints pin `psycopg==3.3.5`, so the application pins the same driver
(`[tool.uv] constraint-dependencies`) to run unchanged inside the Airflow image. · Accepted (locks
resolved; Airflow image build pending T07).

**D-009 — Container image digests.** Tags are pinned to a major line (`postgres:16-bookworm`,
`python:3.12-slim-bookworm`). Digests get pinned on first successful pull on a machine with registry
access. A mutable tag is not acceptable for the release (§35). · Open.

**D-020 — Tests must not clobber real role passwords.** · Roles are cluster-global, so the test fixture's
`ALTER ROLE ... PASSWORD` on a shared server silently replaced the passwords `make bootstrap` had set.
Found on the first Compose run: `make test` passed (52/52), then `make fixture-slice` failed with
`password authentication failed for user "commerce_transform"`. Reproduced in the sandbox once it ran
with SCRAM auth, and fixed: the fixture snapshots each role's stored verifier and login flag from
`pg_authid` and restores them in teardown. Verified: all four real logins work before and after a full
run. Residual risk: a hard kill (SIGKILL) skips teardown; `make bootstrap` repairs it. Tests should still
prefer a disposable server. · Accepted.

**D-019 — Local server uses `trust` auth.** Role *privilege* boundaries are verified for real. This is
exactly why D-020 went unnoticed in the sandbox; the sandbox server now uses SCRAM and the Compose run
on the owner's Mac exercises password auth for real. · Accepted.

## Data model and metrics

**D-010 — Missing is not zero, except in documented display aggregates.** At order grain, an absent
item/payment side gives NULL amounts plus a false coverage flag. Daily marts zero-fill sums over an empty
eligible set and expose coverage counts beside them. · Accepted.

**D-011 — AOV.** Mart AOV is rounded half-up to cents. Any period AOV is `sum(merchandise) /
sum(eligible_orders_with_items)` from the daily mart, never an average of daily AOVs. · Accepted.

**D-012 — `control.quality_results` keyed by `release_id`**, not `run_id`. One run can build several
candidates across retries, and each gets a new schema (§16). · Accepted.

**D-013 — T02 fixture source path (status updated in T05).** The fixture CSVs are COPYed as text into a throwaway
`fixture_<uuid>` schema. Staging parses *text*, so in T05 the same models read bronze latest-revision
selections (`payload ->> field` is also text) unchanged. This harness has no manifests, receipts or
idempotency and is not ingestion. Since T05 it is only the dbt-level test harness (broken-input and mutant tests
need raw text the ledger would reject); real data takes the bronze path. · Accepted.

**D-014 — Fail-closed parsing in SQL** (`strict_money`, `strict_int`, `strict_ts`). A value outside the
contract raises an error naming it, instead of rounding (`10.005`) or accepting loose formats.
Defence in depth: T04's Python validation is the primary gate. · Accepted.

## Replay and manifests (T03)

**D-005 update.** Adopted as proposed (default, owner has not objected): a redelivered exact event with an
earlier `available_at` in a later interval is accepted only as an exact duplicate (same key, revision, hash).
The duplicate scenario stamps redeliveries with the ORIGINAL `available_at`; any other earlier stamp is a
conflict. Enforced at write time (`replay/writer.py`) and at ingestion (`ingestion/ledger.py`); tested in
`tests/integration/test_ingestion.py`.

**D-021 — Manifest carries two checksums.** `sha256` covers the gzip bytes (what is stored), `content_sha256`
covers the uncompressed JSONL. Gzip output can differ between zlib builds, so cross-machine reproducibility
is judged on `content_sha256`; within one build the bytes are identical too (mtime is fixed at 0). · Accepted.

**D-022 — Original archive kept as received** under `archive/dataset=<sha>/original/`, next to `inventory.json`
and `profile.json`. The dataset digest is the SHA-256 of the zip bytes (directory sources: digest of the sorted
(name, sha256) listing). · Accepted.

**D-023 — `payload_hash` is computed at ingestion (T04)** over the typed canonical payload, never stored in raw
files, so a raw file cannot carry a hash that disagrees with its content. · Accepted.

**D-024 — Baseline needs no seed.** Partition is rank-based (purchase timestamp, order_id), so the baseline
uses no randomness and is named `baseline-v1`. Scenarios carry the seed. · Accepted.

**D-025 — Seeded selection without an RNG.** The `count` orders with the smallest SHA-256 of
`seed|scenario|key` are chosen: identical on every platform and Python version. · Accepted.

**D-026 — Dataset licence handling.** Olist data is CC BY-NC-SA 4.0 (text supplied by the owner from the dataset
page, 2026-10-11). The repository is a non-commercial portfolio project. Raw and derived data stay out of git
(`data/store/` is ignored); the README credits Olist, links the licence, and states that data was transformed.
Any published derivative (e.g. a public demo dataset) must carry the same licence. Not legal advice. · Accepted.

## Ingestion (T04)

**D-027 — Unknown order status is rejected at ingestion.** §24 says an unseen status "blocks until its metric
treatment is decided". Rejecting it when the file is ingested (the whole file, no receipt, row-level artefact)
is the earliest and safest place; the dbt gate stays as a second line (D-014). Cost: one new status in a live
feed blocks that day's orders file until the contract is versioned. Other relationship defects (orphan
item) are NOT ingestion errors; they are the quality gate's job (§24, T05). · Accepted.

**D-028 — Receipt key includes the interval (correction to §09).** `file_receipt_key = replay + source +
file_sha256` collides as soon as the same file content appears in two intervals. This happens in the baseline:
every source has an identical empty file in each empty interval. Key is now (replay, source, interval_start,
file_sha256) (migration 0006). A retried file is still recognised; an identical file in another interval gets
its own receipt. Found by the first run of the integration test. · Accepted.

**D-029 — Run shape.** One run covers at most 7 intervals (schema constraint). All manifests and file
checksums are verified before any database write; a missing manifest ends the run `INPUT_NOT_READY` with
nothing ingested. The writer guard is held for the run and released at the end; after a crash it stays held
until an operator runs `commerce guard takeover` (no automatic expiry, §10). · Accepted.

## Bronze to candidate (T05)

**D-030 — Frontier and source selection.** A candidate reads bronze through `frontier_end`, which must be the
end of a replay interval with every interval up to it COMPLETE under the manifest the replay declares
(`build/candidate.py::resolve_frontier`). Missing or differently-completed intervals stop the build
(`FrontierNotReady`). Staging reads `latest_<source>` tables built inside the candidate: highest revision per
entity among events with `available_at < frontier_end`. These expose the same text columns as the T02 fixture,
so the strict parsers did not change. Materialised as tables so a candidate is a stable snapshot. Safe against
concurrent ingestion because rows below a COMPLETE frontier cannot change: later arrivals carry later
`available_at`, and D-005 redeliveries are byte-identical. · Accepted.

**D-031 — Late-order oracle corrected.** The first draft of `expected.json` (T02) assumed canceled order C was
visible while B was missing. In the deterministic replay C arrives on day 21, after B (day 11, or 13 when late),
so before B arrives only A exists. The values were recomputed by hand and the file carries a note; the property
(older purchase-date totals change when B arrives) is unchanged. · Accepted.

**D-032 — Reference reader handles a UTF-8 BOM.** The real `product_category_name_translation.csv` starts with
a BOM; the independent reference crashed on it while the ingestion path (utf-8-sig) did not. Fixed. Lesson:
run the independent oracle on the real archive early. · Accepted.

## Release and publication (T06)

**D-033 — Candidate lifecycle and validation.** A candidate is registered in `control.releases` (BUILDING,
fenced) before dbt runs, then BUILT, then VALIDATED or REJECTED by `release/quality.py` (9 checks, recorded in
`control.quality_results`). Blocking: dbt build complete, serving contract (exact column names, order, types),
one row per source order, every bronze entity accounted for, merchandise and payment reconciliation, and (exact
fixture only) payment-vs-item gaps. Warnings on the archive: payment gaps (576), unmapped categories (623),
negative delivery durations. A validated candidate is handed to the publisher read-only; the transform role
loses INSERT/UPDATE/DELETE/TRUNCATE on it. · Accepted.

**D-034 — Publication transaction.** `control.lock_for_publication` takes the guard row, then the release
pointer, both FOR UPDATE (fixed order); the publisher then re-checks status VALIDATED, zero blocking failures,
frontier monotonic (PUBLISH only) and the serving contract, records the receipt with
`control.record_publication` (generation moved => CG003), replaces `serving.release_info` first, then the data
views in a fixed order, and commits once. Both functions are SECURITY DEFINER and granted to the publisher only.
A second request for an already-published release returns ALREADY_COMMITTED from the receipt. · Accepted.

**D-035 — Contract re-check at publish.** The transform role owns its candidate tables and could still ALTER
them after validation (revoking data writes does not stop DDL by an owner). The contract is therefore checked
again inside the publication transaction; a renamed column is refused. Stronger isolation (candidate owned by a
separate role) is deferred; not needed for a one-writer pipeline. · Accepted.

**D-036 — Rollback.** Same lock order and checks as publish, except the frontier rule, and it requires an
operator and a reason (database CHECK as well); the receipt is a new generation of kind ROLLBACK. Target
defaults to the most recent other validated release. Candidate schemas are never dropped yet (retention/GC from
§23 is not implemented). · Accepted.

**D-037 — Reader protocol and empty state.** `serving/reader.py` follows D-004 (READ ONLY REPEATABLE READ, ACCESS
SHARE lock on `serving.release_info` first). Before the first publication `release_info` has a NULL release and
no data views exist; the reader then returns no tables. · Accepted.

**D-038 — Pipeline connections use autocommit.** The first pipeline run deadlocked against itself: a connection
left idle inside an implicit transaction (after a SELECT) held a FOR SHARE lock on the guard row, and the
publisher's FOR UPDATE waited for it forever. Release runs now use autocommit connections with explicit
transactions per step. Regression covered by the whole publication test module. · Accepted.

## Orchestration (T07)

**D-039 — Orchestration is plain Python; the DAG only calls it.** `commerce.orchestration.stages` has one
function per blueprint task. The run id is `uuid5(replay_version, dag_run_id)`, so every task, retry and
restart finds the same `control.runs` row without XCom. Each stage recovers its inputs from the control tables,
the release row and the immutable store. `dags/commerce_pipeline.py` has no SQL and no heavy import-time work
(a test greps for both). · Accepted.

**D-040 — Stages re-enter the guard instead of assuming it.** A writing stage calls `acquire_guard` for its own
run: re-entrant for the owner, `GuardHeldError` for anyone else, never a takeover. This is what lets a retried
or cleared task resume after the terminal task released the guard. The fencing generation moves on each
re-entry, so a task from before a takeover still fails `assert_writer`. · Accepted.

**D-041 — The terminal task judges evidence.** `pipeline_result` runs with `all_done`, reads the latest attempt
per (stage, source) from `control.stage_results`, and raises unless every required stage succeeded; SKIPPED is
accepted only for the five release stages of a NO_CHANGE run. It maps the first specific error code to a run
status (INPUT_NOT_READY, FAILED_CONTRACT, FAILED_QUALITY, FAILED_VERIFICATION, else FAILED_TRANSIENT), releases
the guard only if its own run owns it, and is the only leaf of the DAG, so DAG state is its state. · Accepted.

**D-042 — NO_CHANGE.** After ingestion, a run ends NO_CHANGE (visible, not an error) when its input digest equals
the serving release's, or when the serving frontier is already beyond the window. The pipeline never rewinds
serving; only an operator rollback does (D-036). Ingestion of an old window still runs and is a receipt no-op. · Accepted.

**D-043 — Retry classification.** Retryable: `psycopg.OperationalError` (connection loss, lock timeout,
statement timeout), `GuardHeldError`, `ConcurrentWriteError`, `TimeoutError`. Everything else fails the task at
once (`AirflowFailException`): rejected data, checksum or contract errors, failed gates, stale writer, moved
generation. The error code is stored in `stage_results`. The publisher connection sets lock_timeout 10 s and
statement_timeout 60 s. · Accepted.

**D-044 — A half-built candidate is retired, not reused.** If a build attempt died, its `BUILDING` row becomes
`DROPPED` and the retry registers a new schema. A `BUILT`/`VALIDATED`/`REJECTED` candidate of the same run is
reused, so resuming after a kill before publication does not build twice. Schemas of dropped candidates stay
until retention is implemented. · Accepted.

**D-045 — dbt runs in its own process group.** Cancelling or timing out a build kills the whole dbt process
group (`start_new_session`, `killpg`), so no orphan dbt keeps writing into a candidate. · Accepted.

**D-046 — What the DAG tests prove and what they do not.** `dag.test` (Airflow 3.3.2, in-process) executes every
task in dependency order against real PostgreSQL and dbt, including mapped ingestion, `all_done` and
`upstream_failed` propagation. It does not exercise the scheduler, executor, pools, sensor rescheduling or retry
timers, and the scheduled path is verified only through `run_window` parity (same function, same interval). The
Airflow image and compose services are written but not run (no Docker in the build sandbox). Scheduler, pool and retry behaviour were later verified natively: see D-067. · Accepted.

## Backfill, equality and recovery (T08)

**D-047 — Equality is a multiset hash of business columns.** A07 compares the 11 business tables of two
candidates by `md5(string_agg(md5(row::text) ORDER BY ...))` plus row counts, in `audit/equality.py`.
Operational columns (run ids, build times, release ids) are excluded; `mart_pipeline_health` is restricted to
`source_name, latest_rows`. The two paths run in two databases sharing only the raw files. A negative control
(a correction replay) must differ, and `differing_rows` must name exactly the changed order. · Accepted.

**D-048 — Audit mode.** `mode=audit` ingests, builds and validates an as-of candidate but marks the publish
stages SKIPPED and never takes the NO_CHANGE shortcut from serving. It is the safe way to check a historical
window. · Accepted.

**D-049 — A gap is not a frontier.** Requesting an interval whose predecessor is not COMPLETE ingests the
interval but cannot advance the frontier: the run ends `INPUT_NOT_READY` (error `FRONTIER_NOT_READY`) and serving
is untouched. Filling the gap later lets the next run advance. · Accepted.

**D-050 — A publishing backfill never rewinds serving.** A window already covered by the served frontier ends
NO_CHANGE; only audit mode rebuilds history. · Accepted.

**D-051 — Reprocessing after a code fix is a new release.** The input digest includes the code SHA, so the same
data with fixed code yields a new release (and a new publication) whose answer is compared with the old one by
D-047. · Accepted.

**D-052 — Faults only run in `commerce_demo_*` databases.** `recovery/demo_db.py` creates and drops a random
`commerce_demo_<hex>` database, refuses any other name, and restores the cluster-global role password hashes it
changed. Fault hooks (`Wiring.faults`) are plain parameters; production wiring leaves them empty. · Accepted.

**D-053 — A lost publication acknowledgement ends NO_CHANGE on retry.** After the commit survives, the rerun
sees serving already at that input digest. A direct re-publish returns ALREADY_COMMITTED (T06 tests). The
blueprint's "inspect the receipt" is satisfied by either path. · Accepted.

## Dashboard (T09)

**D-054 — The customer-state filter applies to the Delivery view only.** The blueprint puts date and state
filters on the Overview, but `mart_sales_daily` and `mart_category_daily` carry no state column. Adding one
is a serving-contract change (versioned migration, new rows per state) that the questions in §03 do not
need. The Overview is filtered by purchase date; Delivery by state and cohort; the page says so. · Accepted.

**D-055 — Filters are applied in memory on one bundle.** A refresh reads all four views in one D-004
transaction; date and state filters then slice that bundle. A filter change never issues a new query, so it
cannot pull a second release into a chart. The bundle is cached for 30 s per Streamlit server and carries its
own `release_id`. Largest view on the archive: 18,943 category rows. · Accepted.

**D-056 — Outage is labelled, never blended.** On a database error the page shows the last complete bundle
marked STALE with its read time and the error, or an UNAVAILABLE message when no read ever succeeded. It never
combines cached and fresh data (§36). · Accepted.

**D-057 — Aggregation rules on screen.** Period AOV = Σ merchandise / Σ orders with items; on-time rate is
pooled (Σ on-time / Σ with both dates), never a mean of cohort rates; lead-time percentiles stay per cohort;
distinct-order counts are added only across dates inside one category. Money is Decimal in every table and
metric; only chart input is converted to float for drawing. · Accepted.

**D-058 — Own environment and image.** Streamlit and its dependencies (pandas, pyarrow, tornado…) live in
`envs/dashboard` with a pinned requirements lock, not in the application environment, so the pipeline image
and lock stay small and the dashboard cannot import pipeline internals beyond `commerce.config` and
`commerce.serving`. The compose service binds 127.0.0.1 only, runs read-only as a non-root user and holds only
the dashboard role's password. · Accepted.

**D-059 — What the evidence view can and cannot show.** The dashboard role may read `serving` only (A11), so
the "Pipeline evidence" tab shows the release identity, frontier, per-source row counts and the blocking and
warning counts from `serving.pipeline_health`. Run history, receipts and logs stay in Airflow and the
`control` schema. Exposing them would need a deliberate serving view, not a wider grant. · Accepted.

## Measurement, backup and retention (T10)

**D-060 — Benchmarks run in a throwaway demo database and report their environment.** `commerce benchmark`
creates and drops a `commerce_demo_*` database, records machine, versions and dataset digest, and writes one JSON
file. Statistics use warm runs; the cold start is reported separately; nothing is extrapolated. · Accepted.

**D-061 — Memory is sampled, not read from `ru_maxrss`.** A child's `ru_maxrss` on Linux keeps the parent's
pre-exec size, which made dbt look as small as the orchestrator. The sampler sums Pss over the process tree and
over PostgreSQL every 0.25 s. It is Linux-only; elsewhere the field is null. · Accepted.

**D-062 — A serving checksum, independent of operational identity.** `serving_checksum` hashes the rows a reader
sees from one D-004 snapshot, leaving out release id, time and generation, so a rebuild, a restore or a cloud
host with the same business answer gives the same value. It is the comparison key for A12. · Accepted.

**D-063 — Candidate retention follows blueprint §23.** Keep the current release, the previous two published
releases, builds in progress, failed candidates for 7 days and incident holds; drop the rest, mark them DROPPED
(so the publisher's VALIDATED check refuses a rollback to them). The operator command is a dry run unless
`--apply`, refuses while a run owns the writer guard, and uses a lock timeout on each drop. Found by measurement:
31 retained candidates grew the database from 0.9 to 3.0 GB. · Accepted.

**D-064 — Backup is a dump plus a manifest; restore is a drill, not a trust.** The dump leaves out non-current
candidate schemas (the serving views depend only on the current one). The drill recreates the NOLOGIN roles,
restores into a separate database and demands equal serving checksum, current release, bronze and control
counts, a free guard, no blocking failures and the role boundaries before it says PASS. Dump and manifest
checksums are verified first. Passwords are not in a dump; logins are bootstrapped again. · Accepted.

**D-065 — Client tools must match the server's major version.** A newer `pg_dump` can emit SQL an older server
rejects (for example `SET transaction_timeout`, PostgreSQL 17). Backup and drill refuse a mismatch; on macOS use
`brew install postgresql@16`. · Accepted.

**D-066 — A12 is met in substitute form, without AWS.** The blueprint asks for a temporary EC2 + S3 run. The owner
has no AWS account, so no cloud is used. What is claimed instead: **environment parity**. The golden fixture is
replayed from raw files through the real pipeline on any machine (`commerce parity`, run by `make test`), and the
serving checksum, canonical business hash and dataset digest must equal the values committed from a reference run
(`evidence/parity/golden_v1_checksums.json`). Independent hosts so far (both PASS): the sandbox VM (x86_64, Ubuntu PostgreSQL
16.15) and the owner's Mac (arm64, Debian-build PostgreSQL 16.15 in Docker); a hosted CI runner is the third once the repository is pushed. The restore drill runs
inside the same check when `pg_dump` 16 is available. **Not demonstrated, and not to be claimed:** S3 object-store
behaviour, an instance role, Session Manager access, a public-cloud deployment, or cloud cost. The CV wording is
"reproduced identical results across independent environments", never "deployed on AWS". · Accepted.

## Security

**D-015 — `TEMPORARY` revoked from PUBLIC.** Otherwise the dashboard role could create temp tables.
Granted back to ingest (staging tables, §09) and transform. · Accepted.

**D-018 — Roles are created NOLOGIN in migrations.** `commerce bootstrap-roles` grants LOGIN with passwords
from env/secret files, so no credential is in SQL or git. · Accepted.

## Scope and ownership

**D-017 — Document owner.** The repository owner is Nikita Chevokin (confirmed 2026-10-13). No claims about the
owner's employment or background appear anywhere in this repository: the README and CV-claims file describe only
what the code, tests and measurements show. · Accepted.

## Scheduler verification (T11)

**D-067 — The scheduler path is verified natively; the Docker images are not.** `scripts/airflow_scheduler_check.py`
(`make airflow-scheduler-check`) starts the real `airflow dag-processor`, `scheduler` and `api-server` (3.3.2,
LocalExecutor, PostgreSQL metadata) and triggers `commerce_pipeline` through the scheduler against a throwaway
`commerce_demo_*` warehouse. Result (`evidence/orchestration/scheduler_check.json`, sandbox, 2026-10-12): the clean
run succeeds with 20 task instances and one publication; measured task overlap never exceeded the pool slots
(ingestion 2 of 2, build 1 of 1); with the `commerce_transform` role's connection limit set to 0 the first task
that needed it failed, the scheduler retried it (try 2) once the limit was lifted, and the run published exactly
one more release (no duplicate publication). Unpausing the DAG also created the latest scheduled interval
(2026-10-12), which is outside the replay calendar: it failed at `resolve_run` on its first try with a
deterministic error and no retry, before any write. This closes the executor, pool and retry-timer part of D-046.
Still not verified: sensor `reschedule` timing under a scheduler. · Accepted.

**D-068 — The Airflow Docker stack starts (owner's Mac, 2026-10-13).** The first real `make airflow-up` found two
bugs that no sandbox check could: (1) the Dockerfile pinned `uv` on the same `pip install` line as Airflow's
constraints file, which pins a different `uv`; (2) the connection-string secret command used `echo`, so the
database name carried a trailing newline (`database "airflow\n" does not exist`); the other secret reads are now
newline-safe too. After the fixes the three Airflow services report Healthy, `airflow dags list` shows
`commerce_pipeline` paused, and pools `commerce_ingest` (2) and `commerce_transform` (1) exist. Idle memory
(`docker stats --no-stream`, Docker Desktop VM 7.65 GiB): api-server 221.5 MiB, scheduler 400.4 MiB, dag-processor
186 MiB (Airflow total about 808 MiB), airflow metadata PostgreSQL 41.6 MiB, warehouse PostgreSQL 85 MiB. Idle only:
no DAG run was executed inside the containers. · Accepted.

