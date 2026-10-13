# Runbooks

Operational procedures for the failure classes of blueprint §28–§29, written against the commands that exist in
this repository. Every procedure ends the same way: recovery goes through the same durable evidence and the same
publication gates as a normal run. **Marking an Airflow task "success" never creates missing receipts or validates
a release.**

Conventions: `commerce` is `python -m commerce.cli`; DSNs and passwords come from `./secrets` (see docs/operations_and_limits.md).
The dashboard keeps serving the last published release throughout every incident below, so the first question is
always "is the current release unchanged?" — `commerce serving-status` answers it.

## Incident record (fill one per incident)

| Field | Content |
|-------|---------|
| Context | run id, manifest hashes, replay version, code SHA, environment |
| Impact | stale, unavailable, or serving a defective release? which release id |
| Cause | one causal sentence backed by a failing row, receipt or log line |
| Recovery | commands, operator, duration, final quality results |
| Prevention | one new test or constraint tied to this incident |

---

## RB-01 Failed or interrupted source load

*Symptom:* an `ingest_source` task failed or the process died mid-file.

1. Read the failed run and the file identity in the task log.
2. Check `control.file_receipts` and the distinct event count for that file **before** retrying. Ingestion is one
   transaction per file, so a file is either fully received or absent — there is no partial file.
3. A matching receipt means the file is already committed: re-running returns the committed result and writes
   nothing (`ALREADY_COMMITTED`).
4. No receipt: fix the transient cause (database down, disk, lock timeout) and re-run the same immutable input:
   `commerce pipeline run --from-index N --to-index M --run-tag <same tag>`. The tag makes the re-invocation
   resume the same runs.
5. Reconcile: the run's `reconcile_ingestion` stage compares receipts with manifest counts.
6. Keep the failed attempt's task log and the recovery report with the incident record.

*Evidence this works:* fault matrix cases C01–C10 (`evidence/recovery/fault_matrix.json`); duplicate replay
produces 0 new events (`evidence/benchmark/olist_local_benchmark.json`).

## RB-02 Contract or checksum rejection

*Symptom:* `verify_manifest` rejects a file (checksum mismatch, row-count mismatch, unknown column).

1. Confirm the mismatch against the immutable manifest: `commerce replay verify --dataset-sha256 <sha>`.
2. Decide: is the stored object corrupt, or is the parser contract wrong? Inspect the object read-only.
3. **Never edit a stored object in place.** Corrupt object: restore it from the original archive (kept by
   `import-data`) by regenerating the replay version. Wrong contract: change the parser under review and bump the
   parser contract version.
4. Add a fixture reproducing the defect.
5. Reprocess under the new explicit identity (new replay version or new code SHA) and compare the audit
   hash with the previous one before publishing: `commerce backfill --mode audit --dry-run ...` first.

## RB-03 dbt or metric invariant failure

*Symptom:* a candidate is `REJECTED`; the DAG's `validate_candidate` failed.

1. Confirm the dashboard release is unchanged: `commerce serving-status`.
2. Read `run_results.json` for the candidate (path in `control.releases.artifact_uri`) and the failing-row query
   of the blocking check in `control.quality_results`.
3. Trace the affected key through staging, facts and `bronze.source_events`.
4. Correct the transformation or the documented data policy (`docs/metric_contract.md`).
5. Run the focused fixture test, then a full candidate build.
6. Publish only when blocking checks pass and the expected aggregate difference is explained. A code fix
   produces a **new release** (D-051); it never edits the rejected one.

## RB-04 Publication timeout or unknown commit outcome

*Symptom:* `publish_release` timed out or the connection dropped around COMMIT.

1. Compare `control.current_release` with `control.publications` for the candidate.
2. Both identify the candidate: publication committed. Run serving verification (`verify_serving`, or
   `commerce serving-checksum`) and finish the original run — re-publishing returns `ALREADY_COMMITTED`.
3. Previous release still current and no publication row: retry after resolving lock contention.
4. Identities disagree: **stop writer submissions** and investigate the atomicity invariant. Do not overwrite
   state to make it agree.

*Evidence:* matrix cases for publication killed before commit (28.2 s fault-to-restored in the benchmark).

## RB-05 Candidate published but reader verification failed

1. Record that publication occurred.
2. Check serving grants, the schema signature and `serving.release_info`.
3. If fixing the reader role restores the validated candidate without data change, rerun verification.
4. Otherwise roll back through the protocol:
   `commerce rollback --operator <you> --reason "<why>" --incident <id>` (defaults to the previous validated
   release; it is a recorded `ROLLBACK` publication, not a delete).
5. Capture old/new release ids and add an interface test before the next release.

## RB-06 Abandoned writer guard

*Symptom:* new runs fail with `GuardHeldError` (CG002) although no run is active.

1. Pause new submissions (pause the DAG).
2. Inspect: `commerce guard status`. Confirm the owner run is terminal and no orphaned process or database
   session of that run remains (`pg_stat_activity`).
3. Stop the old process and wait for its transaction to roll back.
4. Take over in one recorded administrative transaction:
   `commerce guard takeover --operator <you> --reason "<why>"`. The fencing generation increases.
5. Resume through normal task contracts. An old-generation writer fails its next ingestion or publication
   write (CG001/CG003) — verified by the stale-writer tests.

## RB-07 Lost warehouse volume

1. Take or locate the latest backup: `commerce backup` writes a `pg_dump -Fc` and a manifest (checksum,
   PostgreSQL major version, release id, serving checksum). Backups live outside the database volume.
2. Restore into a **clean** instance with the same PostgreSQL major version. Roles are recreated by bootstrap
   (a dump alone is not a complete backup, D-064).
3. Verify before enabling writers: `commerce restore-drill --dump <file> --manifest <file> --server-dsn ...`
   checks checksum, current release, counts, guard state and role grants against the manifest.
4. No usable dump: recreate schemas (`commerce migrate`) and replay the immutable raw manifests in order
   (`commerce pipeline run`). Run ids are new; provenance to the old input identity is kept in the manifests.
5. Never infer completeness from the dashboard opening successfully.

*Evidence:* `evidence/recovery/olist_backup_restore.json` (97.5 MB dump in 23 s, restore in 14.9 s, serving
checksum equal).

## Routine

| Task | Command |
|------|---------|
| What does the dashboard see? | `commerce serving-status` |
| Drop old candidate schemas (dry run) | `make candidates-gc` (add `APPLY=1` to execute; keeps current + previous 2) |
| Bounded backfill | `commerce backfill --start ...Z --end ...Z [--mode audit] [--dry-run]` |
| Prove two environments agree | `make parity` |
