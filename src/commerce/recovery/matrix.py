from __future__ import annotations

import json
import platform
import shutil
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

import psycopg

from commerce import runinfo
from commerce.audit.equality import canonical_report, compare
from commerce.ingestion.ledger import StaleWriterError, ingest_file
from commerce.ingestion.runner import ingest_range
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.recovery.demo_db import demo_database
from commerce.release.pipeline import run_rollback
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore
from commerce.serving.reader import read_snapshot

FIXTURE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "golden_v1"
ORDER_A = "fx0000000000000000000000000order_a"
ORDER_B = "fx0000000000000000000000000order_b"
FINAL = (24, 31)


class InjectedCrash(RuntimeError):
    pass


def once(point: str) -> Callable[[str], None]:
    fired = []

    def hook(p: str) -> None:
        if p == point and not fired:
            fired.append(1)
            raise InjectedCrash(point)
    return hook


@contextmanager
def code_identity(value: str):
    original = runinfo.code_sha
    runinfo.code_sha = lambda *a, **k: value
    try:
        yield
    finally:
        runinfo.code_sha = original


class Ctx:
    def __init__(self, db: dict, root: Path, ds: str, store_root: Path):
        self.db, self.root, self.ds, self.store_root = db, root, ds, store_root
        self.faults: dict = {}
        self.run_ids: list[str] = []

    def w(self, version: str = "baseline-v1", store_root: Path | None = None) -> Wiring:
        env = {"COMMERCE_WAREHOUSE_HOST": str(self.db["host"]), "COMMERCE_WAREHOUSE_PORT": str(self.db["port"]),
               "COMMERCE_WAREHOUSE_DB": self.db["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
               "COMMERCE_TRANSFORM_PASSWORD": self.db["passwords"]["commerce_transform"]}
        return Wiring(store_root=str(store_root or self.store_root), dataset_sha256=self.ds,
                      replay_version=version, artifacts_root=self.root / "builds", dsn_for=self.db["role_dsn"],
                      dbt_env=env, exact_fixture=True, faults=self.faults)

    def go(self, w: Wiring, name: str, first: int, last: int, mode: str = "manual", **kw) -> dict:
        cal = w.reader().calendar()
        out = run_pipeline(w, dag_run_id=name, mode=mode, interval_start=cal[first].start,
                           interval_end=cal[last - 1].end, **kw)
        if out["run_id"] not in self.run_ids:
            self.run_ids.append(out["run_id"])
        return out

    def q(self, sql: str, *params):
        with psycopg.connect(self.db["admin_dsn"]) as c:
            return c.execute(sql, params).fetchall()

    def n(self, table: str) -> int:
        return self.q(f"SELECT count(*) FROM {table}")[0][0]

    def serving(self) -> tuple[str | None, int]:
        rel, gen = self.q("SELECT release_id, generation FROM control.current_release")[0]
        return (None if rel is None else str(rel)), gen

    def publications(self) -> int:
        return self.n("control.publications")

    def snapshot_release(self) -> str | None:
        with psycopg.connect(self.db["role_dsn"]("commerce_dashboard")) as c:
            s = read_snapshot(c)
        return None if s.release is None or s.release["release_id"] is None else str(s.release["release_id"])

    def report(self, release_id) -> dict:
        schema = self.q("SELECT schema_name FROM control.releases WHERE release_id = %s", release_id)[0][0]
        with psycopg.connect(self.db["admin_dsn"]) as c:
            return canonical_report(c, schema)

    def bronze_matches_receipts(self) -> bool:
        rows = self.n("bronze.source_events")
        accepted = self.q("SELECT coalesce(sum(accepted_new_rows), 0) FROM control.file_receipts")[0][0]
        return rows == accepted

    def attempts(self, run_ids: list[str]) -> list[dict]:
        rows = self.q("SELECT run_id, stage, source_name, attempt, status, error_code FROM control.stage_results "
                      "WHERE run_id = ANY(%s::uuid[]) AND (status <> 'SUCCEEDED' OR attempt > 1) "
                      "ORDER BY started_at", run_ids)
        return [{"run": str(r[0])[:8], "stage": r[1], "source": r[2], "attempt": r[3], "status": r[4],
                 "error_code": r[5]} for r in rows]


def c01_kill_loader_before_commit(x: Ctx) -> dict:
    x.faults["ingest"] = once("before_commit")
    w = x.w()
    r1 = x.go(w, "c01", 7, 14)
    killed = {"status": r1["status"], "serving": x.serving(), "no_partial_commit": x.bronze_matches_receipts()}
    x.faults.clear()
    r2 = x.go(w, "c01", 7, 14)
    distinct = x.q("SELECT count(*) = count(DISTINCT (replay_version, source_name, entity_key, revision)) FROM bronze.source_events")[0][0]
    return {"injected": "ingest_file raises before_commit once (process killed inside the file transaction)",
            "recovery": "rerun the same orchestrator run: receipts skip finished files, the killed file is re-read",
            "observations": {"after_kill": killed, "after_retry": r2["status"], "events_unique": distinct},
            "checks": {"first_run_failed": r1["status"] != "SUCCEEDED",
                       "killed_transaction_left_no_rows": killed["no_partial_commit"],
                       "retry_succeeds": r2["status"] == "SUCCEEDED",
                       "each_event_once": bool(distinct) and x.bronze_matches_receipts()}}


def c02_drop_connection_after_commit(x: Ctx) -> dict:
    x.faults["ingest"] = once("after_commit")
    w = x.w()
    r1 = x.go(w, "c02", 14, 21)
    mid = {"status": r1["status"], "no_partial_commit": x.bronze_matches_receipts()}
    receipts_mid = x.n("control.file_receipts")
    x.faults.clear()
    r2 = x.go(w, "c02", 14, 21)
    dup_receipts = x.q("SELECT count(*) FROM (SELECT 1 FROM control.file_receipts GROUP BY replay_version, "
                       "source_name, interval_start HAVING count(*) > 1) d")[0][0]
    return {"injected": "ingest_file raises after the commit (client loses the acknowledgement)",
            "recovery": "rerun: the receipt is recognised and validated, no second insert",
            "observations": {"after_fault": mid, "receipts_after_fault": receipts_mid,
                             "receipts_after_retry": x.n("control.file_receipts")},
            "checks": {"first_run_failed": r1["status"] != "SUCCEEDED",
                       "committed_file_has_its_receipt": mid["no_partial_commit"],
                       "retry_succeeds": r2["status"] == "SUCCEEDED",
                       "one_receipt_per_file": dup_receipts == 0 and x.bronze_matches_receipts()}}


def c03_same_file_delivered_twice(x: Ctx) -> dict:
    dup = x.w("duplicate-v1-seed-42")
    with psycopg.connect(dup.dsn_for("commerce_ingest")) as c:
        for r in ingest_range(c, dup.store(), dup.reader(), code_sha="m", dependency_lock_sha="m", end_index=14):
            assert r.status == "INGESTED", r
    base_run = x.go(x.w(), "c03-base", 7, 14, mode="audit")
    dup_run = x.go(dup, "c03-dup", 7, 14, mode="audit")
    pub = x.q("SELECT release_id FROM control.releases WHERE run_id = %s", base_run["run_id"])[0][0]
    pub2 = x.q("SELECT release_id FROM control.releases WHERE run_id = %s", dup_run["run_id"])[0][0]
    diffs = compare(x.report(pub), x.report(pub2))
    dups = x.q("SELECT coalesce(sum(exact_duplicate_rows), 0) FROM control.file_receipts "
               "WHERE replay_version = 'duplicate-v1-seed-42'")[0][0]
    return {"injected": "the redelivery replay re-sends complete order bundles three days later",
            "recovery": "none needed: exact redelivery is a no-op for the business answer",
            "observations": {"exact_duplicate_rows_counted": dups, "differences": diffs},
            "checks": {"duplicates_were_detected_and_counted": dups > 0,
                       "business_checksums_equal_at_same_frontier": diffs == [],
                       "both_audit_runs_ok": base_run["status"] == dup_run["status"] == "SUCCEEDED"}}


def c04_same_key_altered_payload(x: Ctx) -> dict:
    build_scenario(RawStore(x.store_root), dataset_sha256=x.ds, base_version="baseline-v1", kind="fault",
                   variant="conflicting_payload", order_ids=[ORDER_A])
    w = x.w("fault-conflicting-payload-v1")
    before = x.serving()
    pubs = x.publications()
    outs = []
    for a, b in [(0, 7), (7, 14), (14, 21)]:
        outs.append(x.go(w, f"c04-{a}", a, b))
        if outs[-1]["status"] not in ("SUCCEEDED", "NO_CHANGE"):
            break
    failed = outs[-1]
    return {"injected": "a second event with the same key and revision but different content, same file",
            "recovery": "none by retry (deterministic); the source must be corrected under a new replay identity",
            "observations": {"statuses": [o["status"] for o in outs], "error": failed.get("error", "")[:300]},
            "checks": {"run_failed_closed": failed["status"] == "FAILED_CONTRACT",
                       "serving_release_unchanged": x.serving() == before,
                       "nothing_published": x.publications() == pubs,
                       "guard_released": x.q("SELECT owner_run_id FROM control.run_guard")[0][0] is None}}


def c05_one_source_missing_from_a_batch(x: Ctx) -> dict:
    store2 = x.root / "store-missing"
    shutil.copytree(x.store_root, store2)
    w = x.w(store_root=store2)
    reader = w.reader()
    m = reader.manifest(reader.calendar()[23].start)
    entry = next(e for e in m.sources if e.name == "order_payments")
    obj = store2 / entry.uri
    hidden = obj.with_suffix(obj.suffix + ".hidden")
    obj.rename(hidden)
    before, complete_before = x.serving(), x.q(
        "SELECT count(*) FROM control.intervals WHERE completeness = 'COMPLETE'")[0][0]
    r1 = x.go(w, "c05", 21, 28)
    mid = {"status": r1["status"], "serving": x.serving(),
           "complete_intervals": x.q("SELECT count(*) FROM control.intervals WHERE completeness = 'COMPLETE'")[0][0]}
    hidden.rename(obj)
    r2 = x.go(w, "c05", 21, 28)
    return {"injected": "the stored object of one required source is missing for one interval of the window",
            "recovery": "restore the object (same bytes, same checksum) and rerun the same run",
            "observations": {"after_fault": mid, "after_retry": r2["status"]},
            "checks": {"run_failed_before_any_write": r1["status"] == "FAILED_CONTRACT",
                       "no_interval_completed": mid["complete_intervals"] == complete_before,
                       "serving_release_unchanged": mid["serving"] == before,
                       "retry_succeeds_and_publishes": r2["status"] == "SUCCEEDED" and x.serving()[1] == before[1] + 1}}


def c06_quality_test_fails(x: Ctx) -> dict:
    build_scenario(RawStore(x.store_root), dataset_sha256=x.ds, base_version="baseline-v1", kind="fault",
                   variant="orphan_item", order_ids=[ORDER_A])
    w = x.w("fault-orphan-item-v1")
    before, pubs = x.serving(), x.publications()
    outs = []
    for a, b in [(0, 7), (7, 14), (14, 21)]:
        outs.append(x.go(w, f"c06-{a}", a, b, mode="audit"))
        if outs[-1]["status"] != "SUCCEEDED":
            break
    failed = outs[-1]
    rel = x.q("SELECT release_id, status FROM control.releases WHERE run_id = %s", failed["run_id"])
    failing = [r[0] for r in x.q("SELECT check_id FROM control.quality_results WHERE release_id = %s AND "
                                 "severity = 'BLOCK' AND NOT passed ORDER BY check_id", rel[0][0])] if rel else []
    return {"injected": "an order item whose order never arrives",
            "recovery": "none by retry; fix the input or the data policy, then publish only when gates pass (RB-03)",
            "observations": {"statuses": [o["status"] for o in outs], "failing_blocking_checks": failing,
                             "candidate_status": rel[0][1] if rel else None},
            "checks": {"candidate_rejected": bool(rel) and rel[0][1] == "REJECTED",
                       "blocking_check_recorded": bool(failing),
                       "serving_release_unchanged": x.serving() == before and x.publications() == pubs,
                       "dashboard_still_reads_prior_release": x.snapshot_release() == before[0]}}


def c07_interrupt_publication_before_commit(x: Ctx) -> dict:
    x.faults["publish"] = "before_commit"
    w = x.w()
    before, pubs = x.serving(), x.publications()
    r1 = x.go(w, "c07", 28, 31)
    mid = {"status": r1["status"], "serving": x.serving(), "dashboard_release": x.snapshot_release(),
           "publications": x.publications()}
    x.faults.clear()
    r2 = x.go(w, "c07", 28, 31)
    builds = x.q("SELECT count(*) FROM control.releases WHERE run_id = %s", r1["run_id"])[0][0]
    return {"injected": "publication transaction aborted after the view swap, before commit",
            "recovery": "rerun the same run: the validated candidate is reused and published once",
            "observations": {"after_fault": mid, "after_retry": r2["status"], "candidate_rows_for_run": builds},
            "checks": {"run_failed": r1["status"] != "SUCCEEDED",
                       "all_views_still_prior_release": mid["serving"] == before and mid["dashboard_release"] == before[0],
                       "no_publication_receipt": mid["publications"] == pubs,
                       "retry_publishes_without_rebuilding": r2["status"] == "SUCCEEDED" and builds == 1
                       and x.publications() == pubs + 1}}


def c08_lose_publication_acknowledgement(x: Ctx) -> dict:
    x.faults["publish"] = "after_commit"
    w = x.w()
    pubs = x.publications()
    with code_identity("matrix-code-fix"):
        r1 = x.go(w, "c08", *FINAL)
        mid = {"status": r1["status"], "publications": x.publications(), "serving": x.serving()}
        x.faults.clear()
        r2 = x.go(w, "c08", *FINAL)
    rel = x.q("SELECT release_id FROM control.releases WHERE run_id = %s", r1["run_id"])[0][0]
    n = x.q("SELECT count(*) FROM control.publications WHERE release_id = %s AND kind = 'PUBLISH'", rel)[0][0]
    return {"injected": "the publication commits, then the caller sees a failure",
            "recovery": "rerun: serving already carries this input digest, so the run ends NO_CHANGE and publishes nothing "
                        "(a direct re-publish answers ALREADY_COMMITTED; covered in test_publication)",
            "observations": {"after_fault": mid, "after_retry": r2["status"]},
            "checks": {"commit_survived_the_failed_acknowledgement": mid["publications"] == pubs + 1,
                       "retry_is_a_no_op": r2["status"] == "NO_CHANGE",
                       "exactly_one_publication_for_the_candidate": n == 1 and x.publications() == pubs + 1}}


def c09_old_attempt_writes_after_takeover(x: Ctx) -> dict:
    w = x.w("duplicate-v1-seed-42")
    reader = w.reader()
    iv = reader.calendar()[25]
    m = reader.manifest(iv.start)
    entry = next(e for e in m.sources if e.name == "orders")
    old_run = uuid.uuid4()
    with psycopg.connect(x.db["admin_dsn"], autocommit=True) as admin, \
            psycopg.connect(w.dsn_for("commerce_ingest"), autocommit=True) as ing:
        admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                      "code_sha, dependency_lock_sha, status) VALUES (%s, 'manual', 'x', now(), now() + "
                      "interval '1 second', 'm', 'm', 'CREATED')", (old_run,))
        gen = ing.execute("SELECT control.acquire_guard(%s)", (old_run,)).fetchone()[0]
        rows_before = x.n("bronze.source_events")
        takeover = uuid.uuid4()
        admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                      "code_sha, dependency_lock_sha, status) VALUES (%s, 'audit', 'op', now(), now() + "
                      "interval '1 second', 'o', 'o', 'CREATED')", (takeover,))
        new_gen = admin.execute("SELECT control.takeover_guard(%s, 'matrix', 'stale writer case')",
                                (takeover,)).fetchone()[0]
        refused = False
        try:
            ingest_file(ing, run_id=old_run, generation=gen, replay_version=w.replay_version,
                        interval_start=iv.start, interval_end=iv.end, manifest_sha256=m.sha256, entry=entry,
                        events=list(reader.events(m, "orders")))
        except StaleWriterError:
            refused = True
        admin.execute("SELECT control.release_guard(%s, %s)", (takeover, new_gen))
    return {"injected": "the guard is taken over while an old attempt still holds the previous generation",
            "recovery": "the old attempt is refused; a new run claims the guard normally",
            "observations": {"old_generation": gen, "new_generation": new_gen},
            "checks": {"stale_attempt_refused": refused,
                       "no_data_change": x.n("bronze.source_events") == rows_before,
                       "generation_moved": new_gen > gen}}


def c10_publish_during_dashboard_refresh(x: Ctx) -> dict:
    first = x.q("SELECT release_id FROM control.publications WHERE generation = 1")[0][0]
    cur_before = x.serving()
    started, finished, box = threading.Event(), threading.Event(), {}

    def switch():
        started.set()
        box["r"] = run_rollback(publish_dsn=x.db["role_dsn"]("commerce_publish"), target_release_id=first,
                                operator="matrix", reason="publish during refresh case")
        finished.set()

    t = threading.Thread(target=switch)
    waited = []

    def between():
        t.start()
        started.wait(5)
        waited.append(not finished.wait(1.5))

    with psycopg.connect(x.db["role_dsn"]("commerce_dashboard")) as c:
        snap = read_snapshot(c, between=between)
    t.join(30)
    seen = {str(snap.release["release_id"])} | {str(r["release_id"]) for r in snap.tables["pipeline_health"]}
    return {"injected": "a rollback publication starts while a dashboard refresh is half-way",
            "recovery": "none: the publisher waits for the open refresh, the next refresh sees the new release",
            "observations": {"release_ids_seen_in_one_refresh": sorted(seen)},
            "checks": {"publisher_waited_for_the_refresh": bool(waited) and waited[0],
                       "refresh_saw_exactly_one_release": seen == {cur_before[0]},
                       "next_refresh_sees_the_new_release": x.snapshot_release() == str(first)}}


CASES: list[tuple[str, str, Callable[[Ctx], dict]]] = [
    ("C01", "Kill loader before commit: transaction rolls back, no receipt, retry inserts each event once",
     c01_kill_loader_before_commit),
    ("C02", "Drop connection after commit: retry recognises the receipt, one receipt, unchanged events",
     c02_drop_connection_after_commit),
    ("C03", "Deliver the same file twice: exact redelivery is a no-op, duplicate count rises, checksum equal",
     c03_same_file_delivered_twice),
    ("C04", "Same event key, altered payload: deterministic contract failure, current release unchanged",
     c04_same_key_altered_payload),
    ("C05", "One source missing from a batch: no complete interval, no publication",
     c05_one_source_missing_from_a_batch),
    ("C06", "dbt model or quality test fails: candidate rejected, prior serving schema stays visible",
     c06_quality_test_fails),
    ("C07", "Interrupt publication before commit: whole transaction rolls back, views still prior release",
     c07_interrupt_publication_before_commit),
    ("C08", "Lose publication acknowledgement: commit survives, retry is a no-op, exactly one publication",
     c08_lose_publication_acknowledgement),
    ("C09", "Old task attempts write after takeover: fencing generation check rejects them",
     c09_old_attempt_writes_after_takeover),
    ("C10", "Publish during dashboard refresh: reader sees one coherent release, publisher waits",
     c10_publish_during_dashboard_refresh),
]


def run_matrix(server_dsn: str, workdir: Path, only: list[str] | None = None) -> dict:
    workdir.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="matrix-", dir=workdir))
    started = time.time()
    results = []
    with demo_database(server_dsn) as db:
        src = DirSource(shutil.copytree(FIXTURE, workdir / "csv", ignore=shutil.ignore_patterns("expected.json"),
                                        dirs_exist_ok=True))
        store = RawStore(workdir / "store")
        generate_baseline(src, store)
        ds = src.digest()
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="duplicate",
                       order_ids=[ORDER_A])
        x = Ctx(db, workdir, ds, workdir / "store")
        first = x.go(x.w(), "initial", 0, 7)
        assert first["status"] == "SUCCEEDED", first
        initial_release = x.serving()
        with psycopg.connect(db["admin_dsn"]) as c:
            server_version = c.execute("SHOW server_version").fetchone()[0]
        for cid, expectation, fn in CASES:
            report_it = not only or cid in only
            t0 = time.time()
            n_runs = len(x.run_ids)
            before = x.serving()
            try:
                rec = fn(x)
                error = None
            except Exception as exc:  # noqa: BLE001
                rec, error = {"checks": {}, "observations": {}, "injected": "", "recovery": ""}, repr(exc)
            checks = rec["checks"]
            if not report_it:
                continue
            results.append({
                "case": cid, "expectation": expectation, "injected": rec.get("injected"),
                "recovery": rec.get("recovery"), "release_before": before[0], "generation_before": before[1],
                "release_after": x.serving()[0], "generation_after": x.serving()[1],
                "attempts": x.attempts(x.run_ids[n_runs:]), "observations": rec.get("observations"),
                "checks": checks, "error": error, "seconds": round(time.time() - t0, 1),
                "outcome": "PASS" if checks and all(checks.values()) and not error else "FAIL"})
        env = {"python": sys.version.split()[0], "platform": platform.platform(), "machine": platform.machine(),
               "postgresql": server_version, "initial_release": initial_release[0],
               "dataset": "golden fixture (synthetic, 31 replay intervals)", "wall_seconds": round(time.time() - started)}
    return {"what": "Blueprint §26 fault-injection matrix, run on a throwaway demo database", "environment": env,
            "summary": {"passed": sum(r["outcome"] == "PASS" for r in results), "total": len(results)},
            "cases": results}


def write_report(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
