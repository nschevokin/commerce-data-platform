from __future__ import annotations

import functools
import json
import uuid
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql

from commerce import dbt_runner, runinfo
from commerce.build.candidate import build_candidate, input_digest, resolve_frontier
from commerce.contracts.canonical import utc_iso
from commerce.contracts.manifest import ContractError
from commerce.contracts.sources import REQUIRED_SOURCE_NAMES
from commerce.ingestion.ledger import FileRejected, complete_interval, ingest_file
from commerce.ingestion.runner import MAX_INTERVALS_PER_RUN, _write_validation_artefact
from commerce.orchestration import ledger as L
from commerce.orchestration.errors import (
    DataInvalid,
    InputNotReady,
    PipelineFailed,
    QualityFailed,
    RunConfigError,
    VerificationFailed,
)
from commerce.orchestration.wiring import Wiring
from commerce.release import quality, registry
from commerce.release.contract import SERVING_VIEWS, safe_schema
from commerce.release.publisher import Outcome, publish
from commerce.replay.events import Interval
from commerce.replay.reader import IntervalIncomplete
from commerce.serving.reader import read_snapshot

SOURCES = REQUIRED_SOURCE_NAMES
BASE_STAGES = [("resolve_run", ""), ("claim_writer", ""), ("resolve_manifest", ""), ("verify_manifest", ""),
               *[("ingest_source", s) for s in SOURCES], ("reconcile_ingestion", ""), ("choose_frontier", "")]
RELEASE_STAGES = [("prepare_candidate", ""), ("build_candidate", ""), ("validate_candidate", ""),
                  ("publish_release", ""), ("verify_serving", "")]
RUN_MODES = ("scheduled", "manual", "backfill", "audit")
PUBLICATION_STAGES = [("publish_release", ""), ("verify_serving", "")]
FAILURE_STATUSES = {"FAILED_CONTRACT", "FAILED_QUALITY", "FAILED_TRANSIENT", "FAILED_VERIFICATION",
                    "INPUT_NOT_READY"}


def _run_arg(fn):
    @functools.wraps(fn)
    def inner(w, run_id, *a, **k):
        return fn(w, uuid.UUID(str(run_id)), *a, **k)
    return inner


def _intervals(w: Wiring, run: dict) -> list[Interval]:
    return [iv for iv in w.reader().calendar() if run["interval_start"] <= iv.start < run["interval_end"]]


def _run(w: Wiring, run_id) -> dict:
    with w.connect("commerce_transform") as c:
        return L.load_run(c, run_id)


def _release_for_run(conn: psycopg.Connection, run_id) -> dict | None:
    row = conn.execute(
        "SELECT release_id, schema_name, status, frontier_end, input_digest FROM control.releases "
        "WHERE run_id = %s ORDER BY created_at DESC LIMIT 1", (run_id,)).fetchone()
    if row is None:
        return None
    return dict(zip(("release_id", "schema_name", "status", "frontier_end", "input_digest"), row, strict=True))


def _is_no_change(run: dict) -> bool:
    return run["status"] == "NO_CHANGE"


def _is_audit(run: dict) -> bool:
    return run["mode"] == "audit"


def _serving_state(conn: psycopg.Connection):
    return conn.execute(
        "SELECT r.input_digest, r.frontier_end FROM control.current_release c "
        "JOIN control.releases r USING (release_id)").fetchone()


def resolve_run(w: Wiring, *, dag_run_id: str, mode: str, interval_start: datetime,
                interval_end: datetime) -> str:
    if mode not in RUN_MODES:
        raise RunConfigError(f"unknown mode {mode!r}")
    cal = w.reader().calendar()
    covered = [iv for iv in cal if interval_start <= iv.start < interval_end]
    if (not covered or covered[0].start != interval_start or covered[-1].end != interval_end
            or any(a.end != b.start for a, b in zip(covered, covered[1:], strict=False))):
        raise RunConfigError("the window must start and end on replay interval boundaries")
    if len(covered) > MAX_INTERVALS_PER_RUN:
        raise RunConfigError(f"a run covers at most {MAX_INTERVALS_PER_RUN} intervals; submit serial ranges")
    run_id = L.run_id_for(w.replay_version, dag_run_id)
    with w.connect("commerce_ingest") as conn:
        with conn.transaction():
            conn.execute(
                "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                "code_sha, dependency_lock_sha, airflow_dag_run_id, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'CREATED') ON CONFLICT (run_id) DO NOTHING",
                (run_id, mode, w.replay_version, interval_start, interval_end, runinfo.code_sha(),
                 runinfo.dependency_lock_sha(), dag_run_id))
        run = L.load_run(conn, run_id)
    if (run["interval_start"], run["interval_end"], run["mode"]) != (interval_start, interval_end, mode):
        raise RunConfigError(f"run {run_id} already exists with a different window or mode")
    with L.tracked(w, run_id, "resolve_run"):
        pass
    return str(run_id)


@_run_arg
def claim_writer(w: Wiring, run_id) -> int:
    with L.tracked(w, run_id, "claim_writer", role="commerce_ingest"), w.connect("commerce_ingest") as c:
        return L.ensure_writer(c, run_id)


@_run_arg
def manifests_ready(w: Wiring, run_id) -> bool:
    run = _run(w, run_id)
    reader = w.reader()
    try:
        for iv in _intervals(w, run):
            reader.manifest(iv.start)
    except IntervalIncomplete:
        return False
    return True


@_run_arg
def resolve_manifest(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    reader = w.reader()
    with L.tracked(w, run_id, "resolve_manifest"):
        hashes = []
        for iv in _intervals(w, run):
            try:
                hashes.append(reader.manifest(iv.start).sha256)
            except IntervalIncomplete as exc:
                L.set_run_status(w, run_id, "INPUT_NOT_READY")
                raise InputNotReady(str(exc)) from exc
        with w.connect("commerce_ingest") as conn:
            conn.execute("UPDATE control.runs SET manifest_sha256s = %s WHERE run_id = %s", (hashes, run_id))
    return {"manifests": hashes}


@_run_arg
def verify_manifest(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    reader = w.reader()
    files = 0
    with L.tracked(w, run_id, "verify_manifest"):
        try:
            for iv in _intervals(w, run):
                m = reader.manifest(iv.start)
                for entry in m.sources:
                    for _ in reader.events(m, entry.name):
                        pass
                    files += 1
        except ContractError:
            L.set_run_status(w, run_id, "FAILED_CONTRACT")
            raise
        L.set_run_status(w, run_id, "INPUTS_VERIFIED")
    return {"files_verified": files}


@_run_arg
def ingest_source(w: Wiring, run_id, source: str) -> dict:
    if source not in SOURCES:
        raise RunConfigError(f"unknown source {source!r}")
    run = _run(w, run_id)
    reader, store = w.reader(), w.store()
    new_rows = dup_rows = receipts = files = 0
    with L.tracked(w, run_id, "ingest_source", source, role="commerce_ingest"), \
            w.connect("commerce_ingest") as conn:
        generation = L.ensure_writer(conn, run_id)
        for iv in _intervals(w, run):
            m = reader.manifest(iv.start)
            entry = next((e for e in m.sources if e.name == source), None)
            if entry is None:
                continue
            try:
                out = ingest_file(conn, run_id=run_id, generation=generation, replay_version=w.replay_version,
                                  interval_start=iv.start, interval_end=iv.end, manifest_sha256=m.sha256,
                                  entry=entry, events=list(reader.events(m, source)),
                                  fault=w.faults.get("ingest"))
            except FileRejected as exc:
                uri = _write_validation_artefact(store, reader, run_id, iv, exc)
                L.set_run_status(w, run_id, "FAILED_CONTRACT", role="commerce_ingest")
                raise DataInvalid(f"{exc} (report: {uri})") from exc
            files += 1
            if out.already_committed:
                receipts += 1
            else:
                new_rows += out.accepted_new_rows
                dup_rows += out.exact_duplicate_rows
    return {"source": source, "files": files, "already_committed": receipts,
            "accepted_new_rows": new_rows, "exact_duplicate_rows": dup_rows}


@_run_arg
def reconcile_ingestion(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    reader = w.reader()
    with L.tracked(w, run_id, "reconcile_ingestion", role="commerce_ingest"), \
            w.connect("commerce_ingest") as conn:
        generation = L.ensure_writer(conn, run_id)
        ivs = _intervals(w, run)
        for iv in ivs:
            m = reader.manifest(iv.start)
            complete_interval(conn, run_id=run_id, generation=generation, replay_version=w.replay_version,
                              interval_start=iv.start, interval_end=iv.end, manifest_sha256=m.sha256,
                              entries=m.sources, total_rows=m.total_rows)
        L.set_run_status(w, run_id, "INGESTED", role="commerce_ingest")
    return {"intervals_completed": len(ivs)}


@_run_arg
def choose_frontier(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    reader = w.reader()
    with L.tracked(w, run_id, "choose_frontier"), w.connect("commerce_transform") as conn:
        L.ensure_writer(conn, run_id)
        frontier = resolve_frontier(conn, reader, run["interval_end"])
        digest = input_digest(dataset_sha256=w.dataset_sha256, frontier=frontier,
                              code_sha=runinfo.code_sha(), dependency_lock_sha=runinfo.dependency_lock_sha())
        serving = _serving_state(conn)
        reason = None
        if serving is not None and not _is_audit(run):
            if serving[0] == digest:
                reason = "identical input digest to the serving release"
            elif serving[1] > run["interval_end"]:
                reason = f"serving frontier {utc_iso(serving[1])} is already beyond this window"
        if reason:
            L.set_run_status(w, run_id, "NO_CHANGE")
            return {"decision": "NO_CHANGE", "reason": reason, "input_digest": digest}
    return {"decision": "BUILD", "frontier_end": utc_iso(run["interval_end"]), "input_digest": digest}


@_run_arg
def prepare_candidate(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    if _is_no_change(run):
        L.mark_skipped(w, run_id, "prepare_candidate")
        return {"skipped": True}
    reader = w.reader()
    with L.tracked(w, run_id, "prepare_candidate"), w.connect("commerce_transform") as conn:
        generation = L.ensure_writer(conn, run_id)
        existing = _release_for_run(conn, run_id)
        if existing and existing["status"] in ("BUILT", "VALIDATED", "REJECTED"):
            return {"release_id": str(existing["release_id"]), "schema": existing["schema_name"],
                    "reused": True}
        if existing and existing["status"] == "BUILDING":
            registry.set_status(conn, existing["release_id"], run_id, generation, "DROPPED")
        frontier = resolve_frontier(conn, reader, run["interval_end"])
        digest = input_digest(dataset_sha256=w.dataset_sha256, frontier=frontier,
                              code_sha=runinfo.code_sha(), dependency_lock_sha=runinfo.dependency_lock_sha())
        release_id, schema = uuid.uuid4(), f"cand_{uuid.uuid4().hex}"
        registry.register_release(
            conn, release_id=release_id, schema=schema, run_id=run_id, generation=generation,
            replay_version=w.replay_version, frontier_end=run["interval_end"], input_digest=digest,
            code_sha=runinfo.code_sha(), dependency_lock_sha=runinfo.dependency_lock_sha())
    return {"release_id": str(release_id), "schema": schema, "reused": False}


def _report_path(w: Wiring, schema: str) -> Path:
    return w.artifacts_root / schema / "build_report.json"


@_run_arg
def build_candidate_stage(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    if _is_no_change(run):
        L.mark_skipped(w, run_id, "build_candidate")
        return {"skipped": True}
    reader = w.reader()
    with L.tracked(w, run_id, "build_candidate"), w.connect("commerce_transform") as conn:
        generation = L.ensure_writer(conn, run_id)
        rel = _release_for_run(conn, run_id)
        if rel is None:
            raise ContractError("prepare_candidate has not registered a release for this run")
        if rel["status"] != "BUILDING":
            return {"release_id": str(rel["release_id"]), "reused": True}
        dsn = w.dsn_for("commerce_transform")
        out = build_candidate(
            transform_dsn=dsn, transform_env=dict(w.dbt_env), reader=reader, frontier_end=run["interval_end"],
            artifacts_root=w.artifacts_root, exact_fixture=w.exact_fixture, run_id=str(run_id),
            release_id=str(rel["release_id"]), candidate_schema_name=rel["schema_name"])
        rep = out["report"]
        path = _report_path(w, rel["schema_name"])
        path.write_text(json.dumps({"returncode": rep.returncode, "executed": rep.executed,
                                    "expected": rep.expected, "warnings": rep.warnings,
                                    "failures": rep.failures}), encoding="utf-8")
        registry.set_status(conn, rel["release_id"], run_id, generation, "BUILT")
        L.set_run_status(w, run_id, "CANDIDATE_BUILT")
    return {"release_id": str(rel["release_id"]), "dbt_ok": rep.ok, "nodes": rep.executed}


@_run_arg
def validate_candidate(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    if _is_no_change(run):
        L.mark_skipped(w, run_id, "validate_candidate")
        return {"skipped": True}
    with L.tracked(w, run_id, "validate_candidate"), w.connect("commerce_transform") as conn:
        generation = L.ensure_writer(conn, run_id)
        rel = _release_for_run(conn, run_id)
        if rel is None:
            raise ContractError("no release for this run")
        if rel["status"] == "BUILT":
            data = json.loads(_report_path(w, rel["schema_name"]).read_text(encoding="utf-8"))
            report = dbt_runner.BuildReport(Path(rel["schema_name"]), data["returncode"], data["executed"],
                                            data["expected"], data["warnings"], data["failures"])
            results = quality.evaluate(conn, schema=rel["schema_name"], replay_version=w.replay_version,
                                       frontier_end=rel["frontier_end"], dbt_report=report,
                                       exact_fixture=w.exact_fixture)
            status = registry.record_validation(conn, release_id=rel["release_id"], schema=rel["schema_name"],
                                                run_id=run_id, generation=generation, results=results)
        elif rel["status"] in ("VALIDATED", "REJECTED"):
            status = rel["status"]
        else:
            raise ContractError(f"release {rel['release_id']} is {rel['status']}, expected BUILT")
        if status == "REJECTED":
            failed = [r[0] for r in conn.execute(
                "SELECT check_id FROM control.quality_results WHERE release_id = %s AND severity = 'BLOCK' "
                "AND NOT passed ORDER BY check_id", (rel["release_id"],)).fetchall()]
            L.set_run_status(w, run_id, "FAILED_QUALITY")
            raise QualityFailed("blocking checks failed: " + ", ".join(failed))
        L.set_run_status(w, run_id, "VALIDATED")
    return {"release_id": str(rel["release_id"]), "status": status}


@_run_arg
def publish_release(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    if _is_no_change(run) or _is_audit(run):
        L.mark_skipped(w, run_id, "publish_release")
        return {"skipped": True, "audit_only": _is_audit(run)}
    with L.tracked(w, run_id, "publish_release", role="commerce_publish"), \
            w.connect("commerce_publish") as conn:
        generation = L.ensure_writer(conn, run_id)
        rel = _release_for_run(conn, run_id)
        if rel is None:
            raise ContractError("no release for this run")
        res = publish(conn, release_id=rel["release_id"], run_id=run_id, generation=generation,
                      fault=w.faults.get("publish"))
        L.set_run_status(w, run_id, "PUBLISHED", role="commerce_publish")
    return {"release_id": res.release_id, "serving_generation": res.generation,
            "already_committed": res.outcome is Outcome.ALREADY_COMMITTED}


@_run_arg
def verify_serving(w: Wiring, run_id) -> dict:
    run = _run(w, run_id)
    if _is_no_change(run) or _is_audit(run):
        L.mark_skipped(w, run_id, "verify_serving")
        return {"skipped": True, "audit_only": _is_audit(run)}
    with L.tracked(w, run_id, "verify_serving"), w.connect("commerce_transform") as conn:
        rel = _release_for_run(conn, run_id)
        with psycopg.connect(w.dsn_for("commerce_dashboard")) as dash:
            snap = read_snapshot(dash)
        problems = []
        if snap.release is None or str(snap.release["release_id"]) != str(rel["release_id"]):
            problems.append(f"serving release is {snap.release and snap.release['release_id']}, "
                            f"expected {rel['release_id']}")
        else:
            ident = safe_schema(rel["schema_name"])
            for view, (table, _) in SERVING_VIEWS.items():
                n = conn.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                    ident, sql.Identifier(table))).fetchone()[0]
                if n != len(snap.tables[view]):
                    problems.append(f"{view}: dashboard sees {len(snap.tables[view])} rows, candidate has {n}")
            cand_total = conn.execute(sql.SQL(
                "SELECT coalesce(sum(merchandise_value_brl), 0) FROM {}.mart_sales_daily").format(ident)
            ).fetchone()[0]
            served_total = sum(r["merchandise_value_brl"] for r in snap.tables["sales_daily"])
            if cand_total != served_total:
                problems.append(f"merchandise total: dashboard {served_total}, candidate {cand_total}")
        if problems:
            L.set_run_status(w, run_id, "FAILED_VERIFICATION")
            raise VerificationFailed("; ".join(problems))
        L.set_run_status(w, run_id, "VERIFIED")
    return {"release_id": str(rel["release_id"]), "verified_views": len(SERVING_VIEWS)}


@_run_arg
def pipeline_result(w: Wiring, run_id) -> dict:
    with w.connect("commerce_transform") as conn:
        try:
            run = L.load_run(conn, run_id)
        except ContractError as exc:
            raise PipelineFailed(f"no control.runs row: {exc}") from exc
        stages = L.latest_stages(conn, run_id)
        no_change = run["status"] == "NO_CHANGE"
        problems, codes = [], []
        for key in BASE_STAGES + RELEASE_STAGES:
            state = stages.get(key)
            label = key[0] + (f"[{key[1]}]" if key[1] else "")
            if state is None:
                problems.append(f"{label}: no evidence")
            elif state[0] == "SKIPPED" and no_change and key in RELEASE_STAGES:
                continue
            elif state[0] == "SKIPPED" and run["mode"] == "audit" and key in PUBLICATION_STAGES:
                continue
            elif state[0] != "SUCCEEDED":
                problems.append(f"{label}: {state[0]}{' ' + state[1] if state[1] else ''}")
                codes.append(state[1])
        failed = bool(problems)
        if failed:
            final = final_status(run["status"], codes)
            if final == "FAILED_TRANSIENT" and ("resolve_manifest", "") not in stages \
                    and not manifests_ready_safe(w, run_id):
                final = "INPUT_NOT_READY"
        else:
            final = "NO_CHANGE" if no_change else "SUCCEEDED"
        released = L.release_writer(conn, run_id)
        conn.execute("UPDATE control.runs SET status = %s, updated_at = now() WHERE run_id = %s",
                     (final, run_id))
    with L.tracked(w, run_id, "pipeline_result"):
        if failed:
            raise PipelineFailed(f"run {run_id} ended {final}: " + "; ".join(problems))
    return {"run_id": str(run_id), "status": final, "guard_released": released}


_CODE_TO_STATUS = {"INPUT_NOT_READY": "INPUT_NOT_READY", "QUALITY_FAILED": "FAILED_QUALITY",
                   "VERIFICATION_FAILED": "FAILED_VERIFICATION", "DATA_INVALID": "FAILED_CONTRACT",
                   "CONTRACT": "FAILED_CONTRACT", "RUN_CONFIG": "FAILED_CONTRACT",
                   "STALE_WRITER": "FAILED_CONTRACT",
                   "FRONTIER_NOT_READY": "INPUT_NOT_READY", "GENERATION_MOVED": "FAILED_CONTRACT"}


def final_status(current: str, codes: list[str | None]) -> str:
    if current in FAILURE_STATUSES:
        return current
    for code in codes:
        if code in _CODE_TO_STATUS:
            return _CODE_TO_STATUS[code]
    return "FAILED_TRANSIENT"


def manifests_ready_safe(w: Wiring, run_id) -> bool:
    try:
        return manifests_ready(w, run_id)
    except Exception:
        return True
