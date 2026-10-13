from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import psycopg

from commerce import runinfo
from commerce.build.candidate import build_candidate, input_digest, resolve_frontier
from commerce.ingestion.ledger import translate_db_error
from commerce.release import quality, registry
from commerce.release.publisher import Outcome, PublicationRefused, publish
from commerce.replay.reader import ReplayReader
from commerce.serving.reader import read_snapshot


@dataclass
class ReleaseRun:
    run_id: str
    status: str
    release_id: str | None = None
    candidate_schema: str | None = None
    generation: int | None = None
    detail: str = ""
    checks: list[dict] = field(default_factory=list)

    def to_obj(self) -> dict:
        return {"run_id": self.run_id, "status": self.status, "release_id": self.release_id,
                "candidate_schema": self.candidate_schema, "serving_generation": self.generation,
                "detail": self.detail, "checks": self.checks}


def _set_run(conn: psycopg.Connection, run_id, status: str) -> None:
    with conn.transaction():
        conn.execute("UPDATE control.runs SET status = %s, updated_at = now() WHERE run_id = %s",
                     (status, run_id))


def run_release(*, publish_dsn: str, transform_dsn: str, dashboard_dsn: str, transform_env: dict[str, str],
                reader: ReplayReader, frontier_end: datetime, artifacts_root: Path, exact_fixture: bool,
                dataset_sha256: str, fault: str | None = None, mode: str = "manual") -> ReleaseRun:
    run_id = uuid.uuid4()
    code_sha, lock_sha = runinfo.code_sha(), runinfo.dependency_lock_sha()
    with (psycopg.connect(publish_dsn, autocommit=True) as pub,
          psycopg.connect(transform_dsn, autocommit=True) as trn):
        frontier = resolve_frontier(trn, reader, frontier_end)
        digest = input_digest(dataset_sha256=dataset_sha256, frontier=frontier, code_sha=code_sha,
                              dependency_lock_sha=lock_sha)
        last_iv = next(iv for iv in reader.calendar() if iv.end == frontier_end)
        with pub.transaction():
            pub.execute(
                "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                "manifest_sha256s, code_sha, dependency_lock_sha, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'CREATED')",
                (run_id, mode, reader.replay_version, last_iv.start, last_iv.end,
                 list(frontier.manifest_sha256s), code_sha, lock_sha))
        try:
            with pub.transaction():
                generation = pub.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
        except psycopg.Error as exc:
            _set_run(pub, run_id, "FAILED_TRANSIENT")
            raise translate_db_error(exc) from exc
        result = ReleaseRun(str(run_id), "CREATED")
        try:
            current = pub.execute(
                "SELECT r.input_digest FROM control.current_release c JOIN control.releases r "
                "USING (release_id)").fetchone()
            if current is not None and current[0] == digest:
                _set_run(pub, run_id, "NO_CHANGE")
                result.status, result.detail = "NO_CHANGE", "same input digest as the serving release"
                return result
            release_id = uuid.uuid4()
            schema = f"cand_{uuid.uuid4().hex}"
            registry.register_release(trn, release_id=release_id, schema=schema, run_id=run_id,
                                      generation=generation, replay_version=reader.replay_version,
                                      frontier_end=frontier_end, input_digest=digest, code_sha=code_sha,
                                      dependency_lock_sha=lock_sha)
            result.release_id, result.candidate_schema, result.generation = str(release_id), schema, generation
            built = build_candidate(transform_dsn=transform_dsn, transform_env=transform_env, reader=reader,
                                    frontier_end=frontier_end, artifacts_root=artifacts_root,
                                    exact_fixture=exact_fixture, run_id=str(run_id),
                                    release_id=str(release_id), candidate_schema_name=schema)
            registry.set_status(trn, release_id, run_id, generation, "BUILT")
            _set_run(pub, run_id, "CANDIDATE_BUILT")
            checks = quality.evaluate(trn, schema=schema, replay_version=reader.replay_version,
                                      frontier_end=frontier_end, dbt_report=built["report"],
                                      exact_fixture=exact_fixture)
            result.checks = [{"check": c.check_id, "severity": c.severity, "passed": c.passed,
                              "observed": None if c.observed is None else str(c.observed)} for c in checks]
            status = registry.record_validation(trn, release_id=release_id, schema=schema, run_id=run_id,
                                                generation=generation, results=checks)
            if status != "VALIDATED":
                _set_run(pub, run_id, "FAILED_QUALITY")
                result.status = "FAILED_QUALITY"
                result.detail = "blocking checks failed: " + ", ".join(
                    c.check_id for c in checks if c.severity == quality.BLOCK and not c.passed)
                return result
            _set_run(pub, run_id, "VALIDATED")
            pr = publish(pub, release_id=release_id, run_id=run_id, generation=generation, fault=fault)
            result.generation = pr.generation
            _set_run(pub, run_id, "PUBLISHED")
            if pr.outcome is Outcome.ALREADY_COMMITTED:
                result.detail = "publication was already committed"
            with psycopg.connect(dashboard_dsn) as dash:
                snap = read_snapshot(dash)
            if snap.release is None or str(snap.release["release_id"]) != str(release_id):
                _set_run(pub, run_id, "FAILED_VERIFICATION")
                result.status, result.detail = "FAILED_VERIFICATION", "dashboard role does not see the release"
                return result
            _set_run(pub, run_id, "VERIFIED")
            _set_run(pub, run_id, "SUCCEEDED")
            result.status = "SUCCEEDED"
            return result
        except PublicationRefused as exc:
            _set_run(pub, run_id, "FAILED_QUALITY")
            result.status, result.detail = "FAILED_QUALITY", str(exc)
            return result
        finally:
            try:
                if pub.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    pub.rollback()
                with pub.transaction():
                    pub.execute("SELECT control.release_guard(%s, %s)", (run_id, generation))
            except psycopg.Error:
                pass


def run_rollback(*, publish_dsn: str, target_release_id, operator: str, reason: str,
                 incident: str | None = None, fault: str | None = None):
    run_id = uuid.uuid4()
    code_sha, lock_sha = runinfo.code_sha(), runinfo.dependency_lock_sha()
    with psycopg.connect(publish_dsn, autocommit=True) as pub:
        with pub.transaction():
            pub.execute(
                "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                "code_sha, dependency_lock_sha, status) VALUES (%s, 'audit', 'rollback', now(), "
                "now() + interval '1 second', %s, %s, 'CREATED')", (run_id, code_sha, lock_sha))
        try:
            with pub.transaction():
                generation = pub.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
        except psycopg.Error as exc:
            _set_run(pub, run_id, "FAILED_TRANSIENT")
            raise translate_db_error(exc) from exc
        try:
            result = publish(pub, release_id=target_release_id, run_id=run_id, generation=generation,
                             kind="ROLLBACK", operator=operator, reason=reason, incident=incident,
                             fault=fault)
            _set_run(pub, run_id, "SUCCEEDED")
            return result
        except BaseException:
            if pub.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                pub.rollback()
            _set_run(pub, run_id, "FAILED_QUALITY")
            raise
        finally:
            try:
                if pub.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    pub.rollback()
                with pub.transaction():
                    pub.execute("SELECT control.release_guard(%s, %s)", (run_id, generation))
            except psycopg.Error:
                pass
