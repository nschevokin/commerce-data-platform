from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import psycopg

from commerce.contracts.canonical import canonical_json, utc_iso
from commerce.contracts.manifest import ContractError, Manifest
from commerce.ingestion.ledger import (
    FaultHook,
    FileOutcome,
    FileRejected,
    StaleWriterError,
    complete_interval,
    ingest_file,
    translate_db_error,
)
from commerce.replay.events import Interval
from commerce.replay.reader import IntervalIncomplete, ReplayReader
from commerce.replay.store import RawStore

MAX_INTERVALS_PER_RUN = 7


@dataclass
class RunResult:
    run_id: str
    status: str
    intervals_completed: int = 0
    files: list[FileOutcome] = field(default_factory=list)
    detail: str = ""
    artefact_uri: str | None = None

    def to_obj(self) -> dict:
        return {
            "run_id": self.run_id, "status": self.status,
            "intervals_completed": self.intervals_completed, "detail": self.detail,
            "artefact_uri": self.artefact_uri,
            "files": len(self.files), "already_committed_files": sum(f.already_committed for f in self.files),
            "input_rows": sum(f.input_rows for f in self.files if not f.already_committed),
            "accepted_new_rows": sum(f.accepted_new_rows for f in self.files if not f.already_committed),
            "exact_duplicate_rows": sum(f.exact_duplicate_rows for f in self.files
                                        if not f.already_committed),
        }


def _set_status(conn: psycopg.Connection, run_id, status: str) -> None:
    with conn.transaction():
        conn.execute("UPDATE control.runs SET status = %s, updated_at = now() WHERE run_id = %s",
                     (status, run_id))


def _stage(conn: psycopg.Connection, run_id, stage: str, source: str, status: str,
           error_code: str | None = None, artifact_uri: str | None = None) -> None:
    with conn.transaction():
        conn.execute(
            "INSERT INTO control.stage_results (run_id, stage, source_name, attempt, status, error_code, "
            "artifact_uri, finished_at) VALUES (%s, %s, %s, 1, %s, %s, %s, "
            "CASE WHEN %s = 'RUNNING' THEN NULL ELSE now() END) "
            "ON CONFLICT (run_id, stage, source_name, attempt) DO UPDATE SET status = EXCLUDED.status, "
            "error_code = EXCLUDED.error_code, artifact_uri = EXCLUDED.artifact_uri, "
            "finished_at = EXCLUDED.finished_at",
            (run_id, stage, source, status, error_code, artifact_uri, status))


def ingest_interval_chunk(conn: psycopg.Connection, store: RawStore, reader: ReplayReader,
                          intervals: list[Interval], *, code_sha: str, dependency_lock_sha: str,
                          mode: str = "manual", fault: FaultHook | None = None) -> RunResult:
    if not 1 <= len(intervals) <= MAX_INTERVALS_PER_RUN:
        raise ContractError(f"a run covers 1..{MAX_INTERVALS_PER_RUN} intervals, got {len(intervals)}")
    run_id = uuid.uuid4()
    rv = reader.replay_version

    manifests: list[Manifest] = []
    missing: str | None = None
    try:
        for iv in intervals:
            m = reader.manifest(iv.start)
            for entry in m.sources:
                for _ in reader.events(m, entry.name):
                    pass
            manifests.append(m)
    except IntervalIncomplete as exc:
        missing = str(exc)

    with conn.transaction():
        conn.execute(
            "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
            "manifest_sha256s, code_sha, dependency_lock_sha, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'CREATED')",
            (run_id, mode, rv, intervals[0].start, intervals[-1].end, [m.sha256 for m in manifests],
             code_sha, dependency_lock_sha))
    if missing:
        _set_status(conn, run_id, "INPUT_NOT_READY")
        return RunResult(str(run_id), "INPUT_NOT_READY", detail=missing)
    _set_status(conn, run_id, "INPUTS_VERIFIED")

    try:
        with conn.transaction():
            generation = conn.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
    except psycopg.Error as exc:
        _set_status(conn, run_id, "FAILED_TRANSIENT")
        raise translate_db_error(exc) from exc

    result = RunResult(str(run_id), "INGESTED")
    try:
        for iv, m in zip(intervals, manifests, strict=True):
            for entry in m.sources:
                stage = f"ingest:{iv.day}"
                _stage(conn, run_id, stage, entry.name, "RUNNING")
                events = list(reader.events(m, entry.name))
                try:
                    out = ingest_file(conn, run_id=run_id, generation=generation, replay_version=rv,
                                      interval_start=iv.start, interval_end=iv.end,
                                      manifest_sha256=m.sha256, entry=entry, events=events, fault=fault)
                except FileRejected as exc:
                    uri = _write_validation_artefact(store, reader, run_id, iv, exc)
                    _stage(conn, run_id, stage, entry.name, "FAILED", "FILE_REJECTED", uri)
                    _set_status(conn, run_id, "FAILED_CONTRACT")
                    result.status, result.detail, result.artefact_uri = "FAILED_CONTRACT", str(exc), uri
                    return result
                _stage(conn, run_id, stage, entry.name, "SUCCEEDED")
                result.files.append(out)
            complete_interval(conn, run_id=run_id, generation=generation, replay_version=rv,
                              interval_start=iv.start, interval_end=iv.end, manifest_sha256=m.sha256,
                              entries=m.sources, total_rows=m.total_rows)
            result.intervals_completed += 1
        _set_status(conn, run_id, "INGESTED")
        return result
    except StaleWriterError as exc:
        _set_status(conn, run_id, "FAILED_TRANSIENT")
        result.status, result.detail = "FAILED_TRANSIENT", str(exc)
        raise
    except BaseException:
        _set_status_quietly(conn, run_id, "FAILED_TRANSIENT")
        raise
    finally:
        _release_quietly(conn, run_id, generation)


def _set_status_quietly(conn, run_id, status) -> None:
    try:
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            conn.rollback()
        _set_status(conn, run_id, status)
    except psycopg.Error:
        pass


def _release_quietly(conn, run_id, generation) -> None:
    try:
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            conn.rollback()
        with conn.transaction():
            conn.execute("SELECT control.release_guard(%s, %s)", (run_id, generation))
    except psycopg.Error:
        pass


def _write_validation_artefact(store: RawStore, reader: ReplayReader, run_id, iv: Interval,
                               exc: FileRejected) -> str:
    uri = (f"validation/dataset={reader.dataset_sha256}/replay={reader.replay_version}/"
           f"run={run_id}/day={iv.day}/source={exc.source}.json")
    body = {"run_id": str(run_id), "replay_version": reader.replay_version,
            "interval_start": utc_iso(iv.start), "source": exc.source, "file_sha256": exc.file_sha256,
            "total_issues": exc.total_issues, "listed_issues": len(exc.issues),
            "issues": [i.to_obj() for i in exc.issues]}
    store.put_bytes(uri, canonical_json(body))
    return uri


def ingest_range(conn: psycopg.Connection, store: RawStore, reader: ReplayReader, *, code_sha: str,
                 dependency_lock_sha: str, start_index: int = 0, end_index: int | None = None,
                 mode: str = "manual", fault: FaultHook | None = None) -> list[RunResult]:
    calendar = reader.calendar()
    end = len(calendar) if end_index is None else end_index
    results: list[RunResult] = []
    i = start_index
    while i < end:
        chunk = calendar[i:min(i + MAX_INTERVALS_PER_RUN, end)]
        r = ingest_interval_chunk(conn, store, reader, chunk, code_sha=code_sha,
                                  dependency_lock_sha=dependency_lock_sha, mode=mode, fault=fault)
        results.append(r)
        if r.status != "INGESTED":
            break
        i += len(chunk)
    return results
