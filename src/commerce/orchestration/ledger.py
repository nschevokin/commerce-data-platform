from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

from commerce.contracts.manifest import ContractError
from commerce.ingestion.ledger import translate_db_error
from commerce.orchestration.errors import error_code
from commerce.orchestration.wiring import Wiring

RUN_NAMESPACE = uuid.UUID("6f1d0c0e-5a43-4c64-9d0f-3c7a2f2b9a11")


def run_id_for(replay_version: str, dag_run_id: str) -> uuid.UUID:
    return uuid.uuid5(RUN_NAMESPACE, f"{replay_version}:{dag_run_id}")


@contextmanager
def tracked(w: Wiring, run_id, stage: str, source: str = "",
            role: str = "commerce_transform") -> Iterator[None]:
    with w.connect(role) as conn:
        attempt = conn.execute(
            "INSERT INTO control.stage_results (run_id, stage, source_name, attempt, status) "
            "SELECT %s, %s, %s, coalesce(max(attempt), 0) + 1, 'RUNNING' FROM control.stage_results "
            "WHERE run_id = %s AND stage = %s AND source_name = %s RETURNING attempt",
            (run_id, stage, source, run_id, stage, source)).fetchone()[0]
        try:
            yield
        except BaseException as exc:
            _finish(conn, run_id, stage, source, attempt, "FAILED", error_code(exc))
            raise
        _finish(conn, run_id, stage, source, attempt, "SUCCEEDED", None)


def mark_skipped(w: Wiring, run_id, stage: str, source: str = "") -> None:
    with w.connect("commerce_transform") as conn:
        conn.execute(
            "INSERT INTO control.stage_results (run_id, stage, source_name, attempt, status, finished_at) "
            "SELECT %s, %s, %s, coalesce(max(attempt), 0) + 1, 'SKIPPED', now() FROM control.stage_results "
            "WHERE run_id = %s AND stage = %s AND source_name = %s",
            (run_id, stage, source, run_id, stage, source))


def _finish(conn, run_id, stage, source, attempt, status, code) -> None:
    conn.execute(
        "UPDATE control.stage_results SET status = %s, error_code = %s, finished_at = now() "
        "WHERE run_id = %s AND stage = %s AND source_name = %s AND attempt = %s",
        (status, code, run_id, stage, source, attempt))


def latest_stages(conn: psycopg.Connection, run_id) -> dict[tuple[str, str], tuple[str, str | None]]:
    rows = conn.execute(
        "SELECT DISTINCT ON (stage, source_name) stage, source_name, status, error_code "
        "FROM control.stage_results WHERE run_id = %s ORDER BY stage, source_name, attempt DESC",
        (run_id,)).fetchall()
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


def set_run_status(w: Wiring, run_id, status: str, role: str = "commerce_transform") -> None:
    with w.connect(role) as conn:
        conn.execute("UPDATE control.runs SET status = %s, updated_at = now() WHERE run_id = %s",
                     (status, run_id))


def load_run(conn: psycopg.Connection, run_id) -> dict:
    row = conn.execute(
        "SELECT run_id, mode, replay_version, interval_start, interval_end, manifest_sha256s, status "
        "FROM control.runs WHERE run_id = %s", (run_id,)).fetchone()
    if row is None:
        raise ContractError(f"unknown run {run_id}: resolve_run has not completed")
    keys = ("run_id", "mode", "replay_version", "interval_start", "interval_end", "manifest_sha256s",
            "status")
    return dict(zip(keys, row, strict=True))


def ensure_writer(conn: psycopg.Connection, run_id) -> int:
    try:
        with conn.transaction():
            return conn.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc


def release_writer(conn: psycopg.Connection, run_id) -> bool:
    owner, gen = conn.execute("SELECT owner_run_id, fencing_generation FROM control.run_guard").fetchone()
    if owner != run_id:
        return False
    try:
        with conn.transaction():
            conn.execute("SELECT control.release_guard(%s, %s)", (run_id, gen))
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc
    return True
