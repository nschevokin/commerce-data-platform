from __future__ import annotations

import json
import uuid
from datetime import datetime

import psycopg
from psycopg import sql

from commerce.contracts.manifest import ContractError
from commerce.contracts.payloads import PARSER_CONTRACT_VERSION
from commerce.ingestion.ledger import translate_db_error
from commerce.release.contract import SERVING_CONTRACT_VERSION, SERVING_VIEWS, VIEW_ORDER, safe_schema
from commerce.release.quality import BLOCK, CheckResult, counts


def register_release(conn: psycopg.Connection, *, release_id: uuid.UUID, schema: str, run_id, generation: int,
                     replay_version: str, frontier_end: datetime, input_digest: str, code_sha: str,
                     dependency_lock_sha: str, artifact_uri: str | None = None) -> None:
    safe_schema(schema)
    try:
        with conn.transaction():
            conn.execute("SELECT control.assert_writer(%s, %s)", (run_id, generation))
            conn.execute(
                "INSERT INTO control.releases (release_id, schema_name, run_id, replay_version, frontier_end, "
                "input_digest, code_sha, dependency_lock_sha, parser_contract_version, "
                "serving_contract_version, status, artifact_uri) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'BUILDING', %s)",
                (release_id, schema, run_id, replay_version, frontier_end, input_digest, code_sha,
                 dependency_lock_sha, PARSER_CONTRACT_VERSION, SERVING_CONTRACT_VERSION, artifact_uri))
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc


def set_status(conn: psycopg.Connection, release_id, run_id, generation: int, status: str) -> None:
    try:
        with conn.transaction():
            conn.execute("SELECT control.assert_writer(%s, %s)", (run_id, generation))
            conn.execute("UPDATE control.releases SET status = %s WHERE release_id = %s",
                         (status, release_id))
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc


def record_validation(conn: psycopg.Connection, *, release_id, schema: str, run_id, generation: int,
                      results: list[CheckResult]) -> str:
    ident = safe_schema(schema)
    blocking, warnings = counts(results)
    status = "VALIDATED" if blocking == 0 else "REJECTED"
    try:
        with conn.transaction():
            conn.execute("SELECT control.assert_writer(%s, %s)", (run_id, generation))
            for r in results:
                conn.execute(
                    "INSERT INTO control.quality_results (release_id, check_id, run_id, severity, passed, "
                    "observed, threshold, detail) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (release_id, r.check_id, run_id, r.severity, r.passed, r.observed, r.threshold,
                     json.dumps(r.detail, default=str)))
            if status == "VALIDATED":
                conn.execute(sql.SQL("UPDATE {}.mart_pipeline_health SET blocking_failures = %s, "
                                     "warnings = %s").format(ident), (blocking, warnings))
            conn.execute("UPDATE control.releases SET status = %s, blocking_failures = %s, warnings = %s "
                         "WHERE release_id = %s", (status, blocking, warnings, release_id))
            if status == "VALIDATED":
                conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO commerce_publish").format(ident))
                conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO commerce_publish")
                             .format(ident))
                conn.execute(sql.SQL("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {} "
                                     "FROM commerce_transform").format(ident))
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc
    return status


def release_row(conn: psycopg.Connection, release_id) -> dict:
    row = conn.execute(
        "SELECT release_id, schema_name, replay_version, frontier_end, status, blocking_failures, "
        "warnings, code_sha, input_digest FROM control.releases WHERE release_id = %s", (release_id,)
    ).fetchone()
    if row is None:
        raise ContractError(f"unknown release {release_id}")
    keys = ("release_id", "schema_name", "replay_version", "frontier_end", "status", "blocking_failures",
            "warnings", "code_sha", "input_digest")
    return dict(zip(keys, row, strict=True))


__all__ = ["BLOCK", "SERVING_VIEWS", "VIEW_ORDER", "record_validation", "register_release", "release_row",
           "set_status"]
