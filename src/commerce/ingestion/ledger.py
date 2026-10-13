from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import psycopg

from commerce.contracts.manifest import ContractError, ManifestSource
from commerce.contracts.payloads import PARSER_CONTRACT_VERSION
from commerce.contracts.sources import SPEC_BY_NAME
from commerce.ingestion.validate import RowIssue, validate_file
from commerce.replay.events import SourceEvent

STALE_SQLSTATE = "CG002"
HELD_SQLSTATE = "CG001"
MOVED_SQLSTATE = "CG003"
MAX_ISSUES = 200

FaultHook = Callable[[str], None]


class FileRejected(ContractError):

    def __init__(self, source: str, file_sha256: str, issues: list[RowIssue], total_issues: int):
        super().__init__(f"{source} file {file_sha256[:12]} rejected: {total_issues} issue(s); "
                         f"first: {issues[0].reason if issues else '?'}")
        self.source, self.file_sha256, self.issues, self.total_issues = (
            source, file_sha256, issues, total_issues)


class StaleWriterError(RuntimeError):
    pass


class GuardHeldError(RuntimeError):
    pass


class GenerationMovedError(RuntimeError):
    pass


class ConcurrentWriteError(RuntimeError):
    pass


@dataclass(frozen=True)
class FileOutcome:
    source: str
    file_sha256: str
    already_committed: bool
    input_rows: int
    accepted_new_rows: int
    exact_duplicate_rows: int


def translate_db_error(exc: psycopg.Error) -> Exception:
    state = getattr(getattr(exc, "diag", None), "sqlstate", None)
    if state == STALE_SQLSTATE:
        return StaleWriterError(str(exc).strip())
    if state == HELD_SQLSTATE:
        return GuardHeldError(str(exc).strip())
    if state == MOVED_SQLSTATE:
        return GenerationMovedError(str(exc).strip())
    return exc


def _artefact_issues(issues: list[RowIssue]) -> list[RowIssue]:
    return issues[:MAX_ISSUES]


def ingest_file(conn: psycopg.Connection, *, run_id, generation: int, replay_version: str,
                interval_start: datetime, interval_end: datetime, manifest_sha256: str,
                entry: ManifestSource, events: list[SourceEvent],
                fault: FaultHook | None = None) -> FileOutcome:
    spec = SPEC_BY_NAME[entry.name]
    hook = fault or (lambda _point: None)
    try:
        with conn.transaction():
            outcome = _ingest_in_transaction(conn, spec, run_id, generation, replay_version,
                                             interval_start, interval_end, manifest_sha256, entry,
                                             events, hook)
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc
    hook("after_commit")
    return outcome


def _ingest_in_transaction(conn, spec, run_id, generation, replay_version, interval_start,
                           interval_end, manifest_sha256, entry, events, hook) -> FileOutcome:
    cur = conn.cursor()
    cur.execute("SELECT control.assert_writer(%s, %s)", (run_id, generation))

    row = cur.execute(
        "SELECT manifest_sha256, input_rows, accepted_new_rows, exact_duplicate_rows "
        "FROM control.file_receipts WHERE replay_version = %s AND source_name = %s "
        "AND interval_start = %s AND file_sha256 = %s",
        (replay_version, spec.name, interval_start, entry.sha256)).fetchone()
    if row is not None:
        if row[0] != manifest_sha256:
            raise ContractError(f"{spec.name} file {entry.sha256[:12]} already has a receipt for a "
                                "different manifest")
        return FileOutcome(spec.name, entry.sha256, True, row[1], row[2], row[3])

    v = validate_file(spec, events, interval_end=interval_end)
    if v.issues:
        raise FileRejected(spec.name, entry.sha256, _artefact_issues(v.issues), len(v.issues))

    cur.execute("CREATE TEMP TABLE stg_events (entity_key text, revision integer, available_at "
                "timestamptz, payload jsonb, payload_sha256 text) ON COMMIT DROP")
    with cur.copy("COPY stg_events (entity_key, revision, available_at, payload, payload_sha256) "
                  "FROM STDIN") as cp:
        for e in v.events:
            cp.write_row((e.entity_key, e.revision, e.available_at,
                          json.dumps(e.payload, sort_keys=True, ensure_ascii=False), e.payload_sha256))

    key = "b.replay_version = %s AND b.source_name = %s AND b.entity_key = s.entity_key " \
          "AND b.revision = s.revision"
    conflicts = cur.execute(
        f"SELECT s.entity_key, s.revision, b.payload_sha256, s.payload_sha256, b.available_at, "
        f"s.available_at FROM stg_events s JOIN bronze.source_events b ON {key} "
        "WHERE b.payload_sha256 <> s.payload_sha256 OR b.available_at <> s.available_at "
        "ORDER BY 1, 2 LIMIT %s", (replay_version, spec.name, MAX_ISSUES + 1)).fetchall()
    if conflicts:
        issues = [RowIssue(0, k, f"revision {rev} already in bronze with different content "
                                 f"(stored hash {old[:12]} at {oat}; file hash {new[:12]} at {nat})")
                  for k, rev, old, new, oat, nat in conflicts]
        raise FileRejected(spec.name, entry.sha256, issues[:MAX_ISSUES], len(conflicts))
    early = cur.execute(
        f"SELECT s.entity_key, s.revision, s.available_at FROM stg_events s "
        f"LEFT JOIN bronze.source_events b ON {key} "
        "WHERE s.available_at < %s AND b.entity_key IS NULL ORDER BY 1, 2 LIMIT %s",
        (replay_version, spec.name, interval_start, MAX_ISSUES + 1)).fetchall()
    if early:
        issues = [RowIssue(0, k, f"revision {rev} has available_at {at} before the interval start "
                                 "and is not an exact redelivery of a stored event (D-005)")
                  for k, rev, at in early]
        raise FileRejected(spec.name, entry.sha256, issues[:MAX_ISSUES], len(early))

    existing = cur.execute(
        f"SELECT count(*) FROM stg_events s JOIN bronze.source_events b ON {key}",
        (replay_version, spec.name)).fetchone()[0]
    expected_new = len(v.events) - existing
    cur.execute(
        "INSERT INTO bronze.source_events (replay_version, source_name, entity_key, revision, "
        "available_at, operation, payload, payload_sha256, parser_contract_version, "
        "source_file_sha256, first_run_id) "
        "SELECT %s, %s, entity_key, revision, available_at, 'UPSERT', payload, payload_sha256, %s, %s, %s "
        "FROM stg_events ON CONFLICT (replay_version, source_name, entity_key, revision) DO NOTHING",
        (replay_version, spec.name, PARSER_CONTRACT_VERSION, entry.sha256, run_id))
    inserted = cur.rowcount
    if inserted != expected_new:
        raise ConcurrentWriteError(
            f"{spec.name}: expected to insert {expected_new} events but inserted {inserted}; "
            "a concurrent writer touched the same keys")
    duplicates = existing + v.within_file_duplicates
    hook("before_receipt")
    cur.execute(
        "INSERT INTO control.file_receipts (replay_version, source_name, file_sha256, interval_start, "
        "manifest_sha256, input_rows, accepted_new_rows, exact_duplicate_rows, run_id, "
        "fencing_generation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (replay_version, spec.name, entry.sha256, interval_start, manifest_sha256, v.input_rows,
         inserted, duplicates, run_id, generation))
    hook("before_commit")
    return FileOutcome(spec.name, entry.sha256, False, v.input_rows, inserted, duplicates)


def complete_interval(conn: psycopg.Connection, *, run_id, generation: int, replay_version: str,
                      interval_start: datetime, interval_end: datetime, manifest_sha256: str,
                      entries: tuple[ManifestSource, ...], total_rows: int) -> None:
    try:
        with conn.transaction():
            cur = conn.cursor()
            cur.execute("SELECT control.assert_writer(%s, %s)", (run_id, generation))
            have = {(n, s) for n, s in cur.execute(
                "SELECT source_name, file_sha256 FROM control.file_receipts WHERE replay_version = %s "
                "AND manifest_sha256 = %s AND interval_start = %s",
                (replay_version, manifest_sha256, interval_start)).fetchall()}
            missing = sorted(e.name for e in entries if (e.name, e.sha256) not in have)
            if missing:
                raise ContractError(f"cannot complete interval {interval_start:%Y-%m-%d}: no receipt "
                                    f"for {missing}")
            prior = cur.execute(
                "SELECT manifest_sha256 FROM control.intervals WHERE replay_version = %s "
                "AND interval_start = %s", (replay_version, interval_start)).fetchone()
            if prior is not None and prior[0] != manifest_sha256:
                raise ContractError("interval already completed under a different manifest")
            cur.execute(
                "INSERT INTO control.intervals (replay_version, interval_start, interval_end, "
                "manifest_sha256, completeness, total_rows, run_id) "
                "VALUES (%s, %s, %s, %s, 'COMPLETE', %s, %s) "
                "ON CONFLICT (replay_version, interval_start) DO UPDATE SET updated_at = now(), "
                "run_id = EXCLUDED.run_id",
                (replay_version, interval_start, interval_end, manifest_sha256, total_rows, run_id))
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc
