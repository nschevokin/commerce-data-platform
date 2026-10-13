from __future__ import annotations

import enum
from dataclasses import dataclass

import psycopg

from commerce.contracts.manifest import ContractError
from commerce.ingestion.ledger import GenerationMovedError, translate_db_error
from commerce.release.contract import (
    SERVING_VIEWS,
    VIEW_ORDER,
    contract_problems,
    release_info_ddl,
    view_ddl,
)

FAULT_POINTS = ("before_ddl", "after_ddl", "before_commit", "after_commit")


class PublicationRefused(ContractError):
    pass


class InjectedFault(RuntimeError):
    pass


class Outcome(enum.Enum):
    PUBLISHED = "PUBLISHED"
    ALREADY_COMMITTED = "ALREADY_COMMITTED"


@dataclass(frozen=True)
class PublicationResult:
    outcome: Outcome
    release_id: str
    generation: int
    previous_release_id: str | None
    kind: str


def _fault(hook: str | None, point: str) -> None:
    if hook == point:
        raise InjectedFault(point)


def _existing_receipt(conn: psycopg.Connection, release_id, kind: str):
    if kind == "PUBLISH":
        return conn.execute(
            "SELECT generation, previous_release_id FROM control.publications "
            "WHERE release_id = %s AND kind = 'PUBLISH'", (release_id,)).fetchone()
    return None


def publish(conn: psycopg.Connection, *, release_id, run_id, generation: int, kind: str = "PUBLISH",
            operator: str | None = None, reason: str | None = None, incident: str | None = None,
            fault: str | None = None) -> PublicationResult:
    if fault is not None and fault not in FAULT_POINTS:
        raise ValueError(f"unknown fault point {fault!r}")
    if kind not in ("PUBLISH", "ROLLBACK"):
        raise ValueError(kind)
    if kind == "ROLLBACK" and not ((operator or "").strip() and (reason or "").strip()):
        raise PublicationRefused("a rollback needs an operator and a reason")
    try:
        result = _publish_tx(conn, release_id, run_id, generation, kind, operator, reason, incident, fault)
    except psycopg.Error as exc:
        raise translate_db_error(exc) from exc
    _fault(fault, "after_commit")
    return result


def _publish_tx(conn, release_id, run_id, generation, kind, operator, reason, incident, fault):
    with conn.transaction():
        current_release, current_gen = conn.execute(
            "SELECT current_release_id, current_generation FROM control.lock_for_publication(%s, %s)",
            (run_id, generation)).fetchone()
        receipt = _existing_receipt(conn, release_id, kind)
        if receipt is not None:
            return PublicationResult(Outcome.ALREADY_COMMITTED, str(release_id), receipt[0],
                                     None if receipt[1] is None else str(receipt[1]), kind)
        row = conn.execute(
            "SELECT schema_name, status, blocking_failures, replay_version, frontier_end, code_sha "
            "FROM control.releases WHERE release_id = %s", (release_id,)).fetchone()
        if row is None:
            raise PublicationRefused(f"unknown release {release_id}")
        schema, status, blocking, replay_version, frontier_end, code_sha = row
        if status != "VALIDATED" or blocking != 0:
            raise PublicationRefused(f"release {release_id} is {status} with {blocking} blocking "
                                     "failures: only a VALIDATED release without blocking failures "
                                     "can be published")
        if current_release is not None and str(current_release) == str(release_id):
            raise PublicationRefused("release is already the current one")
        if kind == "PUBLISH" and current_release is not None:
            cur_frontier, = conn.execute("SELECT frontier_end FROM control.releases WHERE release_id = %s",
                                         (current_release,)).fetchone()
            if frontier_end < cur_frontier:
                raise PublicationRefused(
                    f"frontier regression: candidate {frontier_end} is older than serving {cur_frontier}; "
                    "use a rollback with operator and reason to go back")
        problems = contract_problems(conn, schema)
        if problems:
            raise PublicationRefused("serving contract violated at publication: " + "; ".join(problems))
        _fault(fault, "before_ddl")
        new_gen = conn.execute(
            "SELECT control.record_publication(%s, %s, %s, %s, %s, %s, %s, %s)",
            (run_id, generation, release_id, current_gen, kind, operator, reason, incident)).fetchone()[0]
        published_at = conn.execute("SELECT now()").fetchone()[0]
        conn.execute(release_info_ddl(release_id=release_id, status=kind if kind == "ROLLBACK" else "PUBLISHED",
                                      replay_version=replay_version, frontier_end=frontier_end,
                                      code_sha=code_sha, published_at=published_at, generation=new_gen))
        for view in VIEW_ORDER:
            conn.execute(view_ddl(schema, view))
        _fault(fault, "after_ddl")
        _fault(fault, "before_commit")
        return PublicationResult(Outcome.PUBLISHED, str(release_id), new_gen,
                                 None if current_release is None else str(current_release), kind)


def previous_validated_release(conn: psycopg.Connection):
    row = conn.execute(
        "SELECT p.release_id FROM control.publications p JOIN control.releases r USING (release_id) "
        "WHERE r.status = 'VALIDATED' AND p.release_id <> (SELECT release_id FROM control.current_release) "
        "ORDER BY p.generation DESC LIMIT 1").fetchone()
    return None if row is None else row[0]


__all__ = ["FAULT_POINTS", "GenerationMovedError", "InjectedFault", "Outcome", "PublicationRefused",
           "PublicationResult", "SERVING_VIEWS", "previous_validated_release", "publish"]
