from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg import sql

from commerce.release.contract import safe_schema


class RetentionRefused(RuntimeError):
    pass


@dataclass(frozen=True)
class Decision:
    release_id: str
    schema: str
    status: str
    keep: bool
    reason: str


def plan(rows: list[dict], *, current: str | None, published_order: list[str], keep_published: int,
         failed_cutoff: datetime, holds: set[str]) -> list[Decision]:
    keep_ids = set(published_order[:keep_published + 1])
    out = []
    for r in rows:
        rid, status = str(r["release_id"]), r["status"]
        if status == "DROPPED":
            continue
        if rid == current:
            d = (True, "current release")
        elif rid in keep_ids:
            d = (True, f"one of the previous {keep_published} published releases")
        elif rid in holds:
            d = (True, "incident hold")
        elif status in ("BUILDING", "BUILT"):
            d = (True, "build in progress or awaiting validation")
        elif rid not in published_order and r["created_at"] >= failed_cutoff:
            d = (True, "never published, inside the failed-candidate retention window")
        else:
            d = (False, "published long ago" if rid in published_order else "never published, window passed")
        out.append(Decision(rid, r["schema_name"], status, d[0], d[1]))
    return out


def collect(conn: psycopg.Connection, *, keep_published: int = 2, failed_days: int = 7,
            holds: set[str] | None = None, now: datetime | None = None) -> list[Decision]:
    rows = [dict(zip(("release_id", "schema_name", "status", "created_at"), r, strict=True)) for r in
            conn.execute("SELECT release_id, schema_name, status, created_at FROM control.releases "
                         "ORDER BY created_at").fetchall()]
    current = conn.execute("SELECT release_id::text FROM control.current_release").fetchone()
    published = [str(r[0]) for r in conn.execute(
        "SELECT release_id FROM control.publications WHERE kind = 'PUBLISH' ORDER BY generation DESC")]
    cutoff = (now or datetime.now(UTC)) - timedelta(days=failed_days)
    return plan(rows, current=current[0] if current else None, published_order=published,
                keep_published=keep_published, failed_cutoff=cutoff, holds=holds or set())


def run_gc(admin_dsn: str, *, keep_published: int = 2, failed_days: int = 7, holds: set[str] | None = None,
           dry_run: bool = True, lock_timeout_ms: int = 5000) -> dict:
    with psycopg.connect(admin_dsn) as conn:
        with conn.transaction():
            guard = conn.execute("SELECT owner_run_id FROM control.run_guard WHERE singleton_id = 1 "
                                 "FOR UPDATE").fetchone()
            if guard and guard[0] is not None:
                raise RetentionRefused(f"run {guard[0]} owns the writer guard; retry when the pipeline is idle")
            decisions = collect(conn, keep_published=keep_published, failed_days=failed_days, holds=holds)
            drops = [d for d in decisions if not d.keep]
            dropped = []
            if not dry_run:
                conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(f"{lock_timeout_ms}ms")))
                for d in drops:
                    safe_schema(d.schema)
                    conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(d.schema)))
                    conn.execute("UPDATE control.releases SET status = 'DROPPED' WHERE release_id = %s::uuid",
                                 (d.release_id,))
                    dropped.append(d.schema)
    return {"dry_run": dry_run, "kept": [d.__dict__ for d in decisions if d.keep],
            "to_drop": [d.__dict__ for d in drops], "dropped": dropped}
