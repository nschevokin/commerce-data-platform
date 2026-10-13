from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg

from commerce.release.contract import RELEASE_INFO_COLUMNS, SERVING_VIEWS

DATA_VIEWS = tuple(SERVING_VIEWS)


@dataclass
class Snapshot:
    release: dict[str, Any] | None
    tables: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def read_snapshot(conn: psycopg.Connection, views: tuple[str, ...] = DATA_VIEWS,
                  between: Callable[[], None] | None = None) -> Snapshot:
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.rollback()
    conn.read_only = True
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    try:
        with conn.transaction():
            conn.execute("LOCK TABLE serving.release_info IN ACCESS SHARE MODE")
            cur = conn.execute("SELECT * FROM serving.release_info")
            cols = [d.name for d in cur.description]
            assert tuple(cols) == RELEASE_INFO_COLUMNS, cols
            row = cur.fetchone()
            release = dict(zip(cols, row, strict=True)) if row else None
            snap = Snapshot(release)
            if release is None or release["release_id"] is None:
                return snap
            for i, view in enumerate(views):
                cur = conn.execute(f"SELECT * FROM serving.{view}")  # noqa: S608
                names = [d.name for d in cur.description]
                snap.tables[view] = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
                if i == 0 and between is not None:
                    between()
            return snap
    finally:
        conn.read_only = False
        conn.isolation_level = None
