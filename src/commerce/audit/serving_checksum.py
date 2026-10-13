from __future__ import annotations

import hashlib
import json

import psycopg

from commerce.serving.reader import read_snapshot

HEALTH_BUSINESS_COLUMNS = ("source_name", "latest_rows")


def _row_text(view: str, row: dict) -> str:
    cols = HEALTH_BUSINESS_COLUMNS if view == "pipeline_health" else tuple(row)
    return json.dumps([str(row[c]) for c in cols], separators=(",", ":"))


def serving_checksum(conn: psycopg.Connection) -> dict:
    snap = read_snapshot(conn)
    if snap.release is None or snap.release["release_id"] is None:
        return {"release_id": None, "sha256": None, "rows": {}}
    h = hashlib.sha256()
    rows = {}
    for view in sorted(snap.tables):
        lines = sorted(_row_text(view, r) for r in snap.tables[view])
        rows[view] = len(lines)
        h.update(f"{view}\n".encode())
        for line in lines:
            h.update(line.encode() + b"\n")
    rel = snap.release
    return {"release_id": str(rel["release_id"]), "frontier_end": rel["frontier_end"].isoformat(),
            "generation": rel["generation"], "code_sha": rel["code_sha"], "rows": rows,
            "sha256": h.hexdigest()}
