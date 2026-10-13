from __future__ import annotations

from datetime import datetime

from commerce.contracts.canonical import utc_iso
from commerce.orchestration.errors import RunConfigError
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.stages import MAX_INTERVALS_PER_RUN, SOURCES
from commerce.orchestration.wiring import Wiring
from commerce.replay.reader import IntervalIncomplete


def _window(w: Wiring, start: datetime, end: datetime):
    cal = w.reader().calendar()
    covered = [(i, iv) for i, iv in enumerate(cal) if start <= iv.start < end]
    if (not covered or covered[0][1].start != start or covered[-1][1].end != end):
        raise RunConfigError("start and end must be replay interval boundaries inside the calendar")
    return cal, covered


def plan_ranges(w: Wiring, start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    _, covered = _window(w, start, end)
    out = []
    for k in range(0, len(covered), MAX_INTERVALS_PER_RUN):
        chunk = covered[k:k + MAX_INTERVALS_PER_RUN]
        out.append((chunk[0][1].start, chunk[-1][1].end))
    return out


def inventory(w: Wiring, start: datetime, end: datetime) -> dict:
    cal, covered = _window(w, start, end)
    reader = w.reader()
    with w.connect("commerce_transform") as conn:
        done = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT interval_start, manifest_sha256, completeness FROM control.intervals "
            "WHERE replay_version = %s", (w.replay_version,)).fetchall()}
        receipts = {(r[0], r[1]) for r in conn.execute(
            "SELECT interval_start, source_name FROM control.file_receipts WHERE replay_version = %s",
            (w.replay_version,)).fetchall()}
    rows, total_bytes, gaps = [], 0, []
    for _, iv in covered:
        try:
            m = reader.manifest(iv.start)
        except IntervalIncomplete:
            rows.append({"interval": iv.label, "start": utc_iso(iv.start), "manifest": False})
            gaps.append(iv.label)
            continue
        size = sum(s.size_bytes for s in m.sources)
        total_bytes += size
        rows.append({"interval": iv.label, "start": utc_iso(iv.start), "manifest": True, "bytes": size,
                     "rows": m.total_rows, "files": len(m.sources),
                     "files_with_receipt": sum((iv.start, s.name) in receipts for s in m.sources),
                     "complete": done.get(iv.start, (None, None))[1] == "COMPLETE"})
    before = [iv.label for iv in cal if iv.start < start and done.get(iv.start, (None, None))[1] != "COMPLETE"]
    return {"replay_version": w.replay_version, "start": utc_iso(start), "end": utc_iso(end),
            "ranges": len(plan_ranges(w, start, end)), "total_bytes": total_bytes,
            "sources": list(SOURCES), "missing_manifests": gaps,
            "incomplete_before_window": before, "intervals": rows}


def run_backfill(w: Wiring, start: datetime, end: datetime, *, mode: str = "publish",
                 tag: str = "backfill") -> list[dict]:
    if mode not in ("publish", "audit"):
        raise RunConfigError("mode must be publish or audit")
    run_mode = "backfill" if mode == "publish" else "audit"
    results = []
    for a, b in plan_ranges(w, start, end):
        out = run_pipeline(w, dag_run_id=f"{tag}-{run_mode}-{utc_iso(a)}-{utc_iso(b)}", mode=run_mode,
                           interval_start=a, interval_end=b)
        results.append({"start": utc_iso(a), "end": utc_iso(b), **out})
        if out["status"] not in ("SUCCEEDED", "NO_CHANGE"):
            break
    return results
