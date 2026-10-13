from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from commerce.orchestration import stages as S
from commerce.orchestration.wiring import Wiring

INGEST_POOL_SLOTS = 2


def run_pipeline(w: Wiring, *, dag_run_id: str, mode: str, interval_start: datetime,
                 interval_end: datetime, stop_after: str | None = None) -> dict:
    run_id = S.resolve_run(w, dag_run_id=dag_run_id, mode=mode, interval_start=interval_start,
                           interval_end=interval_end)
    error: BaseException | None = None
    try:
        S.claim_writer(w, run_id)
        if stop_after == "claim_writer":
            return {"run_id": run_id, "status": "STOPPED"}
        S.resolve_manifest(w, run_id)
        S.verify_manifest(w, run_id)
        with ThreadPoolExecutor(INGEST_POOL_SLOTS) as pool:
            for fut in [pool.submit(S.ingest_source, w, run_id, s) for s in S.SOURCES]:
                fut.result()
        if stop_after == "ingest_source":
            return {"run_id": run_id, "status": "STOPPED"}
        S.reconcile_ingestion(w, run_id)
        S.choose_frontier(w, run_id)
        S.prepare_candidate(w, run_id)
        S.build_candidate_stage(w, run_id)
        S.validate_candidate(w, run_id)
        if stop_after == "validate_candidate":
            return {"run_id": run_id, "status": "STOPPED"}
        S.publish_release(w, run_id)
        S.verify_serving(w, run_id)
    except Exception as exc:        # noqa: BLE001
        error = exc
    try:
        result = S.pipeline_result(w, uuid.UUID(run_id))
    except S.PipelineFailed as exc:
        return {"run_id": run_id, "status": _status(w, run_id), "error": str(exc), "cause": repr(error)}
    return result


def _status(w: Wiring, run_id: str) -> str:
    with w.connect("commerce_transform") as c:
        return c.execute("SELECT status FROM control.runs WHERE run_id = %s", (run_id,)).fetchone()[0]
