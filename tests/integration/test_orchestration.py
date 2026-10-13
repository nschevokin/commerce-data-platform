from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from replay_helpers import A, copy_fixture

from commerce.fixture.harness import diff, observe_candidate
from commerce.ingestion.ledger import (
    ConcurrentWriteError,
    GenerationMovedError,
    GuardHeldError,
    StaleWriterError,
)
from commerce.orchestration import stages as S
from commerce.orchestration.errors import (
    DataInvalid,
    PipelineFailed,
    RunConfigError,
    error_code,
    is_retryable,
)
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.window import run_window
from commerce.orchestration.wiring import Wiring
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]

EXPECTED = json.loads((Path(__file__).resolve().parents[1] / "fixtures" / "golden_v1" / "expected.json")
                      .read_text())["baseline"]
FAULT = "fault-malformed-money-v1"


@pytest.fixture(scope="module")
def world(own_warehouse, own_role_dsn, tmp_path_factory):
    root = tmp_path_factory.mktemp("orchestration")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault",
                   variant="malformed_money", order_ids=[A])
    env = {"COMMERCE_WAREHOUSE_HOST": str(own_warehouse["host"]),
           "COMMERCE_WAREHOUSE_PORT": str(own_warehouse["port"]),
           "COMMERCE_WAREHOUSE_DB": own_warehouse["dbname"],
           "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
    w = Wiring(store_root=str(root / "store"), dataset_sha256=ds, replay_version="baseline-v1",
               artifacts_root=root / "builds", dsn_for=own_role_dsn, dbt_env=env, exact_fixture=True)
    return {"w": w, "admin": own_warehouse["admin_dsn"], "dsn": own_role_dsn}


def window(w, first, last):
    cal = w.reader().calendar()
    return cal[first].start, cal[last - 1].end


def go(w, name, first, last, **kw):
    start, end = window(w, first, last)
    return run_pipeline(w, dag_run_id=name, mode="manual", interval_start=start, interval_end=end, **kw)


def q(world, sql, *params):
    with psycopg.connect(world["admin"]) as c:
        return c.execute(sql, params).fetchall()


def serving(world):
    rel, gen = q(world, "SELECT release_id, generation FROM control.current_release")[0]
    return (None if rel is None else str(rel)), gen


def guard_owner(world):
    return q(world, "SELECT owner_run_id FROM control.run_guard")[0][0]


def test_scheduled_and_manual_windows_are_the_same_function_of_the_same_interval():
    a = datetime(2026, 1, 2, tzinfo=UTC)
    b = datetime(2026, 1, 3, tzinfo=UTC)
    sched = run_window(run_type="scheduled", data_interval_start=a, data_interval_end=b)
    manual = run_window(run_type="manual", interval_start="2026-01-02T00:00:00Z",
                        interval_end="2026-01-03T00:00:00Z")
    assert sched[1:] == manual[1:] == (a, b) and (sched[0], manual[0]) == ("scheduled", "manual")


@pytest.mark.parametrize("kwargs", [
    {"run_type": "manual"},
    {"run_type": "manual", "interval_start": "2026-01-02T00:00:00Z"},
    {"run_type": "manual", "interval_start": "2026-01-02 00:00:00", "interval_end": "2026-01-03T00:00:00Z"},
    {"run_type": "manual", "interval_start": "2026-01-03T00:00:00Z", "interval_end": "2026-01-02T00:00:00Z"},
    {"run_type": "manual", "interval_start": "2026-01-02T00:00:00+02:00",
     "interval_end": "2026-01-03T00:00:00Z"},
    {"run_type": "backfill"}, {"run_type": "scheduled"},
])
def test_bad_windows_are_rejected(kwargs):
    with pytest.raises(RunConfigError):
        run_window(**kwargs)


def test_retry_classification():
    assert is_retryable(psycopg.OperationalError("connection reset"))
    assert is_retryable(psycopg.errors.LockNotAvailable("lock timeout"))
    assert is_retryable(GuardHeldError("held")) and is_retryable(ConcurrentWriteError("race"))
    for exc in (DataInvalid("bad"), StaleWriterError("stale"), GenerationMovedError("moved"),
                RunConfigError("x"), ValueError("x")):
        assert not is_retryable(exc), exc
    assert error_code(DataInvalid("x")) == "DATA_INVALID" and error_code(GuardHeldError("x")) == "GUARD_HELD"


def test_window_must_align_with_the_replay_calendar_and_fit_seven_intervals(world):
    w = world["w"]
    cal = w.reader().calendar()
    with pytest.raises(RunConfigError, match="boundaries"):
        S.resolve_run(w, dag_run_id="bad-1", mode="manual", interval_start=cal[0].start,
                      interval_end=cal[0].end.replace(hour=12))
    with pytest.raises(RunConfigError, match="at most 7"):
        S.resolve_run(w, dag_run_id="bad-2", mode="manual", interval_start=cal[0].start,
                      interval_end=cal[7].end)
    assert q(world, "SELECT count(*) FROM control.runs")[0][0] == 0


def test_killed_run_holds_the_guard_a_competitor_is_refused_and_the_owner_resumes(world):
    w = world["w"]
    first = go(w, "run-1", 0, 7, stop_after="claim_writer")
    assert first["status"] == "STOPPED"
    owner = guard_owner(world)
    assert str(owner) == first["run_id"]

    rival = go(w, "run-1-rival", 0, 7)
    assert rival["status"] == "FAILED_TRANSIENT"
    assert "claim_writer: FAILED GUARD_HELD" in rival["error"]
    assert guard_owner(world) == owner
    assert q(world, "SELECT count(*) FROM bronze.source_events")[0][0] == 0

    resumed = go(w, "run-1", 0, 7)
    assert resumed["status"] == "SUCCEEDED", resumed
    assert guard_owner(world) is None
    assert serving(world)[1] == 1
    attempts = q(world, "SELECT attempt FROM control.stage_results WHERE run_id = %s "
                        "AND stage = 'claim_writer' ORDER BY attempt", first["run_id"])
    assert [a[0] for a in attempts] == [1, 2]


def test_run_killed_before_publication_resumes_without_a_second_build(world):
    w = world["w"]
    first = go(w, "run-2", 7, 14, stop_after="validate_candidate")
    assert first["status"] == "STOPPED" and serving(world)[1] == 1
    (rel_before,) = q(world, "SELECT release_id FROM control.releases WHERE run_id = %s "
                             "AND status = 'VALIDATED'", first["run_id"])
    resumed = go(w, "run-2", 7, 14)
    assert resumed["status"] == "SUCCEEDED", resumed
    assert serving(world) == (str(rel_before[0]), 2)
    assert q(world, "SELECT count(*) FROM control.releases WHERE run_id = %s", first["run_id"])[0][0] == 1
    assert guard_owner(world) is None


def test_serial_ranges_reach_the_golden_answer(world):
    w = world["w"]
    for n, (a, b) in enumerate([(14, 21), (21, 28), (28, 31)], start=3):
        out = go(w, f"run-{n}", a, b)
        assert out["status"] == "SUCCEEDED", out
    rel, gen = serving(world)
    assert gen == 5
    (schema,) = q(world, "SELECT schema_name FROM control.releases WHERE release_id = %s", rel)[0]
    assert diff(EXPECTED, observe_candidate(world["admin"], schema)) == []
    statuses = q(world, "SELECT status, count(*) FROM control.runs GROUP BY 1")
    assert dict(statuses).get("SUCCEEDED") == 5


def test_rerunning_known_work_changes_nothing_and_says_so(world):
    w = world["w"]
    before = serving(world), q(world, "SELECT count(*) FROM control.releases")[0][0]
    again = go(w, "run-final-again", 28, 31)
    assert again["status"] == "NO_CHANGE", again
    old = go(w, "run-old-window", 0, 7)
    assert old["status"] == "NO_CHANGE", old
    assert (serving(world), q(world, "SELECT count(*) FROM control.releases")[0][0]) == before
    skipped = q(world, "SELECT status FROM control.stage_results WHERE run_id = %s "
                       "AND stage = 'publish_release'", again["run_id"])
    assert skipped == [("SKIPPED",)]
    assert guard_owner(world) is None


def test_ingest_retry_is_idempotent_and_counted_as_a_new_attempt(world):
    w = world["w"]
    run = go(w, "run-final-again", 28, 31)
    rows = q(world, "SELECT attempt, status FROM control.stage_results WHERE run_id = %s AND stage = "
                    "'ingest_source' AND source_name = 'orders' ORDER BY attempt", run["run_id"])
    assert len(rows) >= 2 and all(s == "SUCCEEDED" for _, s in rows)
    receipts_before = q(world, "SELECT count(*) FROM control.file_receipts")[0][0]
    out = S.ingest_source(w, run["run_id"], "orders")
    assert out["accepted_new_rows"] == 0 and out["already_committed"] == out["files"] > 0
    assert q(world, "SELECT count(*) FROM control.file_receipts")[0][0] == receipts_before
    S.pipeline_result(w, run["run_id"])
    assert guard_owner(world) is None


def test_rejected_data_fails_the_run_keeps_serving_and_frees_the_guard(world):
    w = replace(world["w"], replay_version=FAULT)
    before = serving(world)
    results = []
    for n, (a, b) in enumerate([(0, 7), (7, 14), (14, 21)]):
        out = go(w, f"fault-{n}", a, b)
        results.append(out["status"])
        if out["status"] != "SUCCEEDED":
            break
    assert "FAILED_CONTRACT" in results, results
    failed = out
    assert "ingest_source[order_items]" in failed["error"] and "DATA_INVALID" in failed["error"]
    assert serving(world) == before
    assert guard_owner(world) is None
    (status,) = q(world, "SELECT status FROM control.runs WHERE run_id = %s", failed["run_id"])[0]
    assert status == "FAILED_CONTRACT"
    report = next(Path(w.store_root).glob(f"validation/**/run={failed['run_id']}/**/*.json"))
    assert json.loads(report.read_text())["source"] == "order_items"


def test_terminal_stage_refuses_to_report_success_on_missing_evidence(world):
    w = world["w"]
    start, end = window(w, 0, 1)
    run_id = S.resolve_run(w, dag_run_id="half-done", mode="manual", interval_start=start, interval_end=end)
    with pytest.raises(PipelineFailed, match="claim_writer: no evidence"):
        S.pipeline_result(w, run_id)
    assert q(world, "SELECT status FROM control.runs WHERE run_id = %s", run_id) != [("SUCCEEDED",)]
    assert uuid.UUID(run_id)
