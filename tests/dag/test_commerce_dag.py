from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("airflow.dag_processing.dagbag", reason="the locked Airflow is not installed here")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from replay_helpers import A, copy_fixture  # noqa: E402

from commerce.replay.dataset import DirSource  # noqa: E402
from commerce.replay.generator import generate_baseline  # noqa: E402
from commerce.replay.reader import ReplayReader  # noqa: E402
from commerce.replay.scenarios import build_scenario  # noqa: E402
from commerce.replay.store import RawStore  # noqa: E402

pytestmark = pytest.mark.airflow
DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "commerce_pipeline.py"
FAULT = "fault-malformed-money-v1"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(str(DAG_FILE.parent))
    assert bag.import_errors == {}
    return bag.get_dag("commerce_pipeline")


@pytest.fixture(scope="module")
def world(own_warehouse, tmp_path_factory):
    root = tmp_path_factory.mktemp("airflow")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault",
                   variant="malformed_money", order_ids=[A])
    return {"root": root, "ds": ds, "store": store, "admin": own_warehouse["admin_dsn"], "wh": own_warehouse}


@pytest.fixture(autouse=True)
def environment(monkeypatch, world):
    wh = world["wh"]
    values = {"COMMERCE_STORE": str(world["root"] / "store"), "COMMERCE_DATASET_SHA256": world["ds"],
              "COMMERCE_EXACT_FIXTURE": "1", "COMMERCE_WAREHOUSE_DB": wh["dbname"],
              "COMMERCE_WAREHOUSE_HOST": str(wh["host"]), "COMMERCE_WAREHOUSE_PORT": str(wh["port"]),
              "COMMERCE_ARTIFACTS": str(world["root"] / "builds")}
    for role in ("ingest", "transform", "publish", "dashboard"):
        values[f"COMMERCE_{role.upper()}_PASSWORD"] = f"test-only-commerce_{role}"
    for k, v in values.items():
        monkeypatch.setenv(k, v)


def conf(world, first, last, replay_version=None):
    cal = ReplayReader(world["store"], world["ds"], replay_version or "baseline-v1").calendar()
    out = {"interval_start": cal[first].start.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "interval_end": cal[last - 1].end.strftime("%Y-%m-%dT%H:%M:%SZ")}
    if replay_version:
        out["replay_version"] = replay_version
    return out


def states(dagrun):
    return {(ti.task_id, ti.map_index): str(ti.state) for ti in dagrun.get_task_instances()}


def q(world, sql, *params):
    with psycopg.connect(world["admin"]) as c:
        return c.execute(sql, params).fetchall()


def test_dag_shape_and_operating_settings(dag):
    ids = {t.task_id for t in dag.tasks}
    assert ids == {"resolve_run", "claim_writer", "wait_for_manifests", "resolve_manifest", "verify_manifest",
                   "ingest_source", "reconcile_ingestion", "choose_frontier", "prepare_candidate",
                   "build_candidate", "validate_candidate", "publish_release", "verify_serving",
                   "pipeline_result"}
    assert {t.task_id for t in dag.tasks if not t.downstream_list} == {"pipeline_result"}
    assert (dag.max_active_runs, dag.catchup, dag.is_paused_upon_creation) == (1, False, True)
    assert dag.tasks[0].retries == 2
    by_id = dag.task_dict
    assert by_id["ingest_source"].pool == "commerce_ingest"
    assert by_id["build_candidate"].pool == "commerce_transform"
    assert by_id["pipeline_result"].trigger_rule.value == "all_done" and by_id["pipeline_result"].retries == 0
    assert "CronDataIntervalTimetable" in type(dag.timetable).__name__


def test_dag_file_contains_no_business_logic():
    text = DAG_FILE.read_text()
    assert not re.search(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", text)
    assert "psycopg" not in text and "subprocess" not in text


def test_manual_run_publishes_through_the_real_dag(dag, world):
    run = dag.test(run_conf=conf(world, 0, 7))
    assert str(run.state) == "success", states(run)
    assert {s for s in states(run).values()} == {"success"}
    assert len([k for k in states(run) if k[0] == "ingest_source"]) == 7
    assert q(world, "SELECT generation FROM control.current_release") == [(1,)]
    assert q(world, "SELECT status, mode FROM control.runs") == [("SUCCEEDED", "manual")]
    assert q(world, "SELECT owner_run_id FROM control.run_guard") == [(None,)]


def test_a_failure_stays_a_failure_and_the_terminal_task_says_so(dag, world):
    bad = conf(world, 7, 14)
    bad["interval_end"] = bad["interval_end"].replace("T00:00:00Z", "T06:00:00Z")
    run = dag.test(run_conf=bad)
    st = states(run)
    assert str(run.state) == "failed", st
    assert st[("resolve_run", -1)] == "failed" and st[("pipeline_result", -1)] == "failed"
    assert st[("publish_release", -1)] == "upstream_failed"
    assert q(world, "SELECT generation FROM control.current_release") == [(1,)]


def test_rejected_source_data_fails_the_dag_run_and_frees_the_guard(dag, world):
    before = q(world, "SELECT release_id, generation FROM control.current_release")
    failing = None
    for first, last in [(0, 7), (7, 14), (14, 21)]:
        run = dag.test(run_conf=conf(world, first, last, FAULT))
        if str(run.state) != "success":
            failing = run
            break
    assert failing is not None, "the fault replay never failed"
    st = states(failing)
    assert st[("pipeline_result", -1)] == "failed" and st[("reconcile_ingestion", -1)] == "upstream_failed"
    assert any(s == "failed" for (task, _), s in st.items() if task == "ingest_source")
    assert q(world, "SELECT release_id, generation FROM control.current_release") == before
    assert q(world, "SELECT owner_run_id FROM control.run_guard") == [(None,)]
    assert ("FAILED_CONTRACT",) in q(world, "SELECT status FROM control.runs")


def test_nothing_in_the_dag_run_leaves_the_stage_ledger_inconsistent(dag, world):
    rows = q(world, "SELECT count(*) FROM control.stage_results WHERE status = 'RUNNING'")
    assert rows == [(0,)]
    assert q(world, "SELECT count(*) FROM control.runs") == [(2,)]
