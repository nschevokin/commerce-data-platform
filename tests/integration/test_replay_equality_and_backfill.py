from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
from conftest import _provision
from replay_helpers import A, B, copy_fixture

from commerce import runinfo
from commerce.audit.equality import canonical_report, compare, differing_rows
from commerce.ingestion.runner import ingest_range
from commerce.orchestration.backfill import inventory, plan_ranges, run_backfill
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]

WINDOWS = [(0, 7), (7, 14), (14, 21), (21, 28), (28, 31)]
FINAL = (24, 31)
STATE: dict = {}


@pytest.fixture(scope="module")
def daily_db(own_warehouse):
    return own_warehouse


@pytest.fixture(scope="module")
def clean_db(server_dsn):
    yield from _provision(server_dsn)


@pytest.fixture(scope="module")
def world(daily_db, clean_db, own_role_dsn, tmp_path_factory):
    root = tmp_path_factory.mktemp("equality")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    for kind, ids in (("duplicate", [A]), ("late", [B]), ("correction", [B])):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind=kind, order_ids=ids)
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="duplicate", order_ids=[A],
                   new_version="gap-v1")

    def wiring(db, role_dsn, version):
        env = {"COMMERCE_WAREHOUSE_HOST": str(db["host"]), "COMMERCE_WAREHOUSE_PORT": str(db["port"]),
               "COMMERCE_WAREHOUSE_DB": db["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
               "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
        return Wiring(store_root=str(root / "store"), dataset_sha256=ds, replay_version=version,
                      artifacts_root=root / "builds", dsn_for=role_dsn, dbt_env=env, exact_fixture=True)

    import psycopg.conninfo as ci

    def clean_dsn(role):
        return ci.make_conninfo(host=clean_db["host"], port=clean_db["port"], dbname=clean_db["dbname"],
                                user=role, password=f"test-only-{role}")
    return {"ds": ds, "store": store, "root": root, "daily": daily_db, "clean": clean_db,
            "w1": lambda v="baseline-v1": wiring(daily_db, own_role_dsn, v),
            "w2": lambda v="baseline-v1": wiring(clean_db, clean_dsn, v)}


def go(w, name, first, last, mode="manual"):
    cal = w.reader().calendar()
    return run_pipeline(w, dag_run_id=name, mode=mode, interval_start=cal[first].start,
                        interval_end=cal[last - 1].end)


def q(db, sql, *params):
    with psycopg.connect(db["admin_dsn"]) as c:
        return c.execute(sql, params).fetchall()


def serving_report(db):
    schema = q(db, "SELECT schema_name FROM control.releases r JOIN control.current_release c "
                   "USING (release_id)")[0][0]
    with psycopg.connect(db["admin_dsn"]) as c:
        return schema, canonical_report(c, schema)


def serving_generation(db):
    return q(db, "SELECT generation FROM control.current_release")[0][0]


def test_a_gap_before_the_requested_interval_does_not_advance_the_frontier(world):
    w = world["w2"]("gap-v1")
    out = go(w, "gap-late-window", 14, 21)
    assert out["status"] == "INPUT_NOT_READY", out
    assert "choose_frontier: FAILED FRONTIER_NOT_READY" in out["error"]
    assert serving_generation(world["clean"]) == 0
    assert q(world["clean"], "SELECT count(*) FROM control.intervals "
                             "WHERE completeness = 'COMPLETE'")[0][0] == 7
    receipts = q(world["clean"], "SELECT count(*) FROM control.file_receipts")[0][0]

    assert go(w, "gap-w1", 0, 7)["status"] == "SUCCEEDED"
    assert go(w, "gap-w2", 7, 14)["status"] == "SUCCEEDED"
    assert go(w, "gap-w3", 14, 21)["status"] == "SUCCEEDED"
    assert serving_generation(world["clean"]) == 3
    new_receipts = q(world["clean"], "SELECT count(*) FROM control.file_receipts")[0][0]
    assert new_receipts == receipts + 14 * 7


def test_sequential_runs_equal_a_clean_full_replay(world):
    w1, w2 = world["w1"](), world["w2"]()
    for n, (a, b) in enumerate(WINDOWS):
        assert go(w1, f"daily-{n}", a, b)["status"] == "SUCCEEDED"
    with psycopg.connect(w2.dsn_for("commerce_ingest")) as c:
        for r in ingest_range(c, w2.store(), w2.reader(), code_sha="t", dependency_lock_sha="t"):
            assert r.status == "INGESTED", r
    assert go(w2, "clean-final", *FINAL)["status"] == "SUCCEEDED"

    s1, r1 = serving_report(world["daily"])
    s2, r2 = serving_report(world["clean"])
    assert s1 != s2 and r1["tables"]["fct_orders"]["rows"] == 3
    assert compare(r1, r2) == [], compare(r1, r2)
    assert r1["overall_sha256"] == r2["overall_sha256"]
    STATE["baseline"] = r1


def test_the_comparison_notices_a_real_difference(world):
    w = world["w1"]("correction-v1-seed-42")
    with psycopg.connect(w.dsn_for("commerce_ingest")) as c:
        for r in ingest_range(c, w.store(), w.reader(), code_sha="t", dependency_lock_sha="t"):
            assert r.status == "INGESTED"
    assert go(w, "corr-final", *FINAL)["status"] == "SUCCEEDED"
    schema, rep = serving_report(world["daily"])
    diffs = compare(STATE["baseline"], rep)
    assert {d.split(":")[0] for d in diffs} == {"fct_orders", "fct_order_items", "fct_payments",
                                                "mart_sales_daily", "mart_category_daily"}
    base_schema = q(world["daily"], "SELECT schema_name FROM control.releases WHERE input_digest IN "
                    "(SELECT input_digest FROM control.releases) AND replay_version = 'baseline-v1' "
                    "ORDER BY created_at DESC LIMIT 1")[0][0]
    with psycopg.connect(world["daily"]["admin_dsn"]) as c:
        rows = differing_rows(c, base_schema, schema, "fct_orders")
    assert [r[0] for r in rows["only_in_a"]] == [B] and [r[0] for r in rows["only_in_b"]] == [B]


def test_redelivery_and_late_arrival_converge_to_the_baseline_answer(world):
    for version in ("duplicate-v1-seed-42", "late-v1-seed-42"):
        w = world["w1"](version)
        with psycopg.connect(w.dsn_for("commerce_ingest")) as c:
            for r in ingest_range(c, w.store(), w.reader(), code_sha="t", dependency_lock_sha="t"):
                assert r.status == "INGESTED"
        assert go(w, f"final-{version}", *FINAL)["status"] == "SUCCEEDED"
        _, rep = serving_report(world["daily"])
        assert compare(STATE["baseline"], rep) == [], version


def test_a_code_fix_gives_a_new_release_over_unchanged_data(world, monkeypatch):
    w = world["w1"]()
    before = serving_generation(world["daily"])
    monkeypatch.setattr(runinfo, "code_sha", lambda *a, **k: "fixed-code-sha")
    out = go(w, "after-code-fix", *FINAL)
    assert out["status"] == "SUCCEEDED", out
    assert serving_generation(world["daily"]) == before + 1
    _, rep = serving_report(world["daily"])
    assert compare(STATE["baseline"], rep) == []
    digests = q(world["daily"], "SELECT count(DISTINCT input_digest) FROM control.releases "
                                "WHERE replay_version = 'baseline-v1' AND status = 'VALIDATED'")[0][0]
    assert digests >= 2


def test_audit_only_backfill_builds_an_as_of_candidate_and_never_publishes(world):
    w = world["w1"]()
    cal = w.reader().calendar()
    gen = serving_generation(world["daily"])
    current = q(world["daily"], "SELECT release_id FROM control.current_release")
    res = run_backfill(w, cal[0].start, cal[6].end, mode="audit", tag="audit-1")
    assert [r["status"] for r in res] == ["SUCCEEDED"], res
    assert serving_generation(world["daily"]) == gen
    assert q(world["daily"], "SELECT release_id FROM control.current_release") == current
    rel = q(world["daily"], "SELECT status, frontier_end FROM control.releases WHERE run_id = %s",
            res[0]["run_id"])
    assert rel[0][0] == "VALIDATED" and rel[0][1] == cal[6].end
    stages = dict(q(world["daily"], "SELECT stage, status FROM control.stage_results WHERE run_id = %s "
                                    "AND stage IN ('publish_release', 'verify_serving')", res[0]["run_id"]))
    assert stages == {"publish_release": "SKIPPED", "verify_serving": "SKIPPED"}


def test_a_publishing_backfill_never_rewinds_serving(world):
    w = world["w1"]()
    cal = w.reader().calendar()
    gen = serving_generation(world["daily"])
    res = run_backfill(w, cal[0].start, cal[13].end, mode="publish", tag="rewind")
    assert [r["status"] for r in res] == ["NO_CHANGE", "NO_CHANGE"], res
    assert serving_generation(world["daily"]) == gen


def test_dry_run_inventory_lists_the_work_without_doing_it(world):
    w = world["w2"]("late-v1-seed-42")
    cal = w.reader().calendar()
    before = q(world["clean"], "SELECT count(*) FROM control.runs")[0][0], \
        q(world["clean"], "SELECT count(*) FROM control.file_receipts")[0][0]
    inv = inventory(w, cal[7].start, cal[20].end)
    assert inv["ranges"] == 2 and inv["total_bytes"] > 0 and inv["missing_manifests"] == []
    assert len(inv["intervals"]) == 14 and all(i["files_with_receipt"] == 0 for i in inv["intervals"])
    assert inv["incomplete_before_window"] == [cal[i].label for i in range(7)]
    assert (q(world["clean"], "SELECT count(*) FROM control.runs")[0][0],
            q(world["clean"], "SELECT count(*) FROM control.file_receipts")[0][0]) == before
    assert [(b - a).days for a, b in plan_ranges(w, cal[0].start, cal[30].end)] == [7, 7, 7, 7, 3]
    json.dumps(inv)
    assert Path(w.store_root).exists()
