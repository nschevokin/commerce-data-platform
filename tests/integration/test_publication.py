from __future__ import annotations

import threading
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal

import psycopg
import pytest
from replay_helpers import A, copy_fixture

from commerce.ingestion.ledger import GenerationMovedError, StaleWriterError, translate_db_error
from commerce.ingestion.runner import ingest_range
from commerce.release.pipeline import run_release, run_rollback
from commerce.release.publisher import InjectedFault, Outcome, PublicationRefused, publish
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.reader import ReplayReader
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore
from commerce.serving.reader import read_snapshot

pytestmark = [pytest.mark.integration, pytest.mark.dbt]

CORR = "correction-v1-seed-42"
ORPHAN = "fault-orphan-item-v1"
B = "fx0000000000000000000000000order_b"
STATE: dict = {}


@pytest.fixture(scope="module")
def world(own_warehouse, own_role_dsn, tmp_path_factory):
    root = tmp_path_factory.mktemp("publication")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="correction", order_ids=[B])
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault", variant="orphan_item",
                   order_ids=[A])
    with psycopg.connect(own_role_dsn("commerce_ingest")) as conn:
        for v in (CORR, ORPHAN):
            res = ingest_range(conn, store, ReplayReader(store, ds, v), code_sha="t", dependency_lock_sha="t")
            assert all(r.status == "INGESTED" for r in res), res
    env = {"COMMERCE_WAREHOUSE_HOST": str(own_warehouse["host"]),
           "COMMERCE_WAREHOUSE_PORT": str(own_warehouse["port"]),
           "COMMERCE_WAREHOUSE_DB": own_warehouse["dbname"],
           "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
    return {"store": store, "ds": ds, "env": env, "root": root, "admin": own_warehouse["admin_dsn"],
            "dsn": own_role_dsn}


def release(world, version, idx, fault=None):
    reader = ReplayReader(world["store"], world["ds"], version)
    return run_release(
        publish_dsn=world["dsn"]("commerce_publish"), transform_dsn=world["dsn"]("commerce_transform"),
        dashboard_dsn=world["dsn"]("commerce_dashboard"), transform_env=world["env"], reader=reader,
        frontier_end=reader.calendar()[idx].end, artifacts_root=world["root"] / "builds",
        exact_fixture=True, dataset_sha256=world["ds"], fault=fault)


def snapshot(world):
    with psycopg.connect(world["dsn"]("commerce_dashboard")) as c:
        return read_snapshot(c)


def serving_aov(snap) -> Decimal:
    rows = snap.tables["sales_daily"]
    orders = sum(r["eligible_orders"] for r in rows)
    return (sum(r["merchandise_value_brl"] for r in rows) / orders).quantize(Decimal("0.01"))


def pointer(world):
    with psycopg.connect(world["admin"]) as c:
        rel, gen = c.execute("SELECT release_id, generation FROM control.current_release").fetchone()
        n = c.execute("SELECT count(*) FROM control.publications").fetchone()[0]
    return (None if rel is None else str(rel)), gen, n


@contextmanager
def guard(world):
    run_id = uuid.uuid4()
    with psycopg.connect(world["dsn"]("commerce_publish")) as c:
        with c.transaction():
            c.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                      "code_sha, dependency_lock_sha, status) VALUES (%s, 'manual', 'x', now(), "
                      "now() + interval '1 second', 't', 't', 'CREATED')", (run_id,))
            gen = c.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
        try:
            yield c, run_id, gen
        finally:
            try:
                if c.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    c.rollback()
                with c.transaction():
                    c.execute("SELECT control.release_guard(%s, %s)", (run_id, gen))
            except psycopg.Error:
                pass


def release_status(world, rid):
    with psycopg.connect(world["admin"]) as c:
        return c.execute("SELECT status, blocking_failures FROM control.releases WHERE release_id = %s",
                         (rid,)).fetchone()


def test_first_release_is_published_and_the_dashboard_sees_exactly_it(world):
    assert pointer(world) == (None, 0, 0)
    empty = snapshot(world)
    assert empty.release["release_id"] is None and empty.tables == {}
    out = release(world, CORR, 13)
    assert out.status == "SUCCEEDED", out.to_obj()
    STATE["r1"] = out.release_id
    assert pointer(world) == (out.release_id, 1, 1)
    snap = snapshot(world)
    assert str(snap.release["release_id"]) == out.release_id
    assert snap.release["generation"] == 1 and snap.release["status"] == "PUBLISHED"
    assert serving_aov(snap) == Decimal("65.00")
    health = snap.tables["pipeline_health"]
    assert len(health) == 7
    assert {str(r["release_id"]) for r in health} == {out.release_id}
    assert all(r["blocking_failures"] == 0 for r in health)
    with psycopg.connect(world["admin"]) as c:
        assert c.execute("SELECT status FROM control.runs WHERE run_id = %s", (out.run_id,)).fetchone()[0] \
            == "SUCCEEDED"
        assert c.execute("SELECT owner_run_id FROM control.run_guard").fetchone()[0] is None
        checks = c.execute("SELECT count(*) FROM control.quality_results WHERE release_id = %s",
                           (out.release_id,)).fetchone()[0]
    assert checks >= 8


def test_same_inputs_again_changes_nothing(world):
    out = release(world, CORR, 13)
    assert out.status == "NO_CHANGE" and out.release_id is None
    assert pointer(world) == (STATE["r1"], 1, 1)


def test_a_correction_produces_generation_two_with_the_new_answer(world):
    out = release(world, CORR, 14)
    assert out.status == "SUCCEEDED", out.to_obj()
    STATE["r2"] = out.release_id
    assert pointer(world) == (out.release_id, 2, 2)
    assert serving_aov(snapshot(world)) == Decimal("67.50")
    with psycopg.connect(world["admin"]) as c:
        prev = c.execute("SELECT previous_release_id, kind FROM control.publications WHERE generation = 2"
                         ).fetchone()
    assert (str(prev[0]), prev[1]) == (STATE["r1"], "PUBLISH")


def test_a_rejected_candidate_is_never_served(world):
    out = release(world, ORPHAN, 14)
    assert out.status == "FAILED_QUALITY", out.to_obj()
    assert pointer(world) == (STATE["r2"], 2, 2)
    status, _ = release_status(world, out.release_id)
    assert status == "REJECTED"
    with pytest.raises(PublicationRefused, match="only a VALIDATED"):
        with guard(world) as (c, run_id, gen):
            publish(c, release_id=out.release_id, run_id=run_id, generation=gen)
    assert pointer(world) == (STATE["r2"], 2, 2)
    assert serving_aov(snapshot(world)) == Decimal("67.50")


def test_a_crash_before_the_switch_leaves_the_old_release_serving(world):
    with pytest.raises(InjectedFault):
        release(world, CORR, 15, fault="before_ddl")
    with psycopg.connect(world["admin"]) as c:
        row = c.execute("SELECT release_id, status FROM control.releases WHERE status = 'VALIDATED' "
                        "AND release_id NOT IN (SELECT release_id FROM control.publications)").fetchone()
        assert c.execute("SELECT owner_run_id FROM control.run_guard").fetchone()[0] is None
    assert row is not None
    STATE["r3"] = str(row[0])
    assert pointer(world) == (STATE["r2"], 2, 2)
    assert str(snapshot(world).release["release_id"]) == STATE["r2"]


def test_a_stale_publisher_and_a_moved_generation_are_refused(world):
    with guard(world) as (c, run_id, gen), psycopg.connect(world["admin"]) as admin:
        takeover = uuid.uuid4()
        with admin.transaction():
            admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, "
                          "interval_end, code_sha, dependency_lock_sha, status) VALUES (%s, 'audit', 'op', "
                          "now(), now() + interval '1 second', 'o', 'o', 'CREATED')", (takeover,))
            new_gen = admin.execute("SELECT control.takeover_guard(%s, 'tester', 'stale publisher test')",
                                    (takeover,)).fetchone()[0]
        with pytest.raises(StaleWriterError):
            publish(c, release_id=STATE["r3"], run_id=run_id, generation=gen)
        with admin.transaction():
            admin.execute("SELECT control.release_guard(%s, %s)", (takeover, new_gen))
    assert pointer(world) == (STATE["r2"], 2, 2)
    with guard(world) as (c, run_id, gen):
        with pytest.raises(psycopg.Error) as ei, c.transaction():
            c.execute("SELECT * FROM control.lock_for_publication(%s, %s)", (run_id, gen))
            c.execute("SELECT control.record_publication(%s, %s, %s, %s, 'PUBLISH', NULL, NULL, NULL)",
                      (run_id, gen, STATE["r3"], 1))
        assert isinstance(translate_db_error(ei.value), GenerationMovedError)
    assert pointer(world) == (STATE["r2"], 2, 2)


def test_faults_inside_the_publication_transaction_roll_everything_back(world):
    for point in ("after_ddl", "before_commit"):
        with guard(world) as (c, run_id, gen), pytest.raises(InjectedFault):
            publish(c, release_id=STATE["r3"], run_id=run_id, generation=gen, fault=point)
        assert pointer(world) == (STATE["r2"], 2, 2), point
        snap = snapshot(world)
        assert str(snap.release["release_id"]) == STATE["r2"], point
        assert {str(r["release_id"]) for r in snap.tables["pipeline_health"]} == {STATE["r2"]}, point


def test_lost_acknowledgement_does_not_publish_twice(world):
    with guard(world) as (c, run_id, gen), pytest.raises(InjectedFault):
        publish(c, release_id=STATE["r3"], run_id=run_id, generation=gen, fault="after_commit")
    assert pointer(world) == (STATE["r3"], 3, 3)
    with guard(world) as (c, run_id, gen):
        again = publish(c, release_id=STATE["r3"], run_id=run_id, generation=gen)
    assert again.outcome is Outcome.ALREADY_COMMITTED and again.generation == 3
    assert pointer(world) == (STATE["r3"], 3, 3)


def test_older_frontier_cannot_be_published_but_can_be_rolled_back_to(world):
    out = release(world, CORR, 12)
    assert out.status == "FAILED_QUALITY" and "frontier regression" in out.detail, out.to_obj()
    STATE["r_old"] = out.release_id
    assert release_status(world, out.release_id) == ("VALIDATED", 0)
    assert pointer(world) == (STATE["r3"], 3, 3)

    with psycopg.connect(world["dsn"]("commerce_transform")) as t:
        schema = t.execute("SELECT schema_name FROM control.releases WHERE release_id = %s",
                           (out.release_id,)).fetchone()[0]
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            t.execute(f"DELETE FROM {schema}.mart_sales_daily")
        t.rollback()
        t.execute(f"ALTER TABLE {schema}.mart_sales_daily RENAME COLUMN excluded_orders TO excluded")
        t.commit()
        with pytest.raises(PublicationRefused, match="serving contract"):
            run_rollback(publish_dsn=world["dsn"]("commerce_publish"), target_release_id=out.release_id,
                         operator="tester", reason="contract tamper test")
        t.execute(f"ALTER TABLE {schema}.mart_sales_daily RENAME COLUMN excluded TO excluded_orders")
        t.commit()
    assert pointer(world) == (STATE["r3"], 3, 3)

    with pytest.raises(PublicationRefused, match="operator and a reason"):
        run_rollback(publish_dsn=world["dsn"]("commerce_publish"), target_release_id=out.release_id,
                     operator="", reason="")
    res = run_rollback(publish_dsn=world["dsn"]("commerce_publish"), target_release_id=out.release_id,
                       operator="tester", reason="rollback test", incident="INC-1")
    assert res.kind == "ROLLBACK" and res.generation == 4 and res.previous_release_id == STATE["r3"]
    assert pointer(world) == (out.release_id, 4, 4)
    snap = snapshot(world)
    assert str(snap.release["release_id"]) == out.release_id and snap.release["status"] == "ROLLBACK"
    with psycopg.connect(world["admin"]) as c:
        assert c.execute("SELECT operator, reason, incident_id FROM control.publications "
                         "WHERE generation = 4").fetchone() == ("tester", "rollback test", "INC-1")
        assert c.execute("SELECT owner_run_id FROM control.run_guard").fetchone()[0] is None


def test_a_refresh_sees_one_release_even_while_a_publication_waits(world):
    started, finished = threading.Event(), threading.Event()
    box: dict = {}

    def switch():
        started.set()
        box["result"] = run_rollback(publish_dsn=world["dsn"]("commerce_publish"),
                                     target_release_id=STATE["r3"], operator="tester",
                                     reason="concurrency test")
        finished.set()

    thread = threading.Thread(target=switch)

    def between():
        thread.start()
        started.wait(5)
        assert not finished.wait(1.5), "publication must wait for the open refresh"

    with psycopg.connect(world["dsn"]("commerce_dashboard")) as c:
        snap = read_snapshot(c, between=between)
    thread.join(30)
    assert finished.is_set()
    seen = {str(snap.release["release_id"])} | {str(r["release_id"]) for r in snap.tables["pipeline_health"]}
    assert seen == {STATE["r_old"]}
    after = snapshot(world)
    assert str(after.release["release_id"]) == STATE["r3"] and after.release["generation"] == 5
    assert {str(r["release_id"]) for r in after.tables["pipeline_health"]} == {STATE["r3"]}


def test_dashboard_role_sees_only_the_serving_schema(world):
    with psycopg.connect(world["dsn"]("commerce_dashboard"), autocommit=True) as c:
        schema = psycopg.connect(world["admin"]).execute(
            "SELECT schema_name FROM control.releases WHERE release_id = %s", (STATE["r3"],)).fetchone()[0]
        for target in (f"{schema}.mart_sales_daily", "bronze.source_events", "control.releases"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                c.execute(f"SELECT 1 FROM {target}")
        with pytest.raises(psycopg.errors.Error):
            c.execute("CREATE OR REPLACE VIEW serving.sales_daily AS SELECT 1")
    started = time.monotonic()
    assert snapshot(world).release is not None
    assert time.monotonic() - started < 10
