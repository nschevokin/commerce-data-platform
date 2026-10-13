from __future__ import annotations

import shutil

import psycopg
import pytest
from replay_helpers import copy_fixture

from commerce.audit.serving_checksum import serving_checksum
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.recovery.backup import BackupError, backup, restore_drill
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]
STATE: dict = {}


@pytest.fixture(scope="module")
def published(own_warehouse, own_role_dsn, tmp_path_factory):
    if not shutil.which("pg_dump"):
        pytest.skip("PostgreSQL client tools (pg_dump) are not installed")
    root = tmp_path_factory.mktemp("backup")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    env = {"COMMERCE_WAREHOUSE_HOST": str(own_warehouse["host"]),
           "COMMERCE_WAREHOUSE_PORT": str(own_warehouse["port"]),
           "COMMERCE_WAREHOUSE_DB": own_warehouse["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
    w = Wiring(store_root=str(root / "store"), dataset_sha256=src.digest(), replay_version="baseline-v1",
               artifacts_root=root / "builds", dsn_for=own_role_dsn, dbt_env=env, exact_fixture=True)
    cal = w.reader().calendar()
    for i, (a, b) in enumerate([(0, 7), (7, 14), (14, 21), (21, 28), (28, 31)]):
        out = run_pipeline(w, dag_run_id=f"bk-{i}", mode="manual", interval_start=cal[a].start,
                           interval_end=cal[b - 1].end)
        assert out["status"] in ("SUCCEEDED", "NO_CHANGE"), out
    return {"root": root, "db": own_warehouse, "dsn": own_role_dsn}


def test_the_checksum_ignores_operational_identity_and_sees_business_changes(published):
    with psycopg.connect(published["dsn"]("commerce_dashboard")) as c:
        a = serving_checksum(c)
        b = serving_checksum(c)
    assert a["sha256"] == b["sha256"] and len(a["sha256"]) == 64
    assert set(a["rows"]) == {"sales_daily", "category_daily", "delivery_cohorts", "pipeline_health"}
    STATE["checksum"] = a["sha256"]


def test_backup_and_restore_prove_the_same_serving_state(published, server_dsn):
    out = published["root"] / "backups"
    manifest = backup(published["db"]["admin_dsn"], out)
    assert manifest["serving_checksum"]["sha256"] == STATE["checksum"]
    assert manifest["dump_bytes"] > 0 and manifest["pg_dump_version"].startswith("pg_dump")
    STATE["manifest"], STATE["dump"] = manifest, out / manifest["dump_file"]
    res = restore_drill(server_dsn, STATE["dump"], manifest)
    assert res["outcome"] == "PASS", res
    for key in ("serving_checksum_equal", "current_release_equal", "control_and_bronze_counts_equal",
                "no_writer_holds_the_guard", "dashboard_role_reads_serving",
                "dashboard_role_cannot_read_bronze"):
        assert res["checks"][key] is True, key
    assert res["checks"]["blocking_failures_in_current_release"] == 0
    with psycopg.connect(server_dsn, dbname="postgres") as c:
        assert c.execute("SELECT count(*) FROM pg_database WHERE datname LIKE 'commerce_demo_restore_%'"
                         ).fetchone()[0] == 0


def test_a_damaged_dump_or_a_wrong_manifest_is_refused(published, server_dsn, tmp_path):
    bad = tmp_path / "bad.dump"
    data = bytearray(STATE["dump"].read_bytes())
    data[len(data) // 2] ^= 0xFF
    bad.write_bytes(bytes(data))
    assert restore_drill(server_dsn, bad, STATE["manifest"])["outcome"] == "FAIL"
    wrong = {**STATE["manifest"], "serving_checksum": {**STATE["manifest"]["serving_checksum"],
                                                       "sha256": "0" * 64}}
    res = restore_drill(server_dsn, STATE["dump"], wrong)
    assert res["outcome"] == "FAIL" and res["checks"]["serving_checksum_equal"] is False


def test_backup_refuses_while_a_writer_holds_the_guard(published):
    admin = published["db"]["admin_dsn"]
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute("SELECT control.release_guard(owner_run_id, fencing_generation) FROM control.run_guard "
                  "WHERE owner_run_id IS NOT NULL")
        run = c.execute("SELECT run_id FROM control.runs LIMIT 1").fetchone()[0]
        c.execute("SELECT control.acquire_guard(%s)", (run,))
        try:
            with pytest.raises(BackupError, match="guard"):
                backup(admin, published["root"] / "refused")
        finally:
            gen = c.execute("SELECT fencing_generation FROM control.run_guard").fetchone()[0]
            c.execute("SELECT control.release_guard(%s, %s)", (run, gen))


def _schemas(admin):
    with psycopg.connect(admin) as c:
        return {r[0] for r in c.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'cand\\_%'")}


def test_gc_dry_run_changes_nothing_and_names_what_it_would_drop(published):
    from commerce.release.retention import run_gc

    admin = published["db"]["admin_dsn"]
    before = _schemas(admin)
    res = run_gc(admin, dry_run=True)
    assert _schemas(admin) == before and res["dropped"] == []
    kept_reasons = {k["reason"] for k in res["kept"]}
    assert "current release" in kept_reasons
    assert len(before) >= 4 and len(res["to_drop"]) == len(before) - 3
    STATE["to_drop"] = {d["schema"] for d in res["to_drop"]}


def test_gc_apply_keeps_current_and_two_previous_and_serving_still_works(published):
    from commerce.release.retention import run_gc

    admin = published["db"]["admin_dsn"]
    run_gc(admin, dry_run=False)
    left = _schemas(admin)
    assert len(left) == 3 and not (left & STATE["to_drop"])
    with psycopg.connect(published["dsn"]("commerce_dashboard")) as c:
        assert serving_checksum(c)["sha256"] == STATE["checksum"]
    assert run_gc(admin, dry_run=False)["dropped"] == []
    with psycopg.connect(admin) as c:
        statuses = {r[0] for r in c.execute("SELECT status FROM control.releases WHERE schema_name = ANY(%s)",
                                            (sorted(STATE["to_drop"]),))}
    assert statuses == {"DROPPED"}


def test_gc_refuses_while_a_run_owns_the_guard_and_marks_dropped_releases(published):
    from commerce.release.retention import RetentionRefused, run_gc

    admin = published["db"]["admin_dsn"]
    with psycopg.connect(admin, autocommit=True) as c:
        assert c.execute("SELECT count(*) FROM control.releases WHERE status = 'DROPPED'").fetchone()[0] > 0
        run = c.execute("SELECT run_id FROM control.runs LIMIT 1").fetchone()[0]
        gen = c.execute("SELECT control.acquire_guard(%s)", (run,)).fetchone()[0]
        try:
            with pytest.raises(RetentionRefused):
                run_gc(admin, dry_run=False)
        finally:
            c.execute("SELECT control.release_guard(%s, %s)", (run, gen))
