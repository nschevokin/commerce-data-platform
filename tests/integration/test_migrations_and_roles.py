from __future__ import annotations

import shutil

import psycopg
import pytest
from psycopg import errors

from commerce.migrate import MIGRATIONS_DIR, MigrationError, apply_all, discover

pytestmark = pytest.mark.integration


def test_clean_database_applies_every_migration(warehouse):
    assert warehouse["applied"] == [m.path.name for m in discover()]


def test_rerun_is_a_noop(warehouse):
    assert apply_all(warehouse["admin_dsn"]) == []


def test_changed_applied_migration_is_refused(warehouse, tmp_path):
    copy = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS_DIR, copy)
    first = sorted(copy.glob("*.sql"))[0]
    first.write_text(first.read_text() + "\n-- silently edited\n")
    with pytest.raises(MigrationError, match="changed after it was applied"):
        apply_all(warehouse["admin_dsn"], copy)


def test_singletons_bootstrapped_at_generation_zero(warehouse):
    with psycopg.connect(warehouse["admin_dsn"]) as c:
        assert c.execute("SELECT release_id, generation FROM control.current_release").fetchall() \
            == [(None, 0)]
        assert c.execute("SELECT owner_run_id, fencing_generation FROM control.run_guard").fetchall() \
            == [(None, 0)]


def test_receipt_counts_must_reconcile(warehouse):
    with psycopg.connect(warehouse["admin_dsn"]) as c:
        run_id = c.execute(
            "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end,"
            " code_sha, dependency_lock_sha, status) VALUES (gen_random_uuid(), 'manual', 'r',"
            " '2026-01-02Z', '2026-01-03Z', 'x', 'y', 'CREATED') RETURNING run_id").fetchone()[0]
        with pytest.raises(errors.CheckViolation):
            c.execute(
                "INSERT INTO control.file_receipts (replay_version, source_name, file_sha256,"
                " interval_start, manifest_sha256, input_rows, accepted_new_rows,"
                " exact_duplicate_rows, run_id, fencing_generation) VALUES"
                " ('r', 'orders', repeat('a', 64), '2026-01-02Z', 'm', 10, 7, 2, %s, 1)",
                (run_id,))
        c.rollback()


def _denied(dsn: str, statement: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as c:
        with pytest.raises(errors.InsufficientPrivilege):
            c.execute(statement)


def test_dashboard_reads_release_info(role_dsn):
    with psycopg.connect(role_dsn("commerce_dashboard")) as c:
        assert c.execute("SELECT status, generation FROM serving.release_info").fetchone() \
            == ("NOT_PUBLISHED", 0)


@pytest.mark.parametrize("statement", [
    "SELECT * FROM bronze.source_events",
    "SELECT * FROM control.runs",
    "SELECT * FROM control.current_release",
    "SELECT * FROM migration_ledger.applied",
    "CREATE TABLE serving.x (i int)",
    "CREATE TABLE public.x (i int)",
    "CREATE TEMP TABLE x (i int)",
    "CREATE SCHEMA dashboard_owned",
    "CREATE OR REPLACE VIEW serving.release_info AS SELECT 1",
])
def test_dashboard_boundary(role_dsn, statement):
    _denied(role_dsn("commerce_dashboard"), statement)


@pytest.mark.parametrize("role,statement", [
    ("commerce_ingest", "UPDATE bronze.source_events SET revision = 2"),
    ("commerce_ingest", "DELETE FROM bronze.source_events"),
    ("commerce_ingest", "CREATE VIEW serving.v AS SELECT 1"),
    ("commerce_ingest", "UPDATE control.current_release SET generation = 99"),
    ("commerce_ingest", "UPDATE control.run_guard SET fencing_generation = 99"),
    ("commerce_transform", "INSERT INTO bronze.source_events SELECT * FROM bronze.source_events"),
    ("commerce_transform", "CREATE VIEW serving.v AS SELECT 1"),
    ("commerce_transform", "UPDATE control.current_release SET generation = 99"),
    ("commerce_publish", "INSERT INTO bronze.source_events SELECT * FROM bronze.source_events"),
    ("commerce_publish", "SELECT * FROM bronze.source_events"),
])
def test_writer_boundaries(role_dsn, role, statement):
    _denied(role_dsn(role), statement)


def test_publisher_owns_serving_and_transform_creates_candidates(role_dsn):
    with psycopg.connect(role_dsn("commerce_publish"), autocommit=True) as c:
        c.execute("CREATE VIEW serving.publisher_probe AS SELECT 1 AS one")
        c.execute("DROP VIEW serving.publisher_probe")
    with psycopg.connect(role_dsn("commerce_transform"), autocommit=True) as c:
        c.execute("CREATE SCHEMA cand_00000000000000000000000000000000")
        c.execute("DROP SCHEMA cand_00000000000000000000000000000000")


def test_dashboard_cannot_read_a_candidate_schema(role_dsn):
    with psycopg.connect(role_dsn("commerce_transform"), autocommit=True) as c:
        c.execute("CREATE SCHEMA cand_11111111111111111111111111111111")
        c.execute("CREATE TABLE cand_11111111111111111111111111111111.t AS SELECT 1 AS i")
    try:
        _denied(role_dsn("commerce_dashboard"),
                "SELECT * FROM cand_11111111111111111111111111111111.t")
    finally:
        with psycopg.connect(role_dsn("commerce_transform"), autocommit=True) as c:
            c.execute("DROP SCHEMA cand_11111111111111111111111111111111 CASCADE")
