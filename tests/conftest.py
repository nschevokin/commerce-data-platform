from __future__ import annotations

import os
import uuid
from pathlib import Path

import psycopg
import pytest

from commerce.config import SERVICE_ROLES

REPO = Path(__file__).resolve().parents[1]
GOLDEN = REPO / "tests" / "fixtures" / "golden_v1"
TEST_PASSWORDS = {r: f"test-only-{r}" for r in SERVICE_ROLES}


def _server_dsn() -> str:
    dsn = os.environ.get("COMMERCE_TEST_SERVER_DSN")
    if not dsn:
        pytest.skip("COMMERCE_TEST_SERVER_DSN not set; integration tests need PostgreSQL")
    return dsn


def _conninfo(dsn: str) -> dict[str, str]:
    return psycopg.conninfo.conninfo_to_dict(dsn)


@pytest.fixture(scope="session")
def server_dsn() -> str:
    return _server_dsn()


def _snapshot_roles(server_dsn: str) -> dict[str, tuple[str | None, bool]]:
    with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
        rows = c.execute("SELECT rolname, rolpassword, rolcanlogin FROM pg_authid "
                         "WHERE rolname = ANY(%s)", (list(SERVICE_ROLES),)).fetchall()
    return {name: (pw, login) for name, pw, login in rows}


def _restore_roles(server_dsn: str, before: dict[str, tuple[str | None, bool]]) -> None:
    from psycopg import sql
    with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
        for role in SERVICE_ROLES:
            if role not in before:
                continue
            stored, can_login = before[role]
            c.execute(sql.SQL("ALTER ROLE {} {} PASSWORD {}").format(
                sql.Identifier(role), sql.SQL("LOGIN" if can_login else "NOLOGIN"),
                sql.Literal(stored) if stored else sql.SQL("NULL")))


def _provision(server_dsn: str):
    from commerce.migrate import apply_all

    roles_before = _snapshot_roles(server_dsn)

    dbname = f"commerce_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
        c.execute(f'CREATE DATABASE "{dbname}"')
    admin_dsn = psycopg.conninfo.make_conninfo(server_dsn, dbname=dbname)
    try:
        applied = apply_all(admin_dsn)
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            for role, pw in TEST_PASSWORDS.items():
                c.execute(psycopg.sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                    psycopg.sql.Identifier(role), psycopg.sql.Literal(pw)))
        info = _conninfo(admin_dsn)
        yield {
            "dbname": dbname,
            "admin_dsn": admin_dsn,
            "applied": applied,
            "host": info.get("host", "127.0.0.1"),
            "port": info.get("port", "5432"),
        }
    finally:
        with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
            c.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        _restore_roles(server_dsn, roles_before)


@pytest.fixture(scope="session")
def warehouse(server_dsn: str):
    yield from _provision(server_dsn)


@pytest.fixture(scope="module")
def own_warehouse(server_dsn: str):
    yield from _provision(server_dsn)


@pytest.fixture(scope="module")
def own_role_dsn(own_warehouse):
    def make(role: str) -> str:
        return psycopg.conninfo.make_conninfo(
            host=own_warehouse["host"], port=own_warehouse["port"], dbname=own_warehouse["dbname"],
            user=role, password=TEST_PASSWORDS[role])
    return make


@pytest.fixture(scope="session")
def role_dsn(warehouse):
    def make(role: str) -> str:
        return psycopg.conninfo.make_conninfo(
            host=warehouse["host"], port=warehouse["port"], dbname=warehouse["dbname"],
            user=role, password=TEST_PASSWORDS[role])
    return make


@pytest.fixture(scope="session")
def transform_env(warehouse) -> dict[str, str]:
    return {
        "COMMERCE_WAREHOUSE_HOST": str(warehouse["host"]),
        "COMMERCE_WAREHOUSE_PORT": str(warehouse["port"]),
        "COMMERCE_WAREHOUSE_DB": warehouse["dbname"],
        "COMMERCE_TRANSFORM_USER": "commerce_transform",
        "COMMERCE_TRANSFORM_PASSWORD": TEST_PASSWORDS["commerce_transform"],
    }
