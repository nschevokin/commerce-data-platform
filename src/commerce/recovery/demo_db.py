from __future__ import annotations

import secrets
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import psycopg.conninfo
from psycopg import sql

from commerce.config import SERVICE_ROLES
from commerce.migrate import apply_all

DEMO_PREFIX = "commerce_demo_"


class NotADemoDatabase(RuntimeError):
    pass


def assert_demo(dbname: str) -> None:
    if not dbname.startswith(DEMO_PREFIX):
        raise NotADemoDatabase(f"refusing to inject faults into {dbname!r}: not a {DEMO_PREFIX}* database")


@contextmanager
def demo_database(server_dsn: str) -> Iterator[dict]:
    dbname = f"{DEMO_PREFIX}{uuid.uuid4().hex[:12]}"
    assert_demo(dbname)
    passwords = {r: secrets.token_urlsafe(18) for r in SERVICE_ROLES}
    with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
        before = {n: (pw, login) for n, pw, login in c.execute(
            "SELECT rolname, rolpassword, rolcanlogin FROM pg_authid WHERE rolname = ANY(%s)",
            (list(SERVICE_ROLES),)).fetchall()}
        c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    admin_dsn = psycopg.conninfo.make_conninfo(server_dsn, dbname=dbname)
    info = psycopg.conninfo.conninfo_to_dict(admin_dsn)
    try:
        apply_all(admin_dsn)
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            for role, pw in passwords.items():
                c.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(pw)))

        def role_dsn(role: str) -> str:
            return psycopg.conninfo.make_conninfo(
                host=info.get("host", "127.0.0.1"), port=info.get("port", "5432"), dbname=dbname,
                user=role, password=passwords[role])
        yield {"dbname": dbname, "admin_dsn": admin_dsn, "role_dsn": role_dsn,
               "host": info.get("host", "127.0.0.1"), "port": info.get("port", "5432"),
               "passwords": passwords}
    finally:
        with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
            assert_demo(dbname)
            c.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(dbname)))
            for role, (stored, login) in before.items():
                c.execute(sql.SQL("ALTER ROLE {} {} PASSWORD {}").format(
                    sql.Identifier(role), sql.SQL("LOGIN" if login else "NOLOGIN"),
                    sql.Literal(stored) if stored else sql.SQL("NULL")))
