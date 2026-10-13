from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import psycopg.conninfo
from psycopg import sql

from commerce.audit.serving_checksum import serving_checksum
from commerce.config import SERVICE_ROLES
from commerce.recovery.demo_db import DEMO_PREFIX, assert_demo


class BackupError(RuntimeError):
    pass


def _tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise BackupError(f"{name} is not installed on this machine (install the PostgreSQL client tools)")
    return path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _require_matching_client(server_version: str) -> None:
    out = subprocess.run([_tool("pg_dump"), "--version"], capture_output=True, text=True).stdout
    found = re.search(r"\)\s+(\d+)", out)
    client_major = found.group(1) if found else "?"
    if client_major != server_version.split(".")[0]:
        raise BackupError(f"pg_dump major version {client_major} does not match the server "
                          f"({server_version}); install the matching PostgreSQL client tools")


def _pg_env(dsn: str) -> tuple[list[str], dict[str, str]]:
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    args = ["-h", info.get("host", "127.0.0.1"), "-p", str(info.get("port", "5432")),
            "-U", info["user"]]
    env = {"PGPASSWORD": info.get("password", ""), "PATH": "/usr/bin:/bin:/usr/local/bin"}
    return args, env


def backup(admin_dsn: str, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    dbname = psycopg.conninfo.conninfo_to_dict(admin_dsn)["dbname"]
    started = time.perf_counter()
    with psycopg.connect(admin_dsn) as c:
        schemas = [r[0] for r in c.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'cand\\_%'")]
        cur = c.execute("SELECT schema_name, release_id::text, generation FROM control.releases "
                        "JOIN control.current_release USING (release_id)").fetchone()
        guard = c.execute("SELECT count(*) FROM control.run_guard WHERE owner_run_id IS NOT NULL").fetchone()
        checksum = serving_checksum(c)
        counts = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                  for t in ("bronze.source_events", "control.file_receipts", "control.releases",
                            "control.publications", "control.quality_results")}
        server_version = c.execute("SHOW server_version").fetchone()[0]
    _require_matching_client(server_version)
    if cur is None:
        raise BackupError("nothing is published; refusing to back up an empty warehouse")
    if guard and guard[0]:
        raise BackupError("a writer holds the guard; back up when the pipeline is idle")
    keep = cur[0]
    excluded = [s for s in schemas if s != keep]
    args, env = _pg_env(admin_dsn)
    dump = out_dir / f"{dbname}.dump"
    cmd = [_tool("pg_dump"), *args, "-Fc", "-f", str(dump), *sum((["-N", s] for s in excluded), []), dbname]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        raise BackupError(res.stderr.strip()[:500])
    manifest = {
        "created_at": datetime.now(UTC).isoformat(), "database": dbname, "dump_file": dump.name,
        "dump_bytes": dump.stat().st_size, "dump_sha256": _sha256(dump),
        "pg_dump_version": subprocess.run([_tool("pg_dump"), "--version"], capture_output=True,
                                          text=True).stdout.strip(),
        "server_version": server_version, "current_release": cur[1], "generation": cur[2],
        "served_schema": keep, "candidate_schemas_excluded": len(excluded),
        "table_counts": counts, "serving_checksum": checksum,
        "seconds": round(time.perf_counter() - started, 2),
        "restore": "commerce restore-drill --dump <file> --manifest <file> --server-dsn <superuser dsn>"}
    (out_dir / f"{dbname}.manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True),
                                                     encoding="utf-8")
    return manifest


def restore_drill(server_dsn: str, dump: Path, manifest: dict) -> dict:
    checks: dict[str, object] = {}
    started = time.perf_counter()
    checks["dump_checksum_matches_manifest"] = _sha256(dump) == manifest["dump_sha256"]
    if not checks["dump_checksum_matches_manifest"]:
        return {"outcome": "FAIL", "checks": checks}
    with psycopg.connect(server_dsn, dbname="postgres") as c:
        _require_matching_client(c.execute("SHOW server_version").fetchone()[0])
    dbname = f"{DEMO_PREFIX}restore_{uuid.uuid4().hex[:8]}"
    assert_demo(dbname)
    with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
        for role in SERVICE_ROLES:
            if not c.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone():
                c.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT")
                          .format(sql.Identifier(role)))
        c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    admin = psycopg.conninfo.make_conninfo(server_dsn, dbname=dbname)
    try:
        args, env = _pg_env(server_dsn)
        t0 = time.perf_counter()
        res = subprocess.run([_tool("pg_restore"), *args, "-d", dbname, "--exit-on-error", str(dump)],
                             env=env, capture_output=True, text=True)
        checks["pg_restore_exit_code"] = res.returncode
        if res.returncode != 0:
            checks["pg_restore_error"] = res.stderr.strip()[:500]
            return {"outcome": "FAIL", "checks": checks}
        restore_seconds = round(time.perf_counter() - t0, 2)
        with psycopg.connect(admin) as c:
            got = serving_checksum(c)
            checks["serving_checksum_equal"] = got["sha256"] == manifest["serving_checksum"]["sha256"]
            checks["current_release_equal"] = (got["release_id"] == manifest["current_release"]
                                               and got["generation"] == manifest["generation"])
            counts = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in manifest["table_counts"]}
            checks["control_and_bronze_counts_equal"] = counts == manifest["table_counts"]
            checks["no_writer_holds_the_guard"] = c.execute(
                "SELECT count(*) FROM control.run_guard WHERE owner_run_id IS NOT NULL").fetchone()[0] == 0
            checks["blocking_failures_in_current_release"] = c.execute(
                "SELECT blocking_failures FROM control.releases WHERE release_id = %s::uuid",
                (manifest["current_release"],)).fetchone()[0]
            priv = lambda role, obj: c.execute(  # noqa: E731
                "SELECT has_table_privilege(%s, %s, 'SELECT')", (role, obj)).fetchone()[0]
            checks["dashboard_role_reads_serving"] = priv("commerce_dashboard", "serving.sales_daily")
            checks["dashboard_role_cannot_read_bronze"] = not priv("commerce_dashboard", "bronze.source_events")
        ok = (checks["serving_checksum_equal"] and checks["current_release_equal"]
              and checks["control_and_bronze_counts_equal"] and checks["no_writer_holds_the_guard"]
              and checks["blocking_failures_in_current_release"] == 0
              and checks["dashboard_role_reads_serving"] and checks["dashboard_role_cannot_read_bronze"])
        return {"outcome": "PASS" if ok else "FAIL", "checks": checks, "restore_seconds": restore_seconds,
                "total_seconds": round(time.perf_counter() - started, 2), "restored_database": dbname,
                "note": "Passwords are not in a dump; logins must be bootstrapped again before writers run."}
    finally:
        with psycopg.connect(server_dsn, dbname="postgres", autocommit=True) as c:
            assert_demo(dbname)
            c.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(dbname)))
