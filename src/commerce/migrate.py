from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
_NAME = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_LOCK_KEY = 0x636F6D6D

LEDGER_DDL = """
CREATE SCHEMA IF NOT EXISTS migration_ledger;
CREATE TABLE IF NOT EXISTS migration_ledger.applied (
  version     integer PRIMARY KEY,
  filename    text NOT NULL,
  sha256      text NOT NULL,
  applied_at  timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON SCHEMA migration_ledger FROM PUBLIC;
"""


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    path: Path

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.path.read_bytes()).hexdigest()


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found: list[Migration] = []
    for p in sorted(directory.glob("*.sql")):
        m = _NAME.match(p.name)
        if not m:
            raise MigrationError(f"unexpected migration filename: {p.name}")
        found.append(Migration(int(m.group(1)), p))
    versions = [m.version for m in found]
    if versions != list(range(1, len(found) + 1)):
        raise MigrationError(f"migration versions must be contiguous from 0001: {versions}")
    return found


def apply_all(dsn: str, directory: Path = MIGRATIONS_DIR) -> list[str]:
    migrations = discover(directory)
    applied_now: list[str] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
        try:
            conn.execute(LEDGER_DDL)
            done = {
                v: (f, s)
                for v, f, s in conn.execute(
                    "SELECT version, filename, sha256 FROM migration_ledger.applied"
                )
            }
            for mig in migrations:
                if mig.version in done:
                    _, recorded = done[mig.version]
                    if recorded != mig.sha256:
                        raise MigrationError(
                            f"{mig.path.name} changed after it was applied "
                            f"(recorded {recorded[:12]}, now {mig.sha256[:12]}); "
                            "write a new migration instead"
                        )
                    continue
                with conn.transaction():
                    conn.execute(mig.sql)
                    conn.execute(
                        "INSERT INTO migration_ledger.applied (version, filename, sha256) "
                        "VALUES (%s, %s, %s)",
                        (mig.version, mig.path.name, mig.sha256),
                    )
                applied_now.append(mig.path.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))
    return applied_now
