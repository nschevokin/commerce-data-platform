from __future__ import annotations

import os
from pathlib import Path

SERVICE_ROLES = ("commerce_ingest", "commerce_transform", "commerce_publish", "commerce_dashboard")


class ConfigError(RuntimeError):
    pass


def secret(name: str) -> str | None:
    file_var = os.environ.get(f"{name}_FILE")
    if file_var:
        return Path(file_var).read_text(encoding="utf-8").strip()
    return os.environ.get(name)


def require(name: str) -> str:
    value = secret(name)
    if not value:
        raise ConfigError(f"missing required setting {name} (or {name}_FILE)")
    return value


def admin_dsn() -> str:
    return require("COMMERCE_ADMIN_DSN")


def role_password_var(role: str) -> str:
    return f"{role.upper()}_PASSWORD"


def role_dsn(role: str) -> str:
    import psycopg.conninfo

    return psycopg.conninfo.make_conninfo(
        host=os.environ.get("COMMERCE_WAREHOUSE_HOST", "127.0.0.1"),
        port=os.environ.get("COMMERCE_WAREHOUSE_PORT", "5432"),
        dbname=os.environ.get("COMMERCE_WAREHOUSE_DB", "commerce"),
        user=role, password=require(role_password_var(role)))
