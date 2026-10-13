from __future__ import annotations

import psycopg
from psycopg import sql

from commerce.config import SERVICE_ROLES, require, role_password_var


def bootstrap_logins(admin_dsn: str) -> list[str]:
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        for role in SERVICE_ROLES:
            password = require(role_password_var(role))
            conn.execute(
                sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
    return list(SERVICE_ROLES)
