from __future__ import annotations

import psycopg
from psycopg import sql

from commerce.contracts.canonical import canonical_json, sha256_hex
from commerce.release.contract import safe_schema

BUSINESS_TABLES: dict[str, tuple[str, ...] | None] = {
    "dim_customers": None, "dim_products": None, "dim_sellers": None, "dim_date": None,
    "fct_orders": None, "fct_order_items": None, "fct_payments": None,
    "mart_sales_daily": None, "mart_category_daily": None, "mart_delivery_cohorts": None,
    "mart_pipeline_health": ("source_name", "latest_rows"),
}


def _columns(conn: psycopg.Connection, schema: str, table: str, only: tuple[str, ...] | None) -> list[str]:
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position", (schema, table)).fetchall()]
    if not cols:
        raise ValueError(f"{schema}.{table} does not exist")
    return list(only) if only else cols


def table_digest(conn: psycopg.Connection, schema: str, table: str,
                 only: tuple[str, ...] | None = None) -> dict:
    ident = safe_schema(schema)
    cols = _columns(conn, schema, table, only)
    row = sql.SQL("ROW({})").format(sql.SQL(", ").join(sql.Identifier(c) for c in cols))
    n, digest = conn.execute(sql.SQL(
        "SELECT count(*), md5(coalesce(string_agg(h, '' ORDER BY h), '')) FROM "
        "(SELECT md5({row}::text) AS h FROM {s}.{t}) x").format(
        row=row, s=ident, t=sql.Identifier(table))).fetchone()
    return {"rows": n, "digest": digest, "columns": cols}


def canonical_report(conn: psycopg.Connection, schema: str) -> dict:
    tables = {t: table_digest(conn, schema, t, only) for t, only in BUSINESS_TABLES.items()}
    frontier = conn.execute(sql.SQL("SELECT max(frontier_end) FROM {}.mart_pipeline_health").format(
        safe_schema(schema))).fetchone()[0]
    overall = sha256_hex(canonical_json({t: [v["rows"], v["digest"]] for t, v in sorted(tables.items())}))
    return {"frontier_end": None if frontier is None else frontier.isoformat(), "tables": tables,
            "overall_sha256": overall}


def compare(a: dict, b: dict) -> list[str]:
    out = []
    if a["frontier_end"] != b["frontier_end"]:
        out.append(f"frontier differs: {a['frontier_end']} vs {b['frontier_end']}")
    for t in sorted(set(a["tables"]) | set(b["tables"])):
        x, y = a["tables"].get(t), b["tables"].get(t)
        if x is None or y is None:
            out.append(f"{t}: present in only one report")
        elif (x["rows"], x["digest"]) != (y["rows"], y["digest"]):
            out.append(f"{t}: {x['rows']} rows vs {y['rows']} rows, digests differ"
                       if x["rows"] != y["rows"] else f"{t}: {x['rows']} rows, content differs")
    return out


def differing_rows(conn: psycopg.Connection, schema_a: str, schema_b: str, table: str,
                   limit: int = 20) -> dict[str, list[tuple]]:
    only = BUSINESS_TABLES[table]
    cols = _columns(conn, schema_a, table, only)
    cl = sql.SQL(", ").join(sql.Identifier(c) for c in cols)

    def side(x: str, y: str):
        q = sql.SQL("SELECT {c} FROM {x}.{t} EXCEPT SELECT {c} FROM {y}.{t} LIMIT {n}").format(
            c=cl, x=safe_schema(x), y=safe_schema(y), t=sql.Identifier(table), n=sql.Literal(limit))
        return conn.execute(q).fetchall()
    return {"only_in_a": side(schema_a, schema_b), "only_in_b": side(schema_b, schema_a)}
