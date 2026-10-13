from __future__ import annotations

import re

import psycopg
from psycopg import sql

SERVING_CONTRACT_VERSION = 1
SCHEMA_RE = re.compile(r"^cand_[0-9a-f]{32}$")

Column = tuple[str, str]

SERVING_VIEWS: dict[str, tuple[str, tuple[Column, ...]]] = {
    "sales_daily": ("mart_sales_daily", (
        ("purchase_date", "date"), ("eligible_orders", "bigint"), ("excluded_orders", "bigint"),
        ("eligible_orders_with_items", "bigint"), ("eligible_orders_missing_items", "bigint"),
        ("eligible_orders_missing_payments", "bigint"), ("merchandise_value_brl", "numeric(18,2)"),
        ("freight_value_brl", "numeric(18,2)"), ("recorded_payment_brl", "numeric(18,2)"),
        ("average_order_value_brl", "numeric(18,2)"))),
    "category_daily": ("mart_category_daily", (
        ("purchase_date", "date"), ("category_label", "text"), ("category_mapping_missing", "boolean"),
        ("item_rows", "bigint"), ("distinct_orders", "bigint"), ("merchandise_value_brl", "numeric(18,2)"),
        ("freight_value_brl", "numeric(18,2)"))),
    "delivery_cohorts": ("mart_delivery_cohorts", (
        ("purchase_month", "date"), ("customer_state", "text"), ("delivered_orders", "bigint"),
        ("delivered_with_both_dates", "bigint"), ("on_time_orders", "bigint"),
        ("missing_date_orders", "bigint"), ("on_time_rate", "numeric"),
        ("negative_lead_time_orders", "bigint"), ("lead_time_p50_seconds", "double precision"),
        ("lead_time_p90_seconds", "double precision"))),
    "pipeline_health": ("mart_pipeline_health", (
        ("release_id", "uuid"), ("source_name", "text"), ("replay_version", "text"),
        ("frontier_end", "timestamp with time zone"), ("latest_rows", "bigint"),
        ("blocking_failures", "integer"), ("warnings", "integer"))),
}
VIEW_ORDER: tuple[str, ...] = tuple(SERVING_VIEWS)
RELEASE_INFO_COLUMNS = ("release_id", "status", "replay_version", "frontier_end", "code_sha",
                        "published_at", "generation", "serving_contract_version")


def safe_schema(name: str) -> sql.Identifier:
    if not SCHEMA_RE.match(name):
        raise ValueError(f"refusing schema name {name!r}")
    return sql.Identifier(name)


def candidate_columns(conn: psycopg.Connection, schema: str, table: str) -> list[Column]:
    rows = conn.execute(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum", (schema, table)).fetchall()
    return [(r[0], r[1]) for r in rows]


def contract_problems(conn: psycopg.Connection, schema: str) -> list[str]:
    safe_schema(schema)
    problems = []
    for view, (table, expected) in SERVING_VIEWS.items():
        got = candidate_columns(conn, schema, table)
        if got != list(expected):
            problems.append(f"{view} <- {table}: expected {list(expected)}, found {got}")
    return problems


def view_ddl(schema: str, view: str) -> sql.Composed:
    table, cols = SERVING_VIEWS[view]
    return sql.SQL("CREATE OR REPLACE VIEW serving.{} AS SELECT {} FROM {}.{}").format(
        sql.Identifier(view), sql.SQL(", ").join(sql.Identifier(c) for c, _ in cols),
        safe_schema(schema), sql.Identifier(table))


def release_info_ddl(*, release_id, status: str, replay_version: str, frontier_end, code_sha: str,
                     published_at, generation: int) -> sql.Composed:
    return sql.SQL(
        "CREATE OR REPLACE VIEW serving.release_info AS SELECT {rid}::uuid AS release_id, "
        "{st}::text AS status, {rv}::text AS replay_version, {fe}::timestamptz AS frontier_end, "
        "{cs}::text AS code_sha, {pa}::timestamptz AS published_at, {gen}::bigint AS generation, "
        "{ver}::integer AS serving_contract_version").format(
        rid=sql.Literal(str(release_id)), st=sql.Literal(status), rv=sql.Literal(replay_version),
        fe=sql.Literal(frontier_end), cs=sql.Literal(code_sha), pa=sql.Literal(published_at),
        gen=sql.Literal(generation), ver=sql.Literal(SERVING_CONTRACT_VERSION))
