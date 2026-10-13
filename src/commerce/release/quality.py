from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import psycopg
from psycopg import sql

from commerce.release.contract import contract_problems, safe_schema

BLOCK, WARN = "BLOCK", "WARN"
SOURCES = {"orders": "stg_orders", "customers": "stg_customers", "order_items": "stg_order_items",
           "order_payments": "stg_order_payments", "products": "stg_products", "sellers": "stg_sellers",
           "category_translation": "stg_category_translation"}


@dataclass
class CheckResult:
    check_id: str
    severity: str
    passed: bool
    observed: Decimal | None = None
    threshold: Decimal | None = None
    detail: dict = field(default_factory=dict)


def _q(conn: psycopg.Connection, schema: str, text: str, *params):
    s = safe_schema(schema)
    return conn.execute(sql.SQL(text).format(s=s), params).fetchone()


def evaluate(conn: psycopg.Connection, *, schema: str, replay_version: str, frontier_end,
             dbt_report, exact_fixture: bool) -> list[CheckResult]:
    out: list[CheckResult] = []
    out.append(CheckResult("dbt_build_complete", BLOCK, dbt_report.ok, Decimal(dbt_report.executed),
                           Decimal(dbt_report.expected),
                           {"failures": dbt_report.failures[:20], "warnings": dbt_report.warnings[:20]}))
    problems = contract_problems(conn, schema)
    out.append(CheckResult("serving_contract", BLOCK, not problems, Decimal(len(problems)), Decimal(0),
                           {"problems": problems}))
    if problems:
        return out

    n_orders, n_stg, distinct_ids = _q(
        conn, schema, "SELECT (SELECT count(*) FROM {s}.fct_orders), (SELECT count(*) FROM {s}.stg_orders), "
                      "(SELECT count(DISTINCT order_id) FROM {s}.fct_orders)")
    out.append(CheckResult("fct_orders_one_row_per_source_order", BLOCK,
                           n_orders == n_stg == distinct_ids, Decimal(n_orders), Decimal(n_stg)))

    missing = {}
    for source, stg in SOURCES.items():
        in_bronze = conn.execute(
            "SELECT count(DISTINCT entity_key) FROM bronze.source_events WHERE replay_version = %s "
            "AND source_name = %s AND available_at < %s", (replay_version, source, frontier_end)
        ).fetchone()[0]
        in_stg = _q(conn, schema, f"SELECT count(*) FROM {{s}}.{stg}")[0]
        if in_bronze != in_stg:
            missing[source] = {"bronze": in_bronze, "candidate": in_stg}
    out.append(CheckResult("bronze_rows_accounted", BLOCK, not missing, Decimal(len(missing)), Decimal(0),
                           {"mismatched_sources": missing}))

    mart, facts = _q(conn, schema,
                     "SELECT (SELECT coalesce(sum(merchandise_value_brl), 0) FROM {s}.mart_sales_daily), "
                     "(SELECT coalesce(sum(i.price_brl), 0) FROM {s}.fct_order_items i "
                     "JOIN {s}.fct_orders o USING (order_id) WHERE o.is_eligible)")
    out.append(CheckResult("sales_merchandise_reconciles", BLOCK, mart == facts, mart, facts))

    orders_pay, pay_rows = _q(conn, schema,
                              "SELECT (SELECT coalesce(sum(recorded_payment_brl), 0) FROM {s}.fct_orders), "
                              "(SELECT coalesce(sum(recorded_payment_brl), 0) FROM {s}.fct_payments)")
    out.append(CheckResult("payments_reconcile", BLOCK, orders_pay == pay_rows, orders_pay, pay_rows))

    gaps = _q(conn, schema, "SELECT count(*) FROM {s}.fct_orders WHERE has_items AND has_payments "
                            "AND payment_difference_brl <> 0")[0]
    out.append(CheckResult("payment_gap_exceptions", BLOCK if exact_fixture else WARN, gaps == 0,
                           Decimal(gaps), Decimal(0),
                           {"policy": "blocks in the exact fixture, advisory on the profiled archive"}))
    unmapped = _q(conn, schema, "SELECT count(*) FROM {s}.dim_products WHERE category_mapping_missing")[0]
    out.append(CheckResult("unmapped_categories", WARN, unmapped == 0, Decimal(unmapped), Decimal(0)))
    neg = _q(conn, schema, "SELECT count(*) FROM {s}.fct_orders WHERE delivery_lead_time_seconds < 0")[0]
    out.append(CheckResult("negative_delivery_durations", WARN, neg == 0, Decimal(neg), Decimal(0)))
    return out


def counts(results: list[CheckResult]) -> tuple[int, int]:
    return (sum(1 for r in results if r.severity == BLOCK and not r.passed),
            sum(1 for r in results if r.severity == WARN and not r.passed))
