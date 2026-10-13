from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql

from commerce import dbt_runner
from commerce.contracts.money import fmt_money

CSV_TO_SOURCE = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "category_translation",
}
_SCHEMA = re.compile(r"^(fixture|cand)_[0-9a-f]{32}$")


def _safe_schema(name: str) -> sql.Identifier:
    if not _SCHEMA.match(name):
        raise ValueError(f"refusing schema name {name!r}")
    return sql.Identifier(name)


def load_fixture_sources(admin_dsn: str, fixture_dir: Path, schema: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    ident = _safe_schema(schema)
    with psycopg.connect(admin_dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(ident))
        for csv_name, source in CSV_TO_SOURCE.items():
            path = fixture_dir / csv_name
            header = path.open(encoding="utf-8").readline().strip()
            cols = [c.strip().strip('"') for c in header.split(",")]
            tbl = sql.Identifier(schema, source)
            conn.execute(sql.SQL("CREATE TABLE {} ({})").format(
                tbl, sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in cols)))
            with conn.cursor().copy(sql.SQL(
                    "COPY {} FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')").format(tbl)
            ) as cp:
                cp.write(path.read_bytes())
            counts[source] = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(tbl)).fetchone()[0]
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO commerce_transform").format(ident))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO commerce_transform").format(ident))
    return counts


def _m(v: Decimal | None) -> str | None:
    return None if v is None else fmt_money(v)


def observe_candidate(dsn: str, schema: str) -> dict:
    s = _safe_schema(schema)
    with psycopg.connect(dsn) as conn:
        q = lambda text, *a: conn.execute(sql.SQL(text).format(s, *a))  # noqa: E731

        counts = {}
        for name, rel in [("orders", "stg_orders"), ("customers", "stg_customers"),
                          ("order_items", "stg_order_items"), ("order_payments", "stg_order_payments"),
                          ("products", "stg_products"), ("sellers", "stg_sellers"),
                          ("category_translation", "stg_category_translation")]:
            counts[name] = q("SELECT count(*) FROM {}.{}", sql.Identifier(rel)).fetchone()[0]

        allst = q("SELECT count(*), sum(merchandise_value_brl), sum(freight_value_brl), "
                  "sum(recorded_payment_brl) FROM {}.fct_orders").fetchone()
        e = q("SELECT sum(eligible_orders), sum(excluded_orders), sum(eligible_orders_with_items), "
              "sum(merchandise_value_brl), sum(freight_value_brl), sum(recorded_payment_brl), "
              "round(sum(merchandise_value_brl) / nullif(sum(eligible_orders_with_items), 0), 2) "
              "FROM {}.mart_sales_daily").fetchone()

        orders = {}
        for r in q("SELECT order_id, order_status, is_eligible, purchase_date, item_count, "
                   "payment_row_count, merchandise_value_brl, freight_value_brl, item_total_brl, "
                   "recorded_payment_brl, payment_difference_brl, has_items, has_payments, "
                   "has_customer, has_delivery_actual, has_delivery_estimate, is_on_time, "
                   "delivery_lead_time_seconds FROM {}.fct_orders ORDER BY order_id"):
            orders[r[0]] = {
                "status": r[1], "is_eligible": r[2], "purchase_date": r[3].isoformat(),
                "item_count": r[4], "payment_row_count": r[5],
                "merchandise_value_brl": _m(r[6]), "freight_value_brl": _m(r[7]),
                "item_total_brl": _m(r[8]), "recorded_payment_brl": _m(r[9]),
                "payment_difference_brl": _m(r[10]), "has_items": r[11], "has_payments": r[12],
                "has_customer": r[13], "has_delivery_actual": r[14],
                "has_delivery_estimate": r[15], "is_on_time": r[16],
                "delivery_lead_time_seconds": r[17],
            }

        daily = {
            r[0].isoformat(): {
                "eligible_orders": r[1], "excluded_orders": r[2],
                "merchandise_value_brl": _m(r[3]), "freight_value_brl": _m(r[4]),
                "recorded_payment_brl": _m(r[5]), "average_order_value_brl": _m(r[6]),
            }
            for r in q("SELECT purchase_date, eligible_orders, excluded_orders, merchandise_value_brl, "
                       "freight_value_brl, recorded_payment_brl, average_order_value_brl "
                       "FROM {}.mart_sales_daily ORDER BY purchase_date")
        }

        d = q("SELECT sum(delivered_orders), sum(delivered_with_both_dates), sum(on_time_orders) "
              "FROM {}.mart_delivery_cohorts").fetchone()
        rate = q("SELECT round(sum(on_time_orders)::numeric / nullif(sum(delivered_with_both_dates), 0), 6) "
                 "FROM {}.mart_delivery_cohorts").fetchone()[0]

        category = {
            r[0]: {"item_rows": r[1], "distinct_orders": r[2], "merchandise_value_brl": _m(r[3]),
                   "freight_value_brl": _m(r[4]), "mapping_missing": r[5]}
            for r in q("SELECT category_label, sum(item_rows)::int, sum(distinct_orders)::int, "
                       "sum(merchandise_value_brl), sum(freight_value_brl), bool_or(category_mapping_missing) "
                       "FROM {}.mart_category_daily GROUP BY category_label ORDER BY category_label")
        }

        dims = {
            "customer_postal_prefix": dict(q(
                "SELECT customer_id, postal_prefix FROM {}.dim_customers "
                "WHERE customer_id = 'fx00000000000000000000000000cust_a'").fetchall()),
            "seller_postal_prefix": dict(q(
                "SELECT seller_id, postal_prefix FROM {}.dim_sellers "
                "WHERE seller_id = 'fx0000000000000000000000000seller_2'").fetchall()),
            "seller_state": dict(q(
                "SELECT seller_id, state FROM {}.dim_sellers "
                "WHERE seller_id = 'fx0000000000000000000000000seller_2'").fetchall()),
            "distinct_customer_unique_ids": q(
                "SELECT count(DISTINCT customer_unique_id) FROM {}.dim_customers").fetchone()[0],
        }

    return {
        "row_counts": counts,
        "all_status": {"orders": allst[0], "merchandise_value_brl": _m(allst[1]),
                       "freight_value_brl": _m(allst[2]), "recorded_payment_brl": _m(allst[3])},
        "eligible": {"eligible_orders": int(e[0]), "excluded_orders": int(e[1]),
                     "eligible_orders_with_items": int(e[2]), "merchandise_value_brl": _m(e[3]),
                     "freight_value_brl": _m(e[4]), "recorded_payment_brl": _m(e[5]),
                     "average_order_value_brl": _m(e[6])},
        "orders": orders,
        "sales_daily": daily,
        "delivery": {"delivered_orders": int(d[0]), "delivered_with_both_dates": int(d[1]),
                     "on_time_orders": int(d[2]), "on_time_rate": None if rate is None else str(rate)},
        "category": category,
        "dimensions": dims,
    }


def diff(expected: dict, observed: dict, path: str = "") -> list[str]:
    out: list[str] = []
    if isinstance(expected, dict) and isinstance(observed, dict):
        for k in sorted(set(expected) | set(observed)):
            if k.startswith("_"):
                continue
            if k not in observed:
                out.append(f"{path}/{k}: missing in observed")
            elif k not in expected:
                out.append(f"{path}/{k}: unexpected in observed")
            else:
                out += diff(expected[k], observed[k], f"{path}/{k}")
    elif expected != observed:
        out.append(f"{path}: expected {expected!r}, observed {observed!r}")
    return out


def run_slice(*, admin_dsn: str, transform_env: dict[str, str], fixture_dir: Path,
              artifacts_root: Path, project_dir: Path = dbt_runner.DBT_PROJECT,
              raise_on_build_failure: bool = True) -> dict:
    run_id = str(uuid.uuid4())
    source_schema = f"fixture_{uuid.uuid4().hex}"
    candidate_schema = f"cand_{uuid.uuid4().hex}"
    target = artifacts_root / candidate_schema

    loaded = load_fixture_sources(admin_dsn, fixture_dir, source_schema)
    report = dbt_runner.build(candidate_schema=candidate_schema, source_schema=source_schema,
                              run_id=run_id, exact_fixture=True, target_path=target,
                              env=transform_env, project_dir=project_dir)
    if not report.ok:
        if raise_on_build_failure:
            raise dbt_runner.BuildFailed("; ".join(report.failures) or "dbt build failed", report)
        return {"passed": False, "candidate_schema": candidate_schema,
                "dbt": {"executed": report.executed, "expected": report.expected,
                        "warnings": report.warnings, "failures": report.failures}}

    expected = json.loads((fixture_dir / "expected.json").read_text())["baseline"]
    observed = observe_candidate(admin_dsn, candidate_schema)
    from commerce.fixture.reference import compute_from_dir
    reference = compute_from_dir(fixture_dir)

    result = {
        "run_id": run_id,
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "fixture": fixture_dir.name,
        "candidate_schema": candidate_schema,
        "source_rows_loaded": loaded,
        "dbt": {"executed": report.executed, "expected": report.expected,
                "warnings": report.warnings, "failures": report.failures},
        "sql_vs_oracle_diffs": diff(expected, observed),
        "python_reference_vs_oracle_diffs": diff(
            {k: expected[k] for k in reference}, reference),
        "observed": observed,
    }
    result["passed"] = (report.ok and not result["sql_vs_oracle_diffs"]
                        and not result["python_reference_vs_oracle_diffs"])
    return result
