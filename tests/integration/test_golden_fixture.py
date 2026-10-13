from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from commerce.fixture.harness import run_slice

pytestmark = [pytest.mark.integration, pytest.mark.dbt]

REPO = Path(__file__).resolve().parents[2]
GOLDEN = REPO / "tests" / "fixtures" / "golden_v1"


def _slice(warehouse, transform_env, tmp_path, fixture_dir=GOLDEN, project_dir=None):
    kw = {"project_dir": project_dir} if project_dir else {}
    return run_slice(admin_dsn=warehouse["admin_dsn"], transform_env=transform_env,
                     fixture_dir=fixture_dir, artifacts_root=tmp_path / "builds",
                     raise_on_build_failure=False, **kw)


def test_golden_fixture_exact_answers(warehouse, transform_env, tmp_path):
    r = _slice(warehouse, transform_env, tmp_path)
    assert r["dbt"]["failures"] == []
    assert r["dbt"]["executed"] == r["dbt"]["expected"]
    assert r["sql_vs_oracle_diffs"] == []
    assert r["python_reference_vs_oracle_diffs"] == []
    e = r["observed"]["eligible"]
    assert (e["eligible_orders"], e["merchandise_value_brl"], e["freight_value_brl"],
            e["recorded_payment_brl"], e["average_order_value_brl"]) == (
        2, "130.00", "13.00", "143.00", "65.00")
    assert r["observed"]["delivery"]["on_time_rate"] == "0.500000"
    assert r["observed"]["orders"]["fx0000000000000000000000000order_c"]["is_eligible"] is False
    assert r["observed"]["all_status"]["orders"] == 3
    assert r["passed"]


FAN_OUT_FCT_ORDERS = """
-- MUTANT: the classic bug. Raw items joined to raw payments, then summed.
select
  o.order_id, o.customer_id, o.order_status, s.is_eligible,
  s.contract_version as status_contract_version, o.purchased_at,
  o.purchased_at::date as purchase_date, o.approved_at, o.delivered_carrier_at,
  o.delivered_customer_at, o.estimated_delivery_at,
  count(i.order_id) as item_count, count(p.order_id) as payment_row_count,
  sum(i.price_brl) as merchandise_value_brl, sum(i.freight_brl) as freight_value_brl,
  sum(i.price_brl) + sum(i.freight_brl) as item_total_brl,
  sum(p.recorded_payment_brl) as recorded_payment_brl,
  sum(p.recorded_payment_brl) - (sum(i.price_brl) + sum(i.freight_brl)) as payment_difference_brl,
  bool_or(i.order_id is not null) as has_items, bool_or(p.order_id is not null) as has_payments,
  true as has_customer,
  o.delivered_customer_at is not null as has_delivery_actual,
  o.estimated_delivery_at is not null as has_delivery_estimate,
  null::boolean as is_on_time, null::bigint as delivery_lead_time_seconds
from {{ ref('stg_orders') }} o
join {{ ref('order_status_contract') }} s using (order_status)
left join {{ ref('fct_order_items') }} i using (order_id)
left join {{ ref('fct_payments') }} p using (order_id)
group by 1,2,3,4,5,6,7,8,9,10,11
"""


def test_item_by_payment_fan_out_is_caught(warehouse, transform_env, tmp_path):
    project = tmp_path / "dbt_mutant"
    shutil.copytree(REPO / "dbt", project, ignore=shutil.ignore_patterns("target", "logs"))
    (project / "models" / "facts" / "fct_orders.sql").write_text(FAN_OUT_FCT_ORDERS)
    r = _slice(warehouse, transform_env, tmp_path, project_dir=project)
    assert not r["passed"]
    failed = " ".join(r["dbt"]["failures"])
    assert "assert_order_merchandise_reconciles_to_items" in failed
    assert "assert_order_payments_reconcile_to_payment_rows" in failed


def _broken_variant(tmp_path: Path, csv_name: str, old: str, new: str) -> Path:
    d = tmp_path / "fixture_variant"
    shutil.copytree(GOLDEN, d)
    p = d / csv_name
    text = p.read_text()
    assert old in text, f"variant anchor not found in {csv_name}"
    p.write_text(text.replace(old, new, 1))
    return d


@pytest.mark.parametrize("name,csv_name,old,new,expect", [
    ("malformed_money", "olist_order_items_dataset.csv", "60.00,5.00", "60.005,5.00",
     "malformed money value"),
    ("unknown_status", "olist_orders_dataset.csv", '"canceled"', '"returned"',
     "relationships_stg_orders_order_status"),
    ("missing_parent_order", "olist_order_items_dataset.csv",
     '"fx0000000000000000000000000order_b",1', '"fx0000000000000000000000000order_x",1',
     "relationships_fct_order_items_order_id"),
    ("null_natural_key", "olist_order_payments_dataset.csv",
     '"fx0000000000000000000000000order_b",1,"boleto"', ',1,"boleto"',
     "not_null_stg_order_payments_order_id"),
    ("malformed_timestamp", "olist_orders_dataset.csv", '"2017-03-10 14:20:00"',
     '"10/03/2017 14:20"', "malformed timestamp value"),
])
def test_broken_input_blocks(warehouse, transform_env, tmp_path, name, csv_name, old, new, expect):
    variant = _broken_variant(tmp_path, csv_name, old, new)
    r = _slice(warehouse, transform_env, tmp_path, fixture_dir=variant)
    assert not r["passed"], name
    log = (tmp_path / "builds" / r["candidate_schema"] / "dbt_stdout.log").read_text()
    assert expect in " ".join(r["dbt"]["failures"]) + log, name
