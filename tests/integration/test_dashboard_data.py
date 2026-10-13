from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import psycopg.conninfo
import pytest
from replay_helpers import copy_fixture

from commerce.dashboard import data, metrics
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]
EXPECTED = json.loads(Path("tests/fixtures/golden_v1/expected.json").read_text())["baseline"]
STATE: dict = {}


@pytest.fixture(scope="module")
def published(own_warehouse, own_role_dsn, tmp_path_factory):
    root = tmp_path_factory.mktemp("dashboard")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    env = {"COMMERCE_WAREHOUSE_HOST": str(own_warehouse["host"]),
           "COMMERCE_WAREHOUSE_PORT": str(own_warehouse["port"]),
           "COMMERCE_WAREHOUSE_DB": own_warehouse["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
    w = Wiring(store_root=str(root / "store"), dataset_sha256=src.digest(), replay_version="baseline-v1",
               artifacts_root=root / "builds", dsn_for=own_role_dsn, dbt_env=env, exact_fixture=True)
    cal = w.reader().calendar()
    before = None
    for i, (a, b) in enumerate([(0, 7), (7, 14), (14, 21), (21, 28), (28, 31)]):
        out = run_pipeline(w, dag_run_id=f"dash-{i}", mode="manual", interval_start=cal[a].start,
                           interval_end=cal[b - 1].end)
        assert out["status"] in ("SUCCEEDED", "NO_CHANGE"), out
        before = out
    return {"dsn": own_role_dsn("commerce_dashboard"), "last": before, "db": own_warehouse}


def test_dashboard_numbers_equal_the_hand_calculated_golden_totals(published):
    b = data.fetch_bundle(published["dsn"])
    assert b is not None and b.release["status"] == "PUBLISHED"
    sales = b.tables["sales_daily"]
    lo, hi = metrics.date_bounds(sales)
    o = metrics.overview(sales, lo, hi)
    e = EXPECTED["eligible"]
    assert (o.eligible_orders, o.excluded_orders) == (e["eligible_orders"], e["excluded_orders"])
    assert o.merchandise == Decimal(e["merchandise_value_brl"])
    assert o.freight == Decimal(e["freight_value_brl"])
    assert o.recorded_payment == Decimal(e["recorded_payment_brl"])
    assert o.aov == Decimal(e["average_order_value_brl"])
    assert (o.missing_items, o.missing_payments) == (0, 0)
    STATE["release"] = b.release_id


def test_every_view_is_filled_and_belongs_to_the_one_release(published):
    b = data.fetch_bundle(published["dsn"])
    assert set(b.tables) == {"sales_daily", "category_daily", "delivery_cohorts", "pipeline_health"}
    assert all(b.tables[v] for v in b.tables), {v: len(r) for v, r in b.tables.items()}
    assert {str(r["release_id"]) for r in b.tables["pipeline_health"]} == {b.release_id}
    rank = metrics.category_ranking(b.tables["category_daily"], date(2000, 1, 1), date(2100, 1, 1))
    total = sum(r["merchandise_value_brl"] for r in rank)
    assert total == Decimal(EXPECTED["eligible"]["merchandise_value_brl"])
    d, rows = metrics.delivery(b.tables["delivery_cohorts"])
    assert d.delivered > 0 and rows
    assert b.read_seconds < 5


def test_outage_shows_stale_with_the_last_good_release_never_mixed_data(published):
    r = data.Refresher(published["dsn"])
    good, s = r.refresh()
    assert s.state == data.FRESH
    r.dsn = psycopg.conninfo.make_conninfo(published["dsn"], port="1")
    again, s2 = r.refresh()
    assert s2.state == data.STALE and again is good and s2.last_success_at == good.fetched_at
    fresh_start = data.Refresher(r.dsn)
    assert fresh_start.refresh()[1].state == data.UNAVAILABLE
