from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import psycopg.conninfo
import pytest

pytest.importorskip("streamlit", reason="the dashboard environment is not installed here")
import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

from commerce.release.contract import SERVING_VIEWS, release_info_ddl, view_ddl  # noqa: E402

pytestmark = pytest.mark.dashboard
APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")
RELEASE = uuid.UUID("11111111-2222-3333-4444-555555555555")
VALUES = {
    "mart_sales_daily": [(date(2017, 3, 10), 1, 0, 1, 0, 0, Decimal("100.00"), Decimal("10.00"),
                          Decimal("110.00"), Decimal("100.00")),
                         (date(2017, 3, 11), 3, 1, 1, 2, 1, Decimal("30.00"), Decimal("3.00"),
                          Decimal("0.00"), Decimal("30.00"))],
    "mart_category_daily": [(date(2017, 3, 10), "toys", False, 2, 1, Decimal("100.00"), Decimal("10.00")),
                            (date(2017, 3, 11), "unmapped", True, 1, 1, Decimal("30.00"), Decimal("3.00"))],
    "mart_delivery_cohorts": [(date(2017, 3, 1), "SP", 2, 2, 1, 0, Decimal("0.5"), 0, 100.0, 200.0)],
    "mart_pipeline_health": [(RELEASE, "orders", "baseline-v1", datetime(2017, 4, 1, tzinfo=UTC), 3, 0, 1)],
}


def build_release(admin_dsn: str) -> None:
    schema = "cand_" + uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as c:
        c.execute(f"CREATE SCHEMA {schema}")
        for view, (table, cols) in SERVING_VIEWS.items():
            c.execute(f"CREATE TABLE {schema}.{table} ({', '.join(f'{n} {t}' for n, t in cols)})")
            marks = ", ".join(["%s"] * len(cols))
            for row in VALUES[table]:
                c.execute(f"INSERT INTO {schema}.{table} VALUES ({marks})", row)
            c.execute(view_ddl(schema, view))
        c.execute(release_info_ddl(release_id=RELEASE, status="PUBLISHED", replay_version="baseline-v1",
                                   frontier_end=datetime(2017, 4, 1, tzinfo=UTC), code_sha="abc",
                                   published_at=datetime(2026, 10, 12, tzinfo=UTC), generation=1))
        c.execute("GRANT USAGE ON SCHEMA serving TO commerce_dashboard")
        c.execute("GRANT SELECT ON ALL TABLES IN SCHEMA serving TO commerce_dashboard")


@pytest.fixture
def run_app(monkeypatch):
    def run(dsn: str) -> AppTest:
        st.cache_resource.clear()
        monkeypatch.setenv("COMMERCE_DASHBOARD_DSN", dsn)
        return AppTest.from_file(APP, default_timeout=60).run()
    return run


def text_of(app: AppTest) -> str:
    return " ".join([m.value for m in app.markdown] + [c.value for c in app.caption]
                    + [w.value for w in app.warning] + [e.value for e in app.error])


def test_no_release_yet_says_so_and_stops(own_warehouse, own_role_dsn, run_app):
    app = run_app(own_role_dsn("commerce_dashboard"))
    assert not app.exception
    assert any("No release has been published" in w.value for w in app.warning)
    assert len(app.tabs) == 0


def test_published_release_shows_exact_figures_labels_and_coverage(own_warehouse, own_role_dsn, run_app):
    build_release(own_warehouse["admin_dsn"])
    app = run_app(own_role_dsn("commerce_dashboard"))
    assert not app.exception, [e.value for e in app.exception]
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Eligible orders"] == "4"
    assert metrics["Merchandise value"] == "BRL 130.00"
    assert metrics["Shipping charges"] == "BRL 13.00"
    assert metrics["Average order value"] == "BRL 65.00"
    assert metrics["On-time rate"] == "50.0 %"
    body = text_of(app)
    for must in ("Historical Olist replay", "Not live orders", "BRL", "11111111", "PUBLISHED",
                 "2 eligible orders have no items, 1 have no recorded payment", "1 orders are excluded",
                 "Recorded payment value BRL 110.00", "must not be added across categories",
                 "Delivery tab only"):
        assert must.lower() in body.lower(), must
    assert [t.label for t in app.tabs] == ["Overview", "Categories", "Delivery", "Pipeline evidence"]


def test_state_filter_changes_the_delivery_denominator(own_warehouse, own_role_dsn, run_app):
    app = run_app(own_role_dsn("commerce_dashboard"))
    app.multiselect[0].set_value(["SP"]).run()
    assert not app.exception
    assert {m.label: m.value for m in app.metric}["On-time rate"] == "50.0 %"
    app.multiselect[0].set_value([]).run()
    assert {m.label: m.value for m in app.metric}["Delivered orders"] == "2"


def test_unreachable_database_is_an_explicit_error_not_an_empty_page(own_warehouse, own_role_dsn, run_app):
    bad = psycopg.conninfo.make_conninfo(own_role_dsn("commerce_dashboard"), port="1", connect_timeout="2")
    app = run_app(bad)
    assert not app.exception
    assert any("Data unavailable" in e.value for e in app.error)
    assert len(app.metric) == 0
