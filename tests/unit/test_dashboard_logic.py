from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg

from commerce.dashboard import data, metrics

D = Decimal


def day(d, orders=1, excluded=0, with_items=1, merch="0", freight="0", pay="0", mi=0, mp=0):
    return {"purchase_date": d, "eligible_orders": orders, "excluded_orders": excluded,
            "eligible_orders_with_items": with_items, "eligible_orders_missing_items": mi,
            "eligible_orders_missing_payments": mp, "merchandise_value_brl": D(merch),
            "freight_value_brl": D(freight), "recorded_payment_brl": D(pay),
            "average_order_value_brl": None}


SALES = [day(date(2017, 3, 10), orders=1, with_items=1, merch="100.00", freight="10.00", pay="110.00"),
         day(date(2017, 3, 11), orders=9, with_items=9, merch="30.01", freight="3.00", pay="33.00", mp=2),
         day(date(2017, 3, 12), orders=3, excluded=4, with_items=0, mi=3)]


def test_overview_sums_exactly_and_aov_is_not_an_average_of_averages():
    o = metrics.overview(SALES, date(2017, 3, 10), date(2017, 3, 12))
    assert o.merchandise == D("130.01") and o.freight == D("13.00") and o.recorded_payment == D("143.00")
    assert o.eligible_orders == 13 and o.with_items == 10 and o.excluded_orders == 4
    assert o.missing_items == 3 and o.missing_payments == 2
    assert o.aov == D("13.00")
    assert metrics.brl(o.merchandise) == "BRL 130.01"


def test_range_filter_and_empty_denominator():
    assert metrics.overview(SALES, date(2017, 3, 12), date(2017, 3, 12)).aov is None
    assert metrics.overview(SALES, date(2018, 1, 1), date(2018, 1, 2)).eligible_orders == 0
    assert metrics.brl(None) == "n/a"


def test_money_is_never_float():
    o = metrics.overview(SALES, date(2017, 3, 10), date(2017, 3, 12))
    assert all(isinstance(v, Decimal) for v in (o.merchandise, o.freight, o.recorded_payment, o.aov))
    assert metrics.brl(D("0.1") + D("0.2")) == "BRL 0.30"


def cat(d, label, items, orders, merch, missing=False):
    return {"purchase_date": d, "category_label": label, "category_mapping_missing": missing,
            "item_rows": items, "distinct_orders": orders, "merchandise_value_brl": D(merch),
            "freight_value_brl": D("1.00")}


def test_category_ranking_adds_within_a_category_and_orders_by_value():
    rows = [cat(date(2017, 3, 10), "toys", 2, 1, "50.00"), cat(date(2017, 3, 11), "toys", 1, 1, "20.00"),
            cat(date(2017, 3, 10), "books", 5, 2, "90.00"), cat(date(2017, 3, 10), "x", 1, 1, "5.00", True)]
    r = metrics.category_ranking(rows, date(2017, 3, 10), date(2017, 3, 11))
    assert [a["category_label"] for a in r] == ["books", "toys", "x"]
    toys = r[1]
    assert (toys["item_rows"], toys["distinct_orders"]) == (3, 2)
    assert toys["merchandise_value_brl"] == D("70.00")
    assert metrics.category_ranking(rows, date(2017, 3, 11), date(2017, 3, 11))[0]["category_label"] == "toys"
    assert len(metrics.category_ranking(rows, date(2017, 3, 10), date(2017, 3, 11), top=1)) == 1


def coh(m, state, delivered, both, on_time, missing=0, neg=0):
    return {"purchase_month": m, "customer_state": state, "delivered_orders": delivered,
            "delivered_with_both_dates": both, "on_time_orders": on_time, "missing_date_orders": missing,
            "negative_lead_time_orders": neg, "on_time_rate": None, "lead_time_p50_seconds": 1.0,
            "lead_time_p90_seconds": 2.0}


def test_delivery_rate_is_pooled_not_averaged():
    rows = [coh(date(2017, 3, 1), "SP", 100, 100, 90), coh(date(2017, 3, 1), "RJ", 10, 5, 0, missing=5)]
    total, _ = metrics.delivery(rows)
    assert (total.on_time, total.with_both_dates, total.missing_date) == (90, 105, 5)
    assert total.rate == "85.7 %"
    sp, only = metrics.delivery(rows, ["SP"])
    assert sp.rate == "90.0 %" and len(only) == 1
    assert metrics.delivery([], None)[0].rate == "n/a"
    assert metrics.delivery_states(rows) == ["RJ", "SP"]


def test_csv_export_keeps_exact_text_and_blank_nulls():
    text = metrics.to_csv([{"a": D("1.10"), "b": None, "c": date(2017, 3, 10)}])
    assert text == "a,b,c\n1.10,,2017-03-10\n"
    assert metrics.to_csv([]) == ""


def bundle(rid="r1"):
    return data.Bundle({"release_id": rid}, {}, datetime(2026, 1, 1, tzinfo=UTC), 0.01)


def test_refresher_reports_fresh_stale_unavailable_and_empty():
    calls = {"n": 0, "mode": "ok"}

    def fetch(dsn):
        calls["n"] += 1
        if calls["mode"] == "down":
            raise psycopg.OperationalError("connection refused\nsecond line")
        return None if calls["mode"] == "empty" else bundle()

    r = data.Refresher("dsn", fetch=fetch)
    calls["mode"] = "down"
    b, s = r.refresh()
    assert b is None and s.state == data.UNAVAILABLE and "connection refused" in s.error
    assert "second line" not in s.error
    calls["mode"] = "ok"
    b, s = r.refresh()
    assert s.state == data.FRESH and b.release_id == "r1"
    calls["mode"] = "down"
    b2, s2 = r.refresh()
    assert s2.state == data.STALE and b2 is b and s2.last_success_at == b.fetched_at
    calls["mode"] = "empty"
    assert r.refresh()[1].state == data.EMPTY


def test_refresh_if_due_reads_at_most_once_per_ttl():
    n = {"calls": 0}
    t = {"now": 100.0}

    def fetch(dsn):
        n["calls"] += 1
        return bundle(f"r{n['calls']}")

    r = data.Refresher("dsn", fetch=fetch)
    clock = lambda: t["now"]  # noqa: E731
    a = r.refresh_if_due(30, clock)
    t["now"] = 120.0
    assert r.refresh_if_due(30, clock) is a and n["calls"] == 1
    t["now"] = 131.0
    assert r.refresh_if_due(30, clock)[0].release_id == "r2" and n["calls"] == 2
