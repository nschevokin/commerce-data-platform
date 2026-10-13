from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from commerce.contracts.money import CENT, fmt_money, parse_money
from commerce.contracts.orders import is_eligible

TS = "%Y-%m-%d %H:%M:%S"
UNKNOWN_CATEGORY = "(unknown)"


def _rows(d: Path, name: str) -> list[dict[str, str]]:
    with (d / name).open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _ts(s: str) -> datetime | None:
    s = (s or "").strip()
    return datetime.strptime(s, TS) if s else None


def _aov(merch: Decimal, n: int) -> str | None:
    return fmt_money((merch / n).quantize(CENT, rounding=ROUND_HALF_UP)) if n else None


def compute_from_dir(fixture_dir: str | Path) -> dict:
    d = Path(fixture_dir)
    orders = _rows(d, "olist_orders_dataset.csv")
    items = _rows(d, "olist_order_items_dataset.csv")
    payments = _rows(d, "olist_order_payments_dataset.csv")
    customers = {r["customer_id"].strip() for r in _rows(d, "olist_customers_dataset.csv")}
    products = {r["product_id"]: r for r in _rows(d, "olist_products_dataset.csv")}
    translation = {
        r["product_category_name"]: r["product_category_name_english"]
        for r in _rows(d, "product_category_name_translation.csv")
    }

    item_sum: dict[str, dict] = defaultdict(lambda: {"n": 0, "price": Decimal(0), "freight": Decimal(0)})
    for it in items:
        acc = item_sum[it["order_id"].strip()]
        acc["n"] += 1
        acc["price"] += parse_money(it["price"])
        acc["freight"] += parse_money(it["freight_value"])
    pay_sum: dict[str, dict] = defaultdict(lambda: {"n": 0, "value": Decimal(0)})
    for p in payments:
        acc = pay_sum[p["order_id"].strip()]
        acc["n"] += 1
        acc["value"] += parse_money(p["payment_value"])

    per_order: dict[str, dict] = {}
    daily: dict[str, dict] = defaultdict(
        lambda: {"eligible": 0, "excluded": 0, "with_items": 0,
                 "merch": Decimal(0), "freight": Decimal(0), "pay": Decimal(0)}
    )
    elig = {"orders": 0, "excluded": 0, "with_items": 0,
            "merch": Decimal(0), "freight": Decimal(0), "pay": Decimal(0)}
    allst = {"orders": 0, "merch": Decimal(0), "freight": Decimal(0), "pay": Decimal(0)}
    delivered = both_dates = on_time = 0

    for o in orders:
        oid = o["order_id"].strip()
        status = o["order_status"].strip()
        eligible = is_eligible(status)
        purchased = _ts(o["order_purchase_timestamp"])
        if purchased is None:
            raise ValueError(f"{oid}: order_purchase_timestamp is required")
        actual = _ts(o["order_delivered_customer_date"])
        estimate = _ts(o["order_estimated_delivery_date"])
        i, p = item_sum.get(oid), pay_sum.get(oid)
        day = purchased.date().isoformat()

        is_on_time = None
        lead = None
        if status == "delivered" and eligible:
            delivered += 1
            if actual and estimate:
                both_dates += 1
                is_on_time = actual.date() <= estimate.date()
                on_time += int(is_on_time)
            if actual:
                lead = int((actual - purchased).total_seconds())

        per_order[oid] = {
            "status": status, "is_eligible": eligible, "purchase_date": day,
            "item_count": i["n"] if i else 0, "payment_row_count": p["n"] if p else 0,
            "merchandise_value_brl": fmt_money(i["price"]) if i else None,
            "freight_value_brl": fmt_money(i["freight"]) if i else None,
            "item_total_brl": fmt_money(i["price"] + i["freight"]) if i else None,
            "recorded_payment_brl": fmt_money(p["value"]) if p else None,
            "payment_difference_brl": (
                fmt_money(p["value"] - (i["price"] + i["freight"])) if i and p else None
            ),
            "has_items": i is not None, "has_payments": p is not None,
            "has_customer": o["customer_id"].strip() in customers,
            "has_delivery_actual": actual is not None,
            "has_delivery_estimate": estimate is not None,
            "is_on_time": is_on_time, "delivery_lead_time_seconds": lead,
        }

        allst["orders"] += 1
        if i:
            allst["merch"] += i["price"]
            allst["freight"] += i["freight"]
        if p:
            allst["pay"] += p["value"]
        dd = daily[day]
        if not eligible:
            elig["excluded"] += 1
            dd["excluded"] += 1
            continue
        for bucket in (elig, dd):
            bucket["eligible" if bucket is dd else "orders"] += 1
            if i:
                bucket["with_items"] += 1
                bucket["merch"] += i["price"]
                bucket["freight"] += i["freight"]
            if p:
                bucket["pay"] += p["value"]

    eligible_ids = {oid for oid, v in per_order.items() if v["is_eligible"]}
    category: dict[str, dict] = defaultdict(
        lambda: {"item_rows": 0, "orders": set(), "merch": Decimal(0), "freight": Decimal(0),
                 "mapping_missing": False}
    )
    for it in items:
        oid = it["order_id"].strip()
        if oid not in eligible_ids:
            continue
        original = (products[it["product_id"]]["product_category_name"] or "").strip()
        english = translation.get(original) if original else None
        label = english or original or UNKNOWN_CATEGORY
        c = category[label]
        c["item_rows"] += 1
        c["orders"].add(oid)
        c["merch"] += parse_money(it["price"])
        c["freight"] += parse_money(it["freight_value"])
        c["mapping_missing"] = english is None

    return {
        "all_status": {
            "orders": allst["orders"], "merchandise_value_brl": fmt_money(allst["merch"]),
            "freight_value_brl": fmt_money(allst["freight"]),
            "recorded_payment_brl": fmt_money(allst["pay"]),
        },
        "eligible": {
            "eligible_orders": elig["orders"], "excluded_orders": elig["excluded"],
            "eligible_orders_with_items": elig["with_items"],
            "merchandise_value_brl": fmt_money(elig["merch"]),
            "freight_value_brl": fmt_money(elig["freight"]),
            "recorded_payment_brl": fmt_money(elig["pay"]),
            "average_order_value_brl": _aov(elig["merch"], elig["with_items"]),
        },
        "orders": per_order,
        "sales_daily": {
            day: {
                "eligible_orders": v["eligible"], "excluded_orders": v["excluded"],
                "merchandise_value_brl": fmt_money(v["merch"]),
                "freight_value_brl": fmt_money(v["freight"]),
                "recorded_payment_brl": fmt_money(v["pay"]),
                "average_order_value_brl": _aov(v["merch"], v["with_items"]),
            }
            for day, v in sorted(daily.items())
        },
        "delivery": {
            "delivered_orders": delivered, "delivered_with_both_dates": both_dates,
            "on_time_orders": on_time,
            "on_time_rate": (
                str((Decimal(on_time) / both_dates).quantize(Decimal("0.000001")))
                if both_dates else None
            ),
        },
        "category": {
            k: {"item_rows": v["item_rows"], "distinct_orders": len(v["orders"]),
                "merchandise_value_brl": fmt_money(v["merch"]),
                "freight_value_brl": fmt_money(v["freight"]),
                "mapping_missing": v["mapping_missing"]}
            for k, v in sorted(category.items())
        },
    }
