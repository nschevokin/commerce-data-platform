from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

CENT = Decimal("0.01")
Rows = list[dict[str, Any]]


def brl(value: Decimal | None) -> str:
    return "n/a" if value is None else f"BRL {value.quantize(CENT, ROUND_HALF_UP):,.2f}"


def pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return "n/a"
    return f"{(Decimal(numerator) * 100 / Decimal(denominator)).quantize(Decimal('0.1'), ROUND_HALF_UP)} %"


def date_bounds(sales: Rows) -> tuple[date, date] | None:
    days = [r["purchase_date"] for r in sales]
    return (min(days), max(days)) if days else None


@dataclass(frozen=True)
class Overview:
    days: int
    eligible_orders: int
    excluded_orders: int
    with_items: int
    missing_items: int
    missing_payments: int
    merchandise: Decimal
    freight: Decimal
    recorded_payment: Decimal
    aov: Decimal | None


def overview(sales: Rows, start: date, end: date) -> Overview:
    rows = [r for r in sales if start <= r["purchase_date"] <= end]
    merch = sum((r["merchandise_value_brl"] for r in rows), Decimal(0))
    with_items = sum(r["eligible_orders_with_items"] for r in rows)
    aov = (merch / with_items).quantize(CENT, ROUND_HALF_UP) if with_items else None
    return Overview(
        days=len(rows), eligible_orders=sum(r["eligible_orders"] for r in rows),
        excluded_orders=sum(r["excluded_orders"] for r in rows), with_items=with_items,
        missing_items=sum(r["eligible_orders_missing_items"] for r in rows),
        missing_payments=sum(r["eligible_orders_missing_payments"] for r in rows),
        merchandise=merch, freight=sum((r["freight_value_brl"] for r in rows), Decimal(0)),
        recorded_payment=sum((r["recorded_payment_brl"] for r in rows), Decimal(0)), aov=aov)


def daily_series(sales: Rows, start: date, end: date) -> Rows:
    return sorted((r for r in sales if start <= r["purchase_date"] <= end), key=lambda r: r["purchase_date"])


def category_ranking(categories: Rows, start: date, end: date, top: int | None = None) -> Rows:
    acc: dict[tuple[str, bool], dict[str, Any]] = {}
    for r in categories:
        if not start <= r["purchase_date"] <= end:
            continue
        a = acc.setdefault((r["category_label"], r["category_mapping_missing"]), {
            "category_label": r["category_label"], "category_mapping_missing": r["category_mapping_missing"],
            "item_rows": 0, "distinct_orders": 0, "merchandise_value_brl": Decimal(0),
            "freight_value_brl": Decimal(0)})
        a["item_rows"] += r["item_rows"]
        a["distinct_orders"] += r["distinct_orders"]
        a["merchandise_value_brl"] += r["merchandise_value_brl"]
        a["freight_value_brl"] += r["freight_value_brl"]
    ranked = sorted(acc.values(), key=lambda a: (-a["merchandise_value_brl"], a["category_label"]))
    return ranked[:top] if top else ranked


def delivery_states(cohorts: Rows) -> list[str]:
    return sorted({r["customer_state"] for r in cohorts if r["customer_state"] is not None})


@dataclass(frozen=True)
class Delivery:
    delivered: int
    with_both_dates: int
    on_time: int
    missing_date: int
    negative_lead_time: int

    @property
    def rate(self) -> str:
        return pct(self.on_time, self.with_both_dates)


def delivery(cohorts: Rows, states: list[str] | None = None, month_from: date | None = None,
             month_to: date | None = None) -> tuple[Delivery, Rows]:
    rows = [r for r in cohorts
            if (not states or r["customer_state"] in states)
            and (month_from is None or r["purchase_month"] >= month_from)
            and (month_to is None or r["purchase_month"] <= month_to)]
    total = Delivery(
        delivered=sum(r["delivered_orders"] for r in rows),
        with_both_dates=sum(r["delivered_with_both_dates"] for r in rows),
        on_time=sum(r["on_time_orders"] for r in rows),
        missing_date=sum(r["missing_date_orders"] for r in rows),
        negative_lead_time=sum(r["negative_lead_time_orders"] for r in rows))
    return total, sorted(rows, key=lambda r: (r["purchase_month"], r["customer_state"] or ""))


def to_csv(rows: Rows) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({k: ("" if v is None else str(v)) for k, v in r.items()})
    return buf.getvalue()


def health_summary(health: Rows) -> Rows:
    return sorted(health, key=lambda r: r["source_name"])
