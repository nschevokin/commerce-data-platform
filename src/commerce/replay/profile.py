from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from decimal import Decimal

from commerce.contracts.canonical import SERIALIZER_VERSION
from commerce.contracts.money import MoneyFormatError, parse_money
from commerce.contracts.orders import ORDER_STATUSES_V1
from commerce.contracts.sources import CORE_SOURCES, SPEC_BY_NAME
from commerce.replay.dataset import DatasetSource, load_core_rows

PROFILE_VERSION = 1
_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _money(text: str) -> Decimal | None:
    try:
        return parse_money(text)
    except MoneyFormatError:
        return None


def _is_blank(value: str) -> bool:
    return not value.strip()


def profile_dataset(source: DatasetSource) -> dict:
    data = load_core_rows(source)
    rows = {name: [r for _, _, r in items] for name, items in data.items()}
    out: dict = {
        "profile_version": PROFILE_VERSION, "serializer_version": SERIALIZER_VERSION,
        "dataset_sha256": source.digest(), "sources": {}, "checks": {},
    }
    for spec in CORE_SOURCES:
        rs = rows[spec.name]
        out["sources"][spec.name] = {
            "row_count": len(rs),
            "null_counts": {c: sum(_is_blank(r[c]) for r in rs) for c in spec.columns},
        }

    orders = rows["orders"]
    order_ids = {r["order_id"].strip() for r in orders}
    status = Counter(r["order_status"].strip().lower() for r in orders)
    delivered = [r for r in orders if r["order_status"].strip().lower() == "delivered"]
    non_delivered = [r for r in orders if r["order_status"].strip().lower() != "delivered"]
    bad_purchase = sum(not _TS.match(r["order_purchase_timestamp"].strip()) for r in orders)
    purchases = sorted(r["order_purchase_timestamp"].strip() for r in orders
                       if _TS.match(r["order_purchase_timestamp"].strip()))
    negative_lead = 0
    for r in delivered:
        a, p = r["order_delivered_customer_date"].strip(), r["order_purchase_timestamp"].strip()
        if _TS.match(a) and _TS.match(p):
            fmt = "%Y-%m-%d %H:%M:%S"
            negative_lead += datetime.strptime(a, fmt) < datetime.strptime(p, fmt)
    out["checks"]["orders"] = {
        "status_histogram": dict(sorted(status.items())),
        "statuses_outside_contract_v1": sorted(set(status) - set(ORDER_STATUSES_V1)),
        "malformed_purchase_timestamps": bad_purchase,
        "purchase_min": purchases[0] if purchases else None,
        "purchase_max": purchases[-1] if purchases else None,
        "delivered_status_with_actual_date": sum(not _is_blank(r["order_delivered_customer_date"]) for r in delivered),
        "delivered_status_missing_actual_date": sum(_is_blank(r["order_delivered_customer_date"]) for r in delivered),
        "non_delivered_status_with_actual_date": sum(not _is_blank(r["order_delivered_customer_date"]) for r in non_delivered),
        "delivered_with_negative_lead_time": negative_lead,
    }

    items, payments = rows["order_items"], rows["order_payments"]
    price_bad = sum(_money(r["price"]) is None for r in items)
    freight_bad = sum(_money(r["freight_value"]) is None for r in items)
    pay_bad = sum(_money(r["payment_value"]) is None for r in payments)
    out["checks"]["order_items"] = {
        "malformed_price": price_bad, "malformed_freight": freight_bad,
        "negative_price": sum((m := _money(r["price"])) is not None and m < 0 for r in items),
        "negative_freight": sum((m := _money(r["freight_value"])) is not None and m < 0 for r in items),
    }
    inst = [int(r["payment_installments"]) for r in payments
            if re.fullmatch(r"-?\d{1,9}", r["payment_installments"].strip())]
    out["checks"]["order_payments"] = {
        "type_histogram": dict(sorted(Counter(r["payment_type"].strip() for r in payments).items())),
        "malformed_payment_value": pay_bad,
        "negative_payment_value": sum((m := _money(r["payment_value"])) is not None and m < 0 for r in payments),
        "zero_payment_value": sum((m := _money(r["payment_value"])) == 0 for r in payments),
        "installments_min": min(inst) if inst else None,
        "installments_max": max(inst) if inst else None,
        "installments_nonpositive": sum(i <= 0 for i in inst),
    }

    customer_ids = {r["customer_id"].strip() for r in rows["customers"]}
    referenced = {r["customer_id"].strip() for r in orders}
    product_ids = {r["product_id"].strip() for r in rows["products"]}
    seller_ids = {r["seller_id"].strip() for r in rows["sellers"]}
    translated = {r["product_category_name"].strip() for r in rows["category_translation"]}
    categories = {r["product_category_name"].strip() for r in rows["products"] if not _is_blank(r["product_category_name"])}
    out["checks"]["relationships"] = {
        "items_without_order": sum(r["order_id"].strip() not in order_ids for r in items),
        "payments_without_order": sum(r["order_id"].strip() not in order_ids for r in payments),
        "orders_without_customer": sum(r["customer_id"].strip() not in customer_ids for r in orders),
        "customers_without_order": sum(r["customer_id"].strip() not in referenced for r in rows["customers"]),
        "customers_shared_by_orders": len(orders) - len(referenced),
        "items_without_product": sum(r["product_id"].strip() not in product_ids for r in items),
        "items_without_seller": sum(r["seller_id"].strip() not in seller_ids for r in items),
        "orders_without_items": len(order_ids - {r["order_id"].strip() for r in items}),
        "orders_without_payments": len(order_ids - {r["order_id"].strip() for r in payments}),
        "products_with_blank_category": sum(_is_blank(r["product_category_name"]) for r in rows["products"]),
        "categories_without_translation": sorted(categories - translated),
        "distinct_customer_unique_ids": len({r["customer_unique_id"].strip() for r in rows["customers"]}),
    }

    item_tot: dict[str, Decimal] = {}
    pay_tot: dict[str, Decimal] = {}
    for r in items:
        a, b = _money(r["price"]), _money(r["freight_value"])
        if a is not None and b is not None:
            item_tot[r["order_id"].strip()] = item_tot.get(r["order_id"].strip(), Decimal(0)) + a + b
    for r in payments:
        v = _money(r["payment_value"])
        if v is not None:
            pay_tot[r["order_id"].strip()] = pay_tot.get(r["order_id"].strip(), Decimal(0)) + v
    both = set(item_tot) & set(pay_tot)
    eligible_ids = {r["order_id"].strip() for r in orders
                    if ORDER_STATUSES_V1.get(r["order_status"].strip().lower(), False)}
    gaps = {o for o in both if item_tot[o] != pay_tot[o]}
    out["checks"]["payment_vs_items"] = {
        "orders_with_both_sides": len(both),
        "orders_with_gap": len(gaps),
        "eligible_orders_with_gap": len(gaps & eligible_ids),
        "largest_abs_gap_brl": str(max((abs(item_tot[o] - pay_tot[o]) for o in gaps), default=Decimal(0))),
    }
    assert set(out["sources"]) == set(SPEC_BY_NAME)
    return out
