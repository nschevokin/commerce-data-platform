from __future__ import annotations

import re
from datetime import datetime

from commerce.contracts.canonical import canonical_json, sha256_hex
from commerce.contracts.money import MoneyFormatError, fmt_money, parse_money
from commerce.contracts.orders import ORDER_STATUSES_V1
from commerce.contracts.sources import SourceSpec

PARSER_CONTRACT_VERSION = 1

TEXT, INT, MONEY, TS, STATUS = "text", "int", "money", "timestamp", "status"

FIELD_KINDS: dict[str, dict[str, str]] = {
    "orders": {
        "order_status": STATUS,
        "order_purchase_timestamp": TS, "order_approved_at": TS,
        "order_delivered_carrier_date": TS, "order_delivered_customer_date": TS,
        "order_estimated_delivery_date": TS,
    },
    "order_items": {"order_item_id": INT, "shipping_limit_date": TS, "price": MONEY,
                    "freight_value": MONEY},
    "order_payments": {"payment_sequential": INT, "payment_installments": INT, "payment_value": MONEY},
    "products": {"product_weight_g": INT, "product_name_lenght": INT, "product_description_lenght": INT,
                 "product_photos_qty": INT, "product_length_cm": INT, "product_height_cm": INT,
                 "product_width_cm": INT},
}
REQUIRED: dict[str, tuple[str, ...]] = {
    "orders": ("customer_id", "order_status", "order_purchase_timestamp"),
    "customers": ("customer_unique_id",),
    "order_items": ("product_id", "seller_id", "price", "freight_value"),
    "order_payments": ("payment_type", "payment_installments", "payment_value"),
}

_INT = re.compile(r"^-?\d{1,18}$")
_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


class PayloadError(ValueError):
    pass


def _canon_value(kind: str, field: str, value: str) -> str:
    if kind == INT:
        if not _INT.match(value.strip()):
            raise PayloadError(f"{field}: not an integer: {value!r}")
        return str(int(value.strip()))
    if kind == MONEY:
        try:
            return fmt_money(parse_money(value))
        except MoneyFormatError as exc:
            raise PayloadError(f"{field}: {exc}") from exc
    if kind == TS:
        v = value.strip()
        if not _TS.match(v):
            raise PayloadError(f"{field}: timestamp must be YYYY-MM-DD HH:MM:SS: {value!r}")
        try:
            datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise PayloadError(f"{field}: impossible timestamp {value!r}") from exc
        return v
    if kind == STATUS:
        v = value.strip()
        if v not in ORDER_STATUSES_V1:
            raise PayloadError(f"{field}: status {v!r} is not in order-status contract v1")
        return v
    return value


def canonicalise(spec: SourceSpec, payload: dict[str, str | None]) -> dict[str, str | None]:
    kinds = FIELD_KINDS.get(spec.name, {})
    out: dict[str, str | None] = {}
    for field, value in payload.items():
        if value is None:
            out[field] = None
        elif value == "":
            raise PayloadError(f"{field}: empty string (the replay writer encodes empty as null)")
        else:
            out[field] = _canon_value(kinds.get(field, TEXT), field, value)
    for field in (*spec.key_columns, *REQUIRED.get(spec.name, ())):
        if out.get(field) is None:
            raise PayloadError(f"{field}: required value is missing")
    return out


def entity_key_of(spec: SourceSpec, canonical: dict[str, str | None]) -> str:
    values = [int(canonical[c]) if c in spec.int_key_columns else canonical[c]
              for c in spec.key_columns]
    return canonical_json(values).decode("utf-8")


def payload_hash(canonical: dict[str, str | None], operation: str = "UPSERT") -> str:
    return sha256_hex(canonical_json({"operation": operation, "payload": canonical}))

