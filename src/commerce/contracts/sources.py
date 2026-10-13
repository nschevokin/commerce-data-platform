from __future__ import annotations

from dataclasses import dataclass

SOURCE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SourceSpec:
    name: str
    csv_name: str
    key_columns: tuple[str, ...]
    int_key_columns: frozenset[str]
    columns: tuple[str, ...]
    stage: str


ORDERS = SourceSpec(
    "orders", "olist_orders_dataset.csv", ("order_id",), frozenset(),
    ("order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
     "order_delivered_carrier_date", "order_delivered_customer_date",
     "order_estimated_delivery_date"), "order")
CUSTOMERS = SourceSpec(
    "customers", "olist_customers_dataset.csv", ("customer_id",), frozenset(),
    ("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city",
     "customer_state"), "order")
ORDER_ITEMS = SourceSpec(
    "order_items", "olist_order_items_dataset.csv", ("order_id", "order_item_id"),
    frozenset({"order_item_id"}),
    ("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
     "freight_value"), "order")
ORDER_PAYMENTS = SourceSpec(
    "order_payments", "olist_order_payments_dataset.csv", ("order_id", "payment_sequential"),
    frozenset({"payment_sequential"}),
    ("order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"),
    "order")
PRODUCTS = SourceSpec(
    "products", "olist_products_dataset.csv", ("product_id",), frozenset(),
    ("product_id", "product_category_name", "product_weight_g"), "reference")
SELLERS = SourceSpec(
    "sellers", "olist_sellers_dataset.csv", ("seller_id",), frozenset(),
    ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"), "reference")
CATEGORY_TRANSLATION = SourceSpec(
    "category_translation", "product_category_name_translation.csv", ("product_category_name",),
    frozenset(), ("product_category_name", "product_category_name_english"), "reference")

CORE_SOURCES: tuple[SourceSpec, ...] = (
    ORDERS, CUSTOMERS, ORDER_ITEMS, ORDER_PAYMENTS, PRODUCTS, SELLERS, CATEGORY_TRANSLATION)
SPEC_BY_NAME = {s.name: s for s in CORE_SOURCES}
SPEC_BY_CSV = {s.csv_name: s for s in CORE_SOURCES}
REQUIRED_SOURCE_NAMES: tuple[str, ...] = tuple(sorted(SPEC_BY_NAME))
REFERENCE_SOURCES = tuple(s for s in CORE_SOURCES if s.stage == "reference")
ORDER_SOURCES = tuple(s for s in CORE_SOURCES if s.stage == "order")
