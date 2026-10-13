from __future__ import annotations

import pytest
from replay_helpers import copy_fixture, make_zip

from commerce.contracts.manifest import ContractError
from commerce.replay.dataset import DirSource, ZipSource, inventory
from commerce.replay.profile import profile_dataset

PROV = {"retrieved_on": "2026-10-11", "dataset_url": "https://example.test/d", "licence": "CC BY-NC-SA 4.0"}


def test_inventory_lists_core_files_and_defers_the_rest(tmp_path):
    z = ZipSource(make_zip(tmp_path / "a.zip", extra={
        "olist_order_reviews_dataset.csv": 'id,comment\n1,"two\nlines"\n',
        "olist_geolocation_dataset.csv": "zip,lat\n1,2\n"}))
    inv = inventory(z, **PROV)
    roles = {f["name"]: f["role"] for f in inv["files"]}
    assert roles["olist_orders_dataset.csv"] == "core:orders"
    assert roles["olist_order_reviews_dataset.csv"] == roles["olist_geolocation_dataset.csv"] == "deferred"
    reviews = next(f for f in inv["files"] if f["name"] == "olist_order_reviews_dataset.csv")
    assert reviews["row_count"] == 1
    assert inv["licence"] == PROV["licence"] and inv["dataset_sha256"] == z.digest()


def test_inventory_is_deterministic(tmp_path):
    z = ZipSource(make_zip(tmp_path / "a.zip"))
    assert inventory(z, **PROV) == inventory(z, **PROV)


def test_missing_core_file_is_an_error(tmp_path):
    z = ZipSource(make_zip(tmp_path / "a.zip", skip={"olist_sellers_dataset.csv"}))
    with pytest.raises(ContractError, match="olist_sellers_dataset.csv"):
        inventory(z, **PROV)


@pytest.mark.parametrize("missing", ["retrieved_on", "dataset_url", "licence"])
def test_provenance_is_required_not_guessed(tmp_path, missing):
    z = ZipSource(make_zip(tmp_path / "a.zip"))
    with pytest.raises(ContractError, match="provenance"):
        inventory(z, **{**PROV, missing: ""})


def test_profile_of_fixture_matches_hand_counts(tmp_path):
    p = profile_dataset(DirSource(copy_fixture(tmp_path / "csv")))
    assert {k: v["row_count"] for k, v in p["sources"].items()} == {
        "orders": 3, "customers": 3, "order_items": 4, "order_payments": 4,
        "products": 3, "sellers": 2, "category_translation": 2}
    c = p["checks"]
    assert c["orders"]["status_histogram"] == {"canceled": 1, "delivered": 2}
    assert c["orders"]["delivered_status_with_actual_date"] == 2
    assert c["orders"]["statuses_outside_contract_v1"] == []
    assert c["order_payments"]["type_histogram"] == {"boleto": 1, "credit_card": 2, "voucher": 1}
    assert c["relationships"]["items_without_order"] == c["relationships"]["payments_without_order"] == 0


def test_profile_reports_bad_data_instead_of_hiding_it(tmp_path):
    csv = copy_fixture(tmp_path / "csv")
    f = csv / "olist_order_items_dataset.csv"
    f.write_text(f.read_text(encoding="utf-8").replace(",60.00,", ",12.345,", 1), encoding="utf-8")
    g = csv / "olist_orders_dataset.csv"
    g.write_text(g.read_text(encoding="utf-8").replace('"delivered"', '"returned"', 1), encoding="utf-8")
    c = profile_dataset(DirSource(csv))["checks"]
    assert c["order_items"]["malformed_price"] == 1
    assert c["orders"]["statuses_outside_contract_v1"] == ["returned"]
