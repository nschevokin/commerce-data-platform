from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal

import pytest
from replay_helpers import A, B, C, copy_fixture, tree_hashes

from commerce.contracts.manifest import ContractError
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.reader import ReplayReader
from commerce.replay.scenarios import FAULT_VARIANTS, build_scenario, select_keys
from commerce.replay.store import RawStore


@pytest.fixture()
def base(tmp_path):
    src = DirSource(copy_fixture(tmp_path / "csv"))
    store = RawStore(tmp_path / "store")
    generate_baseline(src, store)
    return src.digest(), store


def _events(store, ds, version):
    return ReplayReader(store, ds, version).all_events()


def _owner(e):
    k = json.loads(e.entity_key)[0]
    return next(o for o in (A, B, C) if o[-1] == k[-1]) if e.source == "customers" else k


def _flat(by_day):
    return [(d, e) for d, evs in by_day.items() for e in evs]


def test_select_keys_is_deterministic_and_order_independent():
    keys = [f"k{i}" for i in range(50)]
    a = select_keys(keys, seed=1, scenario="x", count=5)
    assert a == select_keys(list(reversed(keys)), seed=1, scenario="x", count=5)
    assert a != select_keys(keys, seed=2, scenario="x", count=5)
    assert len(a) == 5


def test_duplicate_redelivers_identical_events_later(base):
    ds, store = base
    before = tree_hashes(store.root)
    out = build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="duplicate",
                         order_ids=[A])
    assert out["replay_version"] == "duplicate-v1-seed-42"
    assert {k: v for k, v in tree_hashes(store.root).items() if k in before} == before
    b, s = _flat(_events(store, ds, "baseline-v1")), _flat(_events(store, ds, "duplicate-v1-seed-42"))
    extra = [(d, e) for d, e in s if (d, e) not in b]
    assert len(extra) == 6
    assert len(s) == len(b) + len(extra)
    assert all(d > next(bd for bd, be in b if be == e) for d, e in extra)
    assert all(e in [x for _, x in b] for _, e in extra)


def test_late_shifts_whole_order_two_days(base):
    ds, store = base
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="late", order_ids=[B])
    b, s = _events(store, ds, "baseline-v1"), _events(store, ds, "late-v1-seed-42")
    base_b = [(d, e) for d, e in _flat(b) if _owner(e) == B]
    late_b = [(d, e) for d, e in _flat(s) if _owner(e) == B]
    assert len(base_b) == len(late_b) > 0
    days = sorted(b)
    for (d0, e0), (d1, e1) in zip(sorted(base_b, key=lambda t: t[1].sort_key()),
                                  sorted(late_b, key=lambda t: t[1].sort_key()), strict=True):
        assert days.index(d1) - days.index(d0) == 2
        assert e1.available_at - e0.available_at == timedelta(days=2) and e1.payload == e0.payload
    untouched = [t for t in _flat(b) if _owner(t[1]) != B]
    assert [t for t in _flat(s) if _owner(t[1]) != B] == untouched


def test_correction_adds_revision_two_with_exact_delta(base):
    ds, store = base
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="correction", order_ids=[A])
    b, s = _flat(_events(store, ds, "baseline-v1")), _flat(_events(store, ds, "correction-v1-seed-42"))
    new = [(d, e) for d, e in s if (d, e) not in b]
    assert sorted(e.source for _, e in new) == ["order_items", "order_payments"]
    assert all(e.revision == 2 for _, e in new)
    for _, e in new:
        old = next(x for _, x in b if x.entity_key == e.entity_key and x.source == e.source)
        field = "price" if e.source == "order_items" else "payment_value"
        assert Decimal(e.payload[field]) - Decimal(old.payload[field]) == Decimal("5.00")
        assert e.available_at > old.available_at
        assert {k: v for k, v in e.payload.items() if k != field} == \
            {k: v for k, v in old.payload.items() if k != field}


def test_seeded_scenario_is_reproducible_across_stores(tmp_path):
    csv = copy_fixture(tmp_path / "csv")
    digests = []
    for n in ("a", "b"):
        store = RawStore(tmp_path / n)
        generate_baseline(DirSource(csv), store)
        build_scenario(store, dataset_sha256=DirSource(csv).digest(), base_version="baseline-v1",
                       kind="late", seed=7, count=1)
        digests.append(tree_hashes(store.root))
    assert digests[0] == digests[1]


def test_scenario_input_validation(base):
    ds, store = base
    with pytest.raises(ContractError, match="not in the baseline"):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="late", order_ids=["nope"])
    with pytest.raises(ContractError, match="only"):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="duplicate", count=99)
    with pytest.raises(ContractError, match="unknown scenario"):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="chaos")
    with pytest.raises(ContractError, match="unknown fault"):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault", variant="x")
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="late", order_ids=[C])
    with pytest.raises(ContractError, match="baseline"):
        build_scenario(store, dataset_sha256=ds, base_version="late-v1-seed-42", kind="late", count=1)


@pytest.mark.parametrize("variant", FAULT_VARIANTS)
def test_fault_variants_are_written_as_separate_replays(base, variant):
    ds, store = base
    out = build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault",
                         variant=variant, order_ids=[A])
    assert out["replay_version"] == f"fault-{variant.replace('_', '-')}-v1"
    ReplayReader(store, ds, out["replay_version"]).verify() if variant != "orphan_item" else None
    assert ReplayReader(store, ds, "baseline-v1").verify()["total_rows"] == 21


def test_fault_malformed_money_carries_three_decimals(base):
    ds, store = base
    build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault",
                   variant="malformed_money", order_ids=[A])
    evs = _flat(_events(store, ds, "fault-malformed-money-v1"))
    assert any(e.source == "order_items" and e.payload["price"] == "12.345" for _, e in evs)
