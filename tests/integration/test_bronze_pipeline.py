from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from replay_helpers import A, B, copy_fixture

from commerce.build.candidate import FrontierNotReady, build_candidate, input_digest, resolve_frontier
from commerce.contracts.canonical import canonical_json, sha256_hex
from commerce.fixture.harness import diff, observe_candidate
from commerce.ingestion.runner import ingest_range
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.reader import ReplayReader
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden_v1"
EXPECTED = json.loads((GOLDEN / "expected.json").read_text())
SCEN = EXPECTED["scenarios_for_later_tickets"]
FINAL = 30


@pytest.fixture(scope="module")
def world(own_warehouse, own_role_dsn, tmp_path_factory):
    root = tmp_path_factory.mktemp("bronze")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    for kind, ids in (("duplicate", [A]), ("late", [B]), ("correction", [B])):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind=kind, order_ids=ids)
    with psycopg.connect(own_role_dsn("commerce_ingest")) as conn:
        for v in ("baseline-v1", "duplicate-v1-seed-42", "late-v1-seed-42", "correction-v1-seed-42"):
            res = ingest_range(conn, store, ReplayReader(store, ds, v), code_sha="t", dependency_lock_sha="t")
            assert all(r.status == "INGESTED" for r in res), res
    env = {"COMMERCE_WAREHOUSE_HOST": str(own_warehouse["host"]),
           "COMMERCE_WAREHOUSE_PORT": str(own_warehouse["port"]),
           "COMMERCE_WAREHOUSE_DB": own_warehouse["dbname"],
           "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": "test-only-commerce_transform"}
    return {"store": store, "ds": ds, "env": env, "root": root, "wh": own_warehouse,
            "dsn": own_role_dsn("commerce_transform")}


def _build(world, version, idx):
    reader = ReplayReader(world["store"], world["ds"], version)
    end = reader.calendar()[idx].end
    out = build_candidate(transform_dsn=world["dsn"], transform_env=world["env"], reader=reader,
                          frontier_end=end, artifacts_root=world["root"] / "builds", exact_fixture=True)
    assert out["report"].ok, out["report"].failures
    out["observed"] = observe_candidate(world["wh"]["admin_dsn"], out["candidate_schema"])
    return out


def _business_checksum(observed: dict) -> str:
    return sha256_hex(canonical_json(observed))


def test_golden_answers_through_the_real_path(world):
    out = _build(world, "baseline-v1", FINAL)
    assert diff(EXPECTED["baseline"], out["observed"]) == []
    e = out["observed"]["eligible"]
    assert (e["eligible_orders"], e["merchandise_value_brl"], e["freight_value_brl"],
            e["recorded_payment_brl"], e["average_order_value_brl"]) == (
        2, "130.00", "13.00", "143.00", "65.00")
    assert out["report"].executed == out["report"].expected


def test_repeated_delivery_has_no_business_effect(world):
    base = _build(world, "baseline-v1", FINAL)
    dup = _build(world, "duplicate-v1-seed-42", FINAL)
    assert _business_checksum(dup["observed"]) == _business_checksum(base["observed"])
    assert dup["frontier"].manifest_sha256s != base["frontier"].manifest_sha256s


def test_before_b_arrives_only_a_is_visible(world):
    out = _build(world, "baseline-v1", 10)
    b = SCEN["B_late_two_replay_days"]["before_B_available"]
    o = out["observed"]
    assert o["eligible"]["eligible_orders"] == b["eligible_orders"]
    assert o["eligible"]["merchandise_value_brl"] == b["merchandise_value_brl"]
    assert o["eligible"]["recorded_payment_brl"] == b["recorded_payment_brl"]
    assert sorted(o["sales_daily"]) == b["purchase_dates"]


def test_late_order_changes_an_older_purchase_date(world):
    before = _build(world, "late-v1-seed-42", 12)
    after = _build(world, "late-v1-seed-42", 13)
    s_before, s_after = before["observed"]["sales_daily"], after["observed"]["sales_daily"]
    spec = SCEN["B_late_two_replay_days"]
    assert sorted(s_before) == spec["before_B_available"]["purchase_dates"]
    assert sorted(s_after) == spec["after_B_available"]["purchase_dates"]
    assert s_after["2017-03-11"]["eligible_orders"] == \
        spec["after_B_available"]["2017-03-11"]["eligible_orders"]
    assert s_after["2017-03-11"]["merchandise_value_brl"] == "30.00"
    assert after["observed"]["eligible"]["merchandise_value_brl"] == \
        spec["after_B_available"]["eligible_total_merchandise_brl"]
    assert diff(EXPECTED["baseline"], _build(world, "late-v1-seed-42", FINAL)["observed"]) == []


def test_correction_moves_aov_to_67_50_only_after_it_arrives(world):
    spec = SCEN["B_revision_2"]
    before = _build(world, "correction-v1-seed-42", 13)
    assert before["observed"]["eligible"]["average_order_value_brl"] == "65.00"
    assert before["observed"]["orders"][B]["merchandise_value_brl"] == "30.00"
    after = _build(world, "correction-v1-seed-42", 14)
    for k, v in spec["eligible"].items():
        assert after["observed"]["eligible"][k] == v, k
    o = after["observed"]["orders"][B]
    assert (o["merchandise_value_brl"], o["freight_value_brl"], o["recorded_payment_brl"]) == (
        spec["item_price_brl"], spec["item_freight_brl"], spec["payment_value_brl"])
    final = _build(world, "correction-v1-seed-42", FINAL)
    assert final["observed"]["eligible"]["average_order_value_brl"] == "67.50"


def test_frontier_must_be_a_complete_contiguous_prefix(world, own_role_dsn, tmp_path):
    reader = ReplayReader(world["store"], world["ds"], "baseline-v1")
    cal = reader.calendar()
    build_scenario(world["store"], dataset_sha256=world["ds"], base_version="baseline-v1", kind="duplicate",
                   order_ids=[A], new_version="partial-v1")
    partial = ReplayReader(world["store"], world["ds"], "partial-v1")
    with psycopg.connect(own_role_dsn("commerce_ingest")) as c:
        ingest_range(c, world["store"], partial, code_sha="t", dependency_lock_sha="t", end_index=5)
    with psycopg.connect(world["dsn"]) as c:
        assert resolve_frontier(c, partial, partial.calendar()[4].end).intervals == 5
        with pytest.raises(FrontierNotReady, match="not COMPLETE"):
            resolve_frontier(c, partial, partial.calendar()[6].end)
        from commerce.contracts.manifest import ContractError
        with pytest.raises(ContractError, match="not the end of a replay interval"):
            resolve_frontier(c, reader, cal[3].start + (cal[3].end - cal[3].start) / 2)


def test_candidate_is_isolated_in_its_own_schema(world):
    out = _build(world, "baseline-v1", FINAL)
    with psycopg.connect(world["wh"]["admin_dsn"]) as c:
        schemas = {r[0] for r in c.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'fct_orders'")}
        assert out["candidate_schema"] in schemas
        assert c.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema IN "
                         "('bronze','control','serving') AND table_name LIKE 'fct_%'").fetchone()[0] == 0


def test_input_digest_depends_on_every_identity_part(world):
    reader = ReplayReader(world["store"], world["ds"], "baseline-v1")
    with psycopg.connect(world["dsn"]) as c:
        f1 = resolve_frontier(c, reader, reader.calendar()[5].end)
        f2 = resolve_frontier(c, reader, reader.calendar()[6].end)
    kw = {"dataset_sha256": world["ds"], "frontier": f1, "code_sha": "c", "dependency_lock_sha": "l"}
    base = input_digest(**kw)
    assert base == input_digest(**kw)
    for change in ({"frontier": f2}, {"code_sha": "c2"}, {"dependency_lock_sha": "l2"},
                   {"dataset_sha256": "0" * 64}):
        assert input_digest(**{**kw, **change}) != base
    assert datetime.now(UTC)
