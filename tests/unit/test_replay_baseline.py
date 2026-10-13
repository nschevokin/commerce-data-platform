from __future__ import annotations

import json
import shutil

import pytest
from replay_helpers import A, B, C, content_digest, copy_fixture, make_zip, tree_hashes

from commerce.contracts.manifest import ContractError
from commerce.contracts.sources import REQUIRED_SOURCE_NAMES
from commerce.replay.dataset import DirSource, ZipSource, open_source
from commerce.replay.generator import ReplayConfig, generate_baseline
from commerce.replay.reader import IntervalIncomplete, ReplayReader
from commerce.replay.store import RawStore
from commerce.replay.writer import manifest_uri


@pytest.fixture()
def built(tmp_path):
    src = DirSource(copy_fixture(tmp_path / "csv"))
    store = RawStore(tmp_path / "store")
    summary = generate_baseline(src, store)
    return src, store, summary


def test_structure_of_the_fixture_baseline(built):
    src, store, summary = built
    r = ReplayReader(store, src.digest(), "baseline-v1")
    cal = r.calendar()
    assert len(cal) == 31
    assert r.verify()["rows_by_source"] == {
        "category_translation": 2, "customers": 3, "order_items": 4, "order_payments": 4,
        "orders": 3, "products": 3, "sellers": 2}
    boot = r.manifest(cal[0].start)
    assert {s.name: s.row_count for s in boot.sources if s.row_count} == {
        "category_translation": 2, "products": 3, "sellers": 2}
    for iv in cal:
        assert [s.name for s in r.manifest(iv.start).sources] == list(REQUIRED_SOURCE_NAMES)
    by_day = r.all_events()
    orders_on = {d: [json.loads(e.entity_key)[0] for e in evs if e.source == "orders"]
                 for d, evs in by_day.items()}
    assert [o for v in orders_on.values() for o in v] == [A, B, C]
    assert summary["report"]["orders"] == 3


def test_same_input_gives_byte_identical_stores(tmp_path):
    csv = copy_fixture(tmp_path / "csv")
    s1, s2 = RawStore(tmp_path / "s1"), RawStore(tmp_path / "s2")
    generate_baseline(DirSource(csv), s1)
    generate_baseline(DirSource(csv), s2)
    assert tree_hashes(s1.root) == tree_hashes(s2.root)


def test_rerun_into_same_store_is_a_no_op(built):
    src, store, _ = built
    before = tree_hashes(store.root)
    generate_baseline(src, store)
    assert tree_hashes(store.root) == before


def test_zip_and_directory_give_identical_event_content(tmp_path):
    d = DirSource(copy_fixture(tmp_path / "csv"))
    z = ZipSource(make_zip(tmp_path / "a.zip"))
    sd, sz = RawStore(tmp_path / "sd"), RawStore(tmp_path / "sz")
    generate_baseline(d, sd)
    generate_baseline(z, sz)
    assert d.digest() != z.digest()
    assert content_digest(ReplayReader(sd, d.digest(), "baseline-v1")) == \
        content_digest(ReplayReader(sz, z.digest(), "baseline-v1"))


def test_zip_ignores_macos_junk(tmp_path):
    clean = ZipSource(make_zip(tmp_path / "a.zip"))
    junk = ZipSource(make_zip(tmp_path / "b.zip", extra={"__MACOSX/._orders.csv": "x", "readme.txt": "hi"}))
    assert clean.members() == junk.members()


def test_pinned_content_digest_of_golden_baseline(built):
    src, store, _ = built
    assert content_digest(ReplayReader(store, src.digest(), "baseline-v1")) == PINNED


def test_tampered_raw_file_is_detected(built):
    src, store, _ = built
    r = ReplayReader(store, src.digest(), "baseline-v1")
    m = r.manifest(r.calendar()[1].start)
    entry = next(s for s in m.sources if s.row_count)
    store.path(entry.uri).write_bytes(b"corrupt")
    with pytest.raises(ContractError, match="checksum mismatch"):
        r.verify()


def test_tampered_manifest_is_detected(built):
    src, store, _ = built
    r = ReplayReader(store, src.digest(), "baseline-v1")
    iv = r.calendar()[1]
    p = store.path(manifest_uri(src.digest(), "baseline-v1", iv))
    p.write_bytes(p.read_bytes() + b" ")
    with pytest.raises(ContractError):
        r.manifest(iv.start)


def test_missing_manifest_means_interval_incomplete_not_empty(built):
    src, store, _ = built
    r = ReplayReader(store, src.digest(), "baseline-v1")
    iv = r.calendar()[5]
    r._by_start.pop(next(k for k, v in r._by_start.items() if v["label"] == iv.label))
    with pytest.raises(IntervalIncomplete):
        r.manifest(iv.start)


def test_crash_before_descriptor_leaves_replay_unreadable(tmp_path, monkeypatch):
    csv = copy_fixture(tmp_path / "csv")
    store = RawStore(tmp_path / "s")
    orig = RawStore.put_bytes

    def boom(self, uri, data):
        if uri.startswith("replays/"):
            raise OSError("killed")
        return orig(self, uri, data)

    monkeypatch.setattr(RawStore, "put_bytes", boom)
    with pytest.raises(OSError):
        generate_baseline(DirSource(csv), store)
    monkeypatch.undo()
    with pytest.raises(Exception):  # noqa: B017
        ReplayReader(store, DirSource(csv).digest(), "baseline-v1")
    generate_baseline(DirSource(csv), store)
    assert ReplayReader(store, DirSource(csv).digest(), "baseline-v1").verify()["total_rows"] == 21


def _mutate(tmp_path, fname, fn, name="csv"):
    csv = copy_fixture(tmp_path / name)
    p = csv / fname
    p.write_text(fn(p.read_text(encoding="utf-8")), encoding="utf-8")
    return DirSource(csv)


def test_orphan_item_fails_unless_explicitly_allowed(tmp_path):
    def add_orphan(t):
        lines = t.rstrip("\n").split("\n")
        row = lines[1].split(",")
        row[0] = "ghost"
        return "\n".join([*lines, ",".join(row)]) + "\n"
    src = _mutate(tmp_path, "olist_order_items_dataset.csv", add_orphan)
    with pytest.raises(ContractError, match="orphan"):
        generate_baseline(src, RawStore(tmp_path / "s1"))
    out = generate_baseline(src, RawStore(tmp_path / "s2"), ReplayConfig(allow_orphans=True))
    assert out["report"]["orphans_excluded"] == {"order_items": 1}


def test_duplicate_key_in_source_fails(tmp_path):
    src = _mutate(tmp_path, "olist_sellers_dataset.csv",
                  lambda t: t + t.split("\n")[1] + "\n")
    with pytest.raises(ContractError, match="(?i)duplicate"):
        generate_baseline(src, RawStore(tmp_path / "s"))


def test_missing_column_and_ragged_row_fail(tmp_path):
    src = _mutate(tmp_path, "olist_sellers_dataset.csv", lambda t: t.replace("seller_state", "state", 1))
    with pytest.raises(ContractError):
        generate_baseline(src, RawStore(tmp_path / "s1"))
    src = _mutate(tmp_path, "olist_sellers_dataset.csv", lambda t: t + "only,two\n", name="csv2")
    with pytest.raises(ContractError):
        generate_baseline(src, RawStore(tmp_path / "s2"))


def test_unreadable_source_directory(tmp_path):
    shutil.rmtree(tmp_path, ignore_errors=True)
    with pytest.raises(Exception):  # noqa: B017
        open_source(tmp_path / "nope")


PINNED = "40d5918fd93ec75d53d3440708459bbd5cd64ae26250d82608e9237a90877f16"
