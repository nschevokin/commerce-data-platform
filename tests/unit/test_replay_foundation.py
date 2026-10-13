from __future__ import annotations

import gzip
import os
from datetime import UTC, datetime

import pytest

from commerce.contracts.canonical import canonical_json, parse_utc, sha256_hex, utc_iso
from commerce.contracts.manifest import ContractError, Manifest, ManifestSource, check_calendar
from commerce.contracts.sources import REQUIRED_SOURCE_NAMES
from commerce.replay.events import SourceEvent, encode_events
from commerce.replay.store import RawStore


def test_canonical_json_is_order_and_whitespace_independent():
    one, two = canonical_json({"b": 1, "a": [1, 2]}), canonical_json({"a": [1, 2], "b": 1})
    assert one == two == b'{"a":[1,2],"b":1}'


def test_canonical_json_keeps_unicode_as_utf8():
    assert canonical_json({"city": "são paulo"}) == '{"city":"são paulo"}'.encode()


@pytest.mark.parametrize("bad", [{"x": 1.5}, {"x": float("nan")}, {1: "a"}, {"x": {1, 2}}])
def test_canonical_json_rejects_floats_and_non_json_types(bad):
    with pytest.raises(TypeError):
        canonical_json(bad)


def test_utc_iso_round_trip_and_strictness():
    dt = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert utc_iso(dt) == "2026-01-02T03:04:05Z" and parse_utc(utc_iso(dt)) == dt
    with pytest.raises(ValueError):
        utc_iso(datetime(2026, 1, 2))
    with pytest.raises(ValueError):
        parse_utc("2026-01-02 03:04:05")


def test_event_file_bytes_do_not_depend_on_input_order():
    t = datetime(2026, 1, 2, tzinfo=UTC)
    evs = [SourceEvent("orders", f'["o{i}"]', 1, t, {"order_id": f"o{i}", "x": None}) for i in range(5)]
    assert encode_events(evs) == encode_events(list(reversed(evs)))


def test_gzip_header_is_constant():
    _, gz = encode_events([])
    assert gz == gzip.compress(b"", compresslevel=6, mtime=0)
    assert gz[4:8] == b"\x00\x00\x00\x00"


def _entry(name: str, rows: int = 0) -> ManifestSource:
    sha = sha256_hex(name.encode())
    return ManifestSource(name, 1, f"raw/dataset=d/replay=r/day=2026-01-02/source={name}/sha256={sha}.jsonl.gz",
                          sha, sha256_hex(b""), 10, rows)


def _manifest(entries=None) -> Manifest:
    entries = entries if entries is not None else [_entry(n) for n in REQUIRED_SOURCE_NAMES]
    return Manifest("a" * 64, "baseline-v1", datetime(2026, 1, 2, tzinfo=UTC),
                    datetime(2026, 1, 3, tzinfo=UTC), tuple(entries))


def test_valid_manifest_round_trips_and_hash_is_stable():
    m = _manifest().validate()
    again = Manifest.from_bytes(m.to_bytes())
    assert again == m and again.sha256 == m.sha256 == sha256_hex(m.to_bytes())


def test_manifest_missing_a_source_is_rejected():
    entries = [_entry(n) for n in REQUIRED_SOURCE_NAMES if n != "order_payments"]
    with pytest.raises(ContractError, match="incomplete.*order_payments"):
        _manifest(entries).validate()


def test_manifest_must_be_sorted_and_unique():
    entries = [_entry(n) for n in REQUIRED_SOURCE_NAMES]
    with pytest.raises(ContractError, match="sorted"):
        _manifest(list(reversed(entries))).validate()


def test_manifest_rejects_bad_checksum_and_foreign_uri():
    good = _entry("orders")
    bad_sha = ManifestSource("orders", 1, good.uri, "zz", good.content_sha256, 1, 0)
    bad_uri = ManifestSource("orders", 1, "elsewhere/x.gz", good.sha256, good.content_sha256, 1, 0)
    for bad in (bad_sha, bad_uri):
        entries = [bad if n == "orders" else _entry(n) for n in REQUIRED_SOURCE_NAMES]
        with pytest.raises(ContractError):
            _manifest(entries).validate()


def test_manifest_bytes_must_be_canonical():
    pretty = _manifest().validate().to_bytes().replace(b",", b", ")
    with pytest.raises(ContractError):
        Manifest.from_bytes(pretty)


def test_calendar_gap_is_rejected():
    d = lambda day: datetime(2026, 1, day, tzinfo=UTC)  # noqa: E731
    check_calendar([(d(2), d(3)), (d(3), d(4))])
    with pytest.raises(ContractError, match="gap or overlap"):
        check_calendar([(d(2), d(3)), (d(4), d(5))])


def test_put_is_create_exclusive_and_idempotent(tmp_path):
    s = RawStore(tmp_path)
    assert s.put_bytes("raw/a/x.bin", b"one") is True
    assert s.put_bytes("raw/a/x.bin", b"one") is False
    with pytest.raises(ContractError, match="different content"):
        s.put_bytes("raw/a/x.bin", b"two")
    assert s.read_bytes("raw/a/x.bin") == b"one"


@pytest.mark.parametrize("uri", ["/abs/path", "../escape", "a/../../b", ""])
def test_unsafe_keys_are_rejected(tmp_path, uri):
    with pytest.raises(ContractError):
        RawStore(tmp_path).put_bytes(uri, b"x")


def test_read_verified_refuses_a_corrupted_object(tmp_path):
    s = RawStore(tmp_path)
    s.put_bytes("raw/o.bin", b"payload")
    sha = sha256_hex(b"payload")
    assert s.read_bytes_verified("raw/o.bin", sha) == b"payload"
    s.path("raw/o.bin").write_bytes(b"paYload")
    with pytest.raises(ContractError, match="checksum mismatch"):
        s.read_verified("raw/o.bin", sha)
    with pytest.raises(ContractError, match="missing"):
        s.read_verified("raw/nope.bin", sha)


def test_crash_while_writing_leaves_no_final_key_and_no_leftover(tmp_path, monkeypatch):
    s = RawStore(tmp_path)
    monkeypatch.setattr(os, "fsync", lambda fd: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        s.put_bytes("raw/crash.bin", b"data")
    monkeypatch.undo()
    assert not s.exists("raw/crash.bin")
    assert s.stale_temp_files() == []
    assert s.put_bytes("raw/crash.bin", b"data") is True


def test_filesystem_without_hard_links_falls_back_to_atomic_rename(tmp_path, monkeypatch):
    def no_link(*_a, **_k):
        raise PermissionError("hard links unsupported")
    monkeypatch.setattr(os, "link", no_link)
    s = RawStore(tmp_path)
    assert s.put_bytes("raw/x.bin", b"abc") is True
    assert s.read_bytes("raw/x.bin") == b"abc" and s.stale_temp_files() == []
