from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from replay_helpers import A, B, copy_fixture

from commerce.contracts.manifest import ContractError, ManifestSource
from commerce.contracts.payloads import canonicalise, payload_hash
from commerce.contracts.sources import SPEC_BY_NAME
from commerce.ingestion.ledger import (
    ConcurrentWriteError,
    FileRejected,
    GuardHeldError,
    StaleWriterError,
    ingest_file,
)
from commerce.ingestion.runner import ingest_interval_chunk, ingest_range
from commerce.replay.dataset import DirSource
from commerce.replay.events import SourceEvent
from commerce.replay.generator import generate_baseline
from commerce.replay.reader import ReplayReader
from commerce.replay.scenarios import build_scenario
from commerce.replay.store import RawStore

pytestmark = pytest.mark.integration

CODE, LOCK = "test-code-sha", "test-lock-sha"
T0 = datetime(2026, 1, 2, tzinfo=UTC)
T1 = T0 + timedelta(days=1)


@pytest.fixture(scope="session")
def replays(tmp_path_factory):
    root = tmp_path_factory.mktemp("ingest")
    src = DirSource(copy_fixture(root / "csv"))
    store = RawStore(root / "store")
    generate_baseline(src, store)
    ds = src.digest()
    for kind, ids in (("duplicate", [A]), ("late", [B]), ("correction", [B])):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind=kind, order_ids=ids)
    for variant in ("malformed_money", "malformed_timestamp", "unknown_status", "conflicting_payload",
                    "orphan_item"):
        build_scenario(store, dataset_sha256=ds, base_version="baseline-v1", kind="fault",
                       variant=variant, order_ids=[A])
    return store, ds


@pytest.fixture()
def conn(own_warehouse, own_role_dsn):
    def clear():
        with psycopg.connect(own_warehouse["admin_dsn"], autocommit=True) as a:
            a.execute("UPDATE control.run_guard SET owner_run_id = NULL WHERE singleton_id = 1")
    clear()
    c = psycopg.connect(own_role_dsn("commerce_ingest"))
    yield c
    c.close()
    clear()


@pytest.fixture()
def admin(own_warehouse):
    with psycopg.connect(own_warehouse["admin_dsn"], autocommit=True) as a:
        yield a


def _ingest(conn, replays, version, **kw):
    store, ds = replays
    return ingest_range(conn, store, ReplayReader(store, ds, version), code_sha=CODE,
                        dependency_lock_sha=LOCK, **kw)


def _count(admin, sql, *args):
    return admin.execute(sql, args).fetchone()[0]


def test_baseline_ingests_every_event_and_interval(conn, admin, replays):
    results = _ingest(conn, replays, "baseline-v1")
    assert [r.status for r in results] == ["INGESTED"] * 5
    assert sum(r.intervals_completed for r in results) == 31
    rows = dict(admin.execute("SELECT source_name, count(*) FROM bronze.source_events "
                              "WHERE replay_version = 'baseline-v1' GROUP BY 1").fetchall())
    assert rows == {"category_translation": 2, "customers": 3, "order_items": 4, "order_payments": 4,
                    "orders": 3, "products": 3, "sellers": 2}
    assert _count(admin, "SELECT count(*) FROM control.file_receipts "
                         "WHERE replay_version = 'baseline-v1'") == 31 * 7
    assert _count(admin, "SELECT count(*) FROM control.intervals WHERE replay_version = 'baseline-v1' "
                         "AND completeness = 'COMPLETE'") == 31
    assert _count(admin, "SELECT owner_run_id IS NULL FROM control.run_guard")
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE input_rows <> "
                         "accepted_new_rows + exact_duplicate_rows + rejected_rows") == 0
    assert _count(admin, "SELECT sum(exact_duplicate_rows) FROM control.file_receipts "
                         "WHERE replay_version = 'baseline-v1'") == 0


def test_bronze_holds_the_typed_canonical_payload_and_its_hash(admin, conn, replays):
    _ingest(conn, replays, "baseline-v1", end_index=3)
    payload, sha = admin.execute(
        "SELECT payload, payload_sha256 FROM bronze.source_events WHERE replay_version = 'baseline-v1' "
        "AND source_name = 'order_items' AND entity_key = %s", (f'["{A}",1]',)).fetchone()
    assert payload["price"] == "60.00" and payload["order_item_id"] == "1"
    spec = SPEC_BY_NAME["order_items"]
    assert sha == payload_hash(canonicalise(spec, payload))
    assert admin.execute("SELECT parser_contract_version FROM bronze.source_events LIMIT 1").fetchone()[0] == 1


def test_rerun_is_a_no_op(conn, admin, replays):
    _ingest(conn, replays, "baseline-v1")
    before = (_count(admin, "SELECT count(*) FROM bronze.source_events"),
              _count(admin, "SELECT count(*) FROM control.file_receipts"))
    again = _ingest(conn, replays, "baseline-v1")
    assert all(r.status == "INGESTED" for r in again)
    assert sum(len(r.files) for r in again) == 217
    assert all(f.already_committed for r in again for f in r.files)
    assert (_count(admin, "SELECT count(*) FROM bronze.source_events"),
            _count(admin, "SELECT count(*) FROM control.file_receipts")) == before


def test_duplicate_delivery_counts_redeliveries_without_changing_events(conn, admin, replays):
    _ingest(conn, replays, "duplicate-v1-seed-42")
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = "
                         "'duplicate-v1-seed-42'") == 21
    assert _count(admin, "SELECT sum(exact_duplicate_rows) FROM control.file_receipts WHERE "
                         "replay_version = 'duplicate-v1-seed-42'") == 6
    assert _count(admin, "SELECT count(DISTINCT (source_name, entity_key, revision)) FROM "
                         "bronze.source_events WHERE replay_version = 'duplicate-v1-seed-42'") == 21


def test_late_order_arrives_two_days_later(conn, admin, replays):
    _ingest(conn, replays, "baseline-v1")
    _ingest(conn, replays, "late-v1-seed-42")
    base, late = (admin.execute("SELECT available_at FROM bronze.source_events WHERE replay_version = %s "
                                "AND source_name = 'orders' AND entity_key = %s",
                                (v, f'["{B}"]')).fetchone()[0] for v in ("baseline-v1", "late-v1-seed-42"))
    assert late - base == timedelta(days=2)


def test_correction_adds_revision_two_and_keeps_revision_one(conn, admin, replays):
    _ingest(conn, replays, "correction-v1-seed-42")
    revs = admin.execute("SELECT revision, payload->>'price' FROM bronze.source_events WHERE replay_version = "
                         "'correction-v1-seed-42' AND source_name = 'order_items' AND entity_key = %s "
                         "ORDER BY revision", (f'["{B}",1]',)).fetchall()
    assert revs == [(1, "30.00"), (2, "35.00")]
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = "
                         "'correction-v1-seed-42'") == 23


@pytest.mark.parametrize("variant,source,needle", [
    ("malformed_money", "order_items", "12.345"),
    ("malformed_timestamp", "orders", "timestamp must be"),
    ("unknown_status", "orders", "not in order-status contract"),
    ("conflicting_payload", "orders", "appears twice in the file with different content"),
])
def test_faulty_file_is_blocked_whole_without_a_receipt(conn, admin, replays, variant, source, needle):
    version = f"fault-{variant.replace('_', '-')}-v1"
    results = _ingest(conn, replays, version)
    bad = results[-1]
    assert bad.status == "FAILED_CONTRACT" and bad.artefact_uri
    store, _ = replays
    import json
    report = json.loads(store.read_bytes(bad.artefact_uri))
    assert report["source"] == source and needle in json.dumps(report["issues"])
    assert report["issues"][0]["row"] >= 1
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE replay_version = %s "
                         "AND file_sha256 = %s", version, report["file_sha256"]) == 0
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE source_file_sha256 = %s",
                  report["file_sha256"]) == 0
    assert _count(admin, "SELECT count(*) FROM control.intervals WHERE replay_version = %s "
                         "AND interval_start = %s", version, report["interval_start"]) == 0
    assert _count(admin, "SELECT status FROM control.runs WHERE run_id = %s", bad.run_id) == "FAILED_CONTRACT"
    assert _count(admin, "SELECT owner_run_id IS NULL FROM control.run_guard")


def test_orphan_item_passes_ingestion_because_relationships_are_a_quality_gate(conn, replays):
    assert all(r.status == "INGESTED" for r in _ingest(conn, replays, "fault-orphan-item-v1"))


def test_missing_manifest_is_input_not_ready_and_ingests_nothing(conn, admin, replays):
    store, ds = replays
    reader = ReplayReader(store, ds, "baseline-v1")
    reader._by_start.pop(next(k for k, v in reader._by_start.items() if v["label"].endswith("-04")))
    r = ingest_interval_chunk(conn, store, reader, reader.calendar()[:7], code_sha=CODE,
                              dependency_lock_sha=LOCK)
    assert r.status == "INPUT_NOT_READY"
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE run_id = %s", r.run_id) == 0
    assert _count(admin, "SELECT status FROM control.runs WHERE run_id = %s", r.run_id) == "INPUT_NOT_READY"


def test_a_run_covers_at_most_seven_intervals(conn, replays):
    store, ds = replays
    reader = ReplayReader(store, ds, "baseline-v1")
    with pytest.raises(ContractError, match="1..7"):
        ingest_interval_chunk(conn, store, reader, reader.calendar()[:8], code_sha=CODE,
                              dependency_lock_sha=LOCK)


def _begin_run(admin, conn, version):
    run_id = uuid.uuid4()
    admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                  "code_sha, dependency_lock_sha, status) VALUES (%s, 'manual', %s, %s, %s, 'x', 'y', "
                  "'CREATED')", (run_id, version, T0, T1))
    gen = conn.execute("SELECT control.acquire_guard(%s)", (run_id,)).fetchone()[0]
    conn.commit()
    return run_id, gen


def _event(oid="o1", status="delivered", rev=1, at=T0 + timedelta(hours=1), price="10.00"):
    p = {"order_id": oid, "customer_id": "c1", "order_status": status,
         "order_purchase_timestamp": "2017-01-01 10:00:00", "order_approved_at": None,
         "order_delivered_carrier_date": None, "order_delivered_customer_date": None,
         "order_estimated_delivery_date": "2017-02-01 00:00:00"}
    return SourceEvent("orders", f'["{oid}"]', rev, at, p)


def _entry(name="orders", sha=None):
    sha = sha or uuid.uuid4().hex * 2
    return ManifestSource(name, 1, f"raw/x/{sha}.jsonl.gz", sha, "0" * 64, 1, 1)


def _file(conn, run_id, gen, version, events, *, start=T0, entry=None, fault=None, manifest="m" * 64):
    return ingest_file(conn, run_id=run_id, generation=gen, replay_version=version, interval_start=start,
                       interval_end=start + timedelta(days=1), manifest_sha256=manifest,
                       entry=entry or _entry(), events=events, fault=fault)


def test_conflicting_revision_against_bronze_blocks_the_file(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-conflict")
    _file(conn, run_id, gen, "rv-conflict", [_event()])
    with pytest.raises(FileRejected, match="different content"):
        _file(conn, run_id, gen, "rv-conflict", [_event(status="shipped")])
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-conflict'") == 1
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE replay_version = 'rv-conflict'") == 1


def test_same_content_with_different_availability_is_a_conflict(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-avail")
    _file(conn, run_id, gen, "rv-avail", [_event()])
    with pytest.raises(FileRejected):
        _file(conn, run_id, gen, "rv-avail", [_event(at=T0 + timedelta(hours=2))])


def test_d005_exact_redelivery_in_a_later_interval_is_a_counted_duplicate(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-d005")
    _file(conn, run_id, gen, "rv-d005", [_event()])
    out = _file(conn, run_id, gen, "rv-d005", [_event()], start=T1)
    assert (out.accepted_new_rows, out.exact_duplicate_rows) == (0, 1)
    with pytest.raises(FileRejected, match="before the interval start"):
        _file(conn, run_id, gen, "rv-d005", [_event("o2")], start=T1)


def test_event_after_the_interval_end_is_rejected(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-late")
    with pytest.raises(FileRejected, match="at or after the end"):
        _file(conn, run_id, gen, "rv-late", [_event(at=T1)])


def test_receipt_reconciliation_counts_within_file_repeats(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-dup")
    out = _file(conn, run_id, gen, "rv-dup", [_event(), _event(), _event("o2")])
    assert (out.input_rows, out.accepted_new_rows, out.exact_duplicate_rows) == (3, 2, 1)


def test_kill_before_commit_leaves_nothing_and_retry_inserts_once(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-kill")
    entry = _entry()

    def die(point):
        if point == "before_commit":
            raise RuntimeError("loader killed")
    with pytest.raises(RuntimeError, match="killed"):
        _file(conn, run_id, gen, "rv-kill", [_event(), _event("o2")], entry=entry, fault=die)
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-kill'") == 0
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE replay_version = 'rv-kill'") == 0
    out = _file(conn, run_id, gen, "rv-kill", [_event(), _event("o2")], entry=entry)
    assert (out.accepted_new_rows, out.already_committed) == (2, False)
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-kill'") == 2


def test_lost_acknowledgement_after_commit_is_recognised_on_retry(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-ack")
    entry = _entry()

    def drop(point):
        if point == "after_commit":
            raise ConnectionError("connection dropped after commit")
    with pytest.raises(ConnectionError):
        _file(conn, run_id, gen, "rv-ack", [_event()], entry=entry, fault=drop)
    out = _file(conn, run_id, gen, "rv-ack", [_event()], entry=entry)
    assert out.already_committed and out.accepted_new_rows == 1
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE replay_version = 'rv-ack'") == 1
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-ack'") == 1


def test_receipt_for_a_different_manifest_is_a_contract_error(conn, admin):
    run_id, gen = _begin_run(admin, conn, "rv-mf")
    entry = _entry()
    _file(conn, run_id, gen, "rv-mf", [_event()], entry=entry)
    with pytest.raises(ContractError, match="different manifest"):
        _file(conn, run_id, gen, "rv-mf", [_event()], entry=entry, manifest="z" * 64)


def test_second_run_cannot_take_the_guard(conn, admin):
    run1, _ = _begin_run(admin, conn, "rv-g1")
    run2 = uuid.uuid4()
    admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                  "code_sha, dependency_lock_sha, status) VALUES (%s, 'manual', 'rv-g1', %s, %s, 'x', 'y', "
                  "'CREATED')", (run2, T0, T1))
    with pytest.raises(psycopg.Error) as ei:
        conn.execute("SELECT control.acquire_guard(%s)", (run2,))
    conn.rollback()
    from commerce.ingestion.ledger import translate_db_error
    assert isinstance(translate_db_error(ei.value), GuardHeldError)
    assert run1 != run2


def test_stale_writer_after_takeover_cannot_write(conn, admin):
    old_run, old_gen = _begin_run(admin, conn, "rv-stale")
    new_run = uuid.uuid4()
    admin.execute("INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                  "code_sha, dependency_lock_sha, status) VALUES (%s, 'manual', 'rv-stale', %s, %s, 'x', "
                  "'y', 'CREATED')", (new_run, T0, T1))
    new_gen = admin.execute("SELECT control.takeover_guard(%s, 'operator', 'old DAG run is terminal')",
                            (new_run,)).fetchone()[0]
    assert new_gen == old_gen + 1
    with pytest.raises(StaleWriterError):
        _file(conn, old_run, old_gen, "rv-stale", [_event()])
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-stale'") == 0
    assert admin.execute("SELECT action, operator FROM control.guard_log ORDER BY log_id DESC LIMIT 1"
                         ).fetchone() == ("TAKEOVER", "operator")
    out = _file(conn, new_run, new_gen, "rv-stale", [_event()])
    assert out.accepted_new_rows == 1


def test_service_roles_cannot_take_over_or_edit_the_guard(conn, admin):
    run_id, _ = _begin_run(admin, conn, "rv-priv")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("SELECT control.takeover_guard(%s, 'a', 'b')", (run_id,))
    conn.rollback()
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("UPDATE control.run_guard SET fencing_generation = 99")
    conn.rollback()


def test_takeover_requires_operator_and_reason(admin):
    with pytest.raises(psycopg.errors.InvalidParameterValue):
        admin.execute("SELECT control.takeover_guard(%s, '', 'why')", (uuid.uuid4(),))


def test_two_writers_on_the_same_file_commit_exactly_once(own_role_dsn, admin, conn):
    run_id, gen = _begin_run(admin, conn, "rv-race")
    entry = _entry()
    events = [_event(f"o{i}") for i in range(50)]
    a_in_flight, errors, outcomes = threading.Event(), [], []

    def slow(point):
        if point == "before_commit":
            a_in_flight.set()
            time.sleep(1.5)

    def writer(fault):
        with psycopg.connect(own_role_dsn("commerce_ingest")) as c:
            try:
                outcomes.append(_file(c, run_id, gen, "rv-race", events, entry=entry, fault=fault))
            except (ConcurrentWriteError, psycopg.errors.UniqueViolation) as exc:
                errors.append(exc)

    ta = threading.Thread(target=writer, args=(slow,))
    ta.start()
    a_in_flight.wait(5)
    tb = threading.Thread(target=writer, args=(None,))
    tb.start()
    ta.join()
    tb.join()
    assert len(outcomes) == 1 and len(errors) == 1
    assert _count(admin, "SELECT count(*) FROM bronze.source_events WHERE replay_version = 'rv-race'") == 50
    assert _count(admin, "SELECT count(*) FROM control.file_receipts WHERE replay_version = 'rv-race'") == 1
    retry = _file(conn, run_id, gen, "rv-race", events, entry=entry)
    assert retry.already_committed and retry.accepted_new_rows == 50
