from __future__ import annotations

import pytest
from replay_helpers import copy_fixture

from commerce.benchmark.run import run_benchmark
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.store import RawStore

pytestmark = [pytest.mark.integration, pytest.mark.dbt]


def test_benchmark_sections_and_invariants_on_the_fixture(server_dsn, tmp_path):
    src = DirSource(copy_fixture(tmp_path / "csv"))
    store = RawStore(tmp_path / "store")
    generate_baseline(src, store)
    r = run_benchmark(server_dsn, str(tmp_path / "store"), src.digest(), tmp_path / "w", exact_fixture=True,
                      daily_intervals=4)
    assert r["equality_daily_vs_baseline"]["canonical_sha256_equal"] is True
    assert r["equality_daily_vs_baseline"]["serving_checksum_equal"] is True
    rec = r["recovery"]
    assert rec["failed_run_status"] == "FAILED_TRANSIENT" and rec["serving_unchanged_while_failed"] is True
    assert rec["resume_status"] == "SUCCEEDED"
    assert rec["generation_during_failure"] == rec["generation_before"]
    assert rec["generation_after_resume"] == rec["generation_before"] + 1
    dup = r["duplicate_replay"]
    assert dup["status"] == "NO_CHANGE" and dup["new_bronze_events"] == 0 and dup["new_receipts"] == 0
    assert dup["checksum_unchanged"] is True
    assert r["daily_replay"]["runs"] == 3 and r["daily_replay"]["warm_run_seconds"]["n"] == 2
    assert {q["query"] for q in r["queries"]} >= {"sales_daily full view", "category_daily full view"}
    assert r["baseline"]["sizes"]["bronze_events"] > 0
