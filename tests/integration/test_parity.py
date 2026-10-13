from __future__ import annotations

import json
from pathlib import Path

import pytest

from commerce.audit.parity import differences, run_parity

pytestmark = [pytest.mark.integration, pytest.mark.dbt]
EXPECTED = Path(__file__).resolve().parents[2] / "evidence" / "parity" / "golden_v1_checksums.json"


def test_this_machine_reproduces_the_committed_checksums(server_dsn):
    res = run_parity(server_dsn, with_restore=True)
    assert differences(res, json.loads(EXPECTED.read_text(encoding="utf-8"))) == []
    assert res["restore_drill"] in ("PASS", None) or str(res["restore_drill"]).startswith("SKIPPED")


def test_a_wrong_expectation_is_reported():
    got = {"dataset_sha256": "a", "serving_sha256": "b", "canonical_sha256": "c"}
    assert len(differences(got, {"dataset_sha256": "a", "serving_sha256": "x", "canonical_sha256": "y"})) == 2
