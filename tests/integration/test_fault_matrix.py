from __future__ import annotations

import os

import pytest

from commerce.recovery.demo_db import NotADemoDatabase, assert_demo
from commerce.recovery.matrix import run_matrix

pytestmark = [pytest.mark.integration, pytest.mark.dbt]


def test_matrix_refuses_non_demo_databases():
    for name in ("commerce", "commerce_real", "postgres", "commerce_test_x"):
        with pytest.raises(NotADemoDatabase):
            assert_demo(name)
    assert_demo("commerce_demo_0123456789ab")


def test_every_fault_case_recovers(tmp_path):
    dsn = os.environ.get("COMMERCE_TEST_SERVER_DSN")
    if not dsn:
        pytest.skip("COMMERCE_TEST_SERVER_DSN not set")
    report = run_matrix(dsn, tmp_path)
    failed = {c["case"]: (c["checks"], c["error"]) for c in report["cases"] if c["outcome"] != "PASS"}
    assert not failed, failed
    assert report["summary"]["total"] == 10
