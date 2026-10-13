from __future__ import annotations

import platform
import shutil
import sys
import tempfile
import time
from pathlib import Path

import psycopg

from commerce.audit.equality import canonical_report
from commerce.audit.serving_checksum import serving_checksum
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.recovery.demo_db import demo_database
from commerce.replay.dataset import DirSource
from commerce.replay.generator import generate_baseline
from commerce.replay.store import RawStore

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "golden_v1"
WINDOWS = [(0, 7), (7, 14), (14, 21), (21, 28), (28, 31)]


def run_parity(server_dsn: str, *, with_restore: bool = True) -> dict:
    started = time.perf_counter()
    root = Path(tempfile.mkdtemp(prefix="parity-"))
    try:
        csv_dir = shutil.copytree(FIXTURE_DIR, root / "csv", ignore=shutil.ignore_patterns("expected.json"))
        src = DirSource(csv_dir)
        store = RawStore(root / "store")
        generate_baseline(src, store)
        ds = src.digest()
        with demo_database(server_dsn) as db, psycopg.connect(server_dsn, dbname="postgres") as srv:
            pg = srv.execute("SHOW server_version").fetchone()[0]
            env = {"COMMERCE_WAREHOUSE_HOST": str(db["host"]), "COMMERCE_WAREHOUSE_PORT": str(db["port"]),
                   "COMMERCE_WAREHOUSE_DB": db["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
                   "COMMERCE_TRANSFORM_PASSWORD": db["passwords"]["commerce_transform"]}
            w = Wiring(store_root=str(root / "store"), dataset_sha256=ds, replay_version="baseline-v1",
                       artifacts_root=root / "builds", dsn_for=db["role_dsn"], dbt_env=env, exact_fixture=True)
            cal = w.reader().calendar()
            for i, (a, b) in enumerate(WINDOWS):
                out = run_pipeline(w, dag_run_id=f"parity-{i}", mode="manual", interval_start=cal[a].start,
                                   interval_end=cal[b - 1].end)
                if out["status"] not in ("SUCCEEDED", "NO_CHANGE"):
                    raise RuntimeError(f"parity run {a}-{b} ended {out['status']}: {out.get('error')}")
            with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
                sc = serving_checksum(c)
            with psycopg.connect(db["admin_dsn"]) as c:
                schema = c.execute("SELECT schema_name FROM control.releases JOIN control.current_release "
                                   "USING (release_id)").fetchone()[0]
                canon = canonical_report(c, schema)
            result = {"dataset_sha256": ds, "serving_sha256": sc["sha256"], "serving_rows": sc["rows"],
                      "canonical_sha256": canon["overall_sha256"],
                      "canonical_rows": {t: v["rows"] for t, v in canon["tables"].items()}}
            restore = None
            if with_restore and shutil.which("pg_dump"):
                from commerce.recovery.backup import BackupError, backup, restore_drill
                try:
                    m = backup(db["admin_dsn"], root / "backup")
                    restore = restore_drill(server_dsn, root / "backup" / m["dump_file"], m)["outcome"]
                except BackupError as exc:
                    restore = f"SKIPPED: {exc}"
            result["restore_drill"] = restore
    finally:
        shutil.rmtree(root, ignore_errors=True)
    result["environment"] = {"python": platform.python_version(), "platform": platform.platform(),
                             "machine": platform.machine(), "postgresql": pg, "executable": sys.platform,
                             "seconds": round(time.perf_counter() - started, 1)}
    return result


EXPECTED_KEYS = ("dataset_sha256", "serving_sha256", "canonical_sha256")


def differences(result: dict, expected: dict) -> list[str]:
    return [f"{k}: got {result.get(k)} expected {expected.get(k)}" for k in EXPECTED_KEYS
            if result.get(k) != expected.get(k)]
