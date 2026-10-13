from __future__ import annotations

import json
import os
import platform
import statistics
import threading
import time
from pathlib import Path

import psycopg

from commerce.audit.equality import canonical_report
from commerce.audit.serving_checksum import serving_checksum
from commerce.orchestration.local import run_pipeline
from commerce.orchestration.wiring import Wiring
from commerce.recovery.demo_db import demo_database


def _p95(xs: list[float]) -> float:
    xs = sorted(xs)
    return xs[max(0, int(round(0.95 * len(xs))) - 1)]


def _stats(xs: list[float]) -> dict:
    return {"n": len(xs), "median": round(statistics.median(xs), 3), "p95": round(_p95(xs), 3),
            "min": round(min(xs), 3), "max": round(max(xs), 3)}


def _wiring(db: dict, store: str, ds: str, root: Path, exact: bool, faults=None) -> Wiring:
    env = {"COMMERCE_WAREHOUSE_HOST": str(db["host"]), "COMMERCE_WAREHOUSE_PORT": str(db["port"]),
           "COMMERCE_WAREHOUSE_DB": db["dbname"], "COMMERCE_TRANSFORM_USER": "commerce_transform",
           "COMMERCE_TRANSFORM_PASSWORD": db["passwords"]["commerce_transform"]}
    return Wiring(store_root=store, dataset_sha256=ds, replay_version="baseline-v1",
                  artifacts_root=root / "builds", dsn_for=db["role_dsn"], dbt_env=env, exact_fixture=exact,
                  faults=faults if faults is not None else {})


def _run(w: Wiring, name: str, a: int, b: int) -> tuple[dict, float]:
    cal = w.reader().calendar()
    t = time.perf_counter()
    out = run_pipeline(w, dag_run_id=name, mode="manual", interval_start=cal[a].start,
                       interval_end=cal[b - 1].end)
    return out, time.perf_counter() - t


def _stage_seconds(admin: str, run_id: str) -> dict[str, float]:
    with psycopg.connect(admin) as c:
        rows = c.execute(
            "SELECT stage, sum(extract(epoch FROM finished_at - started_at)) FROM control.stage_results "
            "WHERE run_id = %s::uuid AND status IN ('SUCCEEDED', 'SKIPPED') AND finished_at IS NOT NULL "
            "GROUP BY stage", (run_id,)).fetchall()
    return {r[0]: float(r[1]) for r in rows}


class MemorySampler:

    def __init__(self) -> None:
        self.app_peak = self.pg_peak = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.supported = Path("/proc/self/smaps_rollup").exists()

    @staticmethod
    def _pss_kb(pid: str) -> int:
        try:
            for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
                if line.startswith("Pss:"):
                    return int(line.split()[1])
        except OSError:
            pass
        return 0

    def _sample(self) -> None:
        me = os.getpid()
        parent: dict[int, int] = {}
        comm: dict[int, str] = {}
        for d in Path("/proc").iterdir():
            if d.name.isdigit():
                try:
                    stat = (d / "stat").read_text()
                    comm[int(d.name)] = stat[stat.index("(") + 1:stat.rindex(")")]
                    parent[int(d.name)] = int(stat[stat.rindex(")") + 2:].split()[1])
                except (OSError, ValueError):
                    continue
        tree = {me}
        grew = True
        while grew:
            grew = False
            for pid, ppid in parent.items():
                if ppid in tree and pid not in tree:
                    tree.add(pid)
                    grew = True
        app = sum(self._pss_kb(str(p)) for p in tree) / 1024
        pg = sum(self._pss_kb(str(p)) for p, c in comm.items() if c.startswith("postgres")) / 1024
        self.app_peak, self.pg_peak = max(self.app_peak, app), max(self.pg_peak, pg)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(0.25)

    def __enter__(self):
        if self.supported:
            self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        return False

    def result(self) -> dict | None:
        if not self.supported:
            return None
        return {"application_tree_peak_pss": round(self.app_peak, 1),
                "postgresql_peak_pss": round(self.pg_peak, 1),
                "method": "sum of Pss over the process tree, sampled every 0.25 s; includes dbt "
                          "subprocesses"}


def _sizes(admin: str) -> dict:
    with psycopg.connect(admin) as c:
        db = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        schema = c.execute("SELECT schema_name FROM control.releases JOIN control.current_release "
                           "USING (release_id)").fetchone()[0]
        bronze = c.execute("SELECT pg_total_relation_size('bronze.source_events')").fetchone()[0]
        cand = c.execute("SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0) FROM pg_class c JOIN "
                         "pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relkind = 'r'",
                         (schema,)).fetchone()[0]
        events = c.execute("SELECT count(*) FROM bronze.source_events").fetchone()[0]
        candidates = c.execute("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'cand\\_%'").fetchone()[0]
    return {"database_bytes": db, "bronze_events_bytes": bronze, "served_candidate_bytes": int(cand),
            "bronze_events": events, "candidate_schemas_retained": candidates}


def _meminfo_gb() -> float | None:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                return round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        return None
    return None


def _store_bytes(store: str) -> int:
    return sum(p.stat().st_size for p in Path(store).rglob("*") if p.is_file())


def _explain(role_dsn: str) -> list[dict]:
    queries = {
        "sales_daily full view": "SELECT * FROM serving.sales_daily",
        "sales_daily one quarter": "SELECT * FROM serving.sales_daily WHERE purchase_date "
                                   "BETWEEN '2017-10-01' AND '2017-12-31'",
        "category_daily full view": "SELECT * FROM serving.category_daily",
        "category ranking, one category": "SELECT category_label, sum(merchandise_value_brl) "
                                          "FROM serving.category_daily WHERE category_label = 'health_beauty' "
                                          "GROUP BY 1",
        "delivery_cohorts full view": "SELECT * FROM serving.delivery_cohorts",
        "pipeline_health full view": "SELECT * FROM serving.pipeline_health"}
    out = []
    with psycopg.connect(role_dsn) as c:
        for name, q in queries.items():
            runs = []
            for _ in range(5):
                plan = c.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + q).fetchone()[0][0]
                runs.append((plan["Execution Time"], plan["Planning Time"]))
            node = plan["Plan"]
            out.append({"query": name, "execution_ms_median": round(statistics.median(r[0] for r in runs), 3),
                        "planning_ms_median": round(statistics.median(r[1] for r in runs), 3),
                        "node": node["Node Type"], "rows": node["Actual Rows"],
                        "shared_hit_blocks": plan["Plan"].get("Shared Hit Blocks"), "samples": 5})
    return out


def run_benchmark(server_dsn: str, store: str, dataset_sha256: str, workdir: Path, *,
                  exact_fixture: bool = False, daily_intervals: int | None = None) -> dict:
    workdir.mkdir(parents=True, exist_ok=True)
    cal_len = len(_wiring({"host": "", "port": "", "dbname": "", "passwords": {"commerce_transform": ""},
                           "role_dsn": lambda r: ""}, store, dataset_sha256, workdir, exact_fixture)
                  .reader().calendar())
    n = cal_len if daily_intervals is None else min(cal_len, daily_intervals)
    report: dict = {"what": "Blueprint §33 local experiments", "intervals_in_replay": cal_len}
    t_all = time.perf_counter()
    with psycopg.connect(server_dsn, dbname="postgres") as c:
        pg = c.execute("SHOW server_version").fetchone()[0]
    report["environment"] = {"python": platform.python_version(), "platform": platform.platform(),
                             "machine": platform.machine(), "cpus": os.cpu_count(), "memory_gb": _meminfo_gb(),
                             "postgresql": pg, "dataset_sha256": dataset_sha256,
                             "raw_store_bytes": _store_bytes(store),
                             "note": "database and application on the same machine; nothing else tuned"}

    sampler = MemorySampler()
    sampler.__enter__()
    with demo_database(server_dsn) as db:
        w = _wiring(db, store, dataset_sha256, workdir / "baseline", exact_fixture)
        t = time.perf_counter()
        runs, i = [], 0
        while i < n:
            j = min(i + 7, n)
            out, secs = _run(w, f"bench-base-{i}", i, j)
            assert out["status"] in ("SUCCEEDED", "NO_CHANGE"), out
            runs.append({"intervals": f"{i}-{j}", "seconds": round(secs, 2), "status": out["status"],
                         "stages": {k: round(v, 2)
                                    for k, v in _stage_seconds(db["admin_dsn"], out["run_id"]).items()}})
            i = j
        wall = time.perf_counter() - t
        with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
            base_checksum = serving_checksum(c)
        with psycopg.connect(db["admin_dsn"]) as c:
            schema = c.execute("SELECT schema_name FROM control.releases JOIN control.current_release "
                               "USING (release_id)").fetchone()[0]
            base_canonical = canonical_report(c, schema)
            receipts = c.execute("SELECT coalesce(sum(input_rows), 0), count(*) FROM control.file_receipts"
                                 ).fetchone()
        report["baseline"] = {"serial_ranges": runs, "wall_seconds": round(wall, 1),
                              "source_rows": int(receipts[0]), "files": int(receipts[1]),
                              "rows_per_second_end_to_end": round(int(receipts[0]) / wall),
                              "sizes": _sizes(db["admin_dsn"]),
                              "serving_checksum": base_checksum}

    with demo_database(server_dsn) as db:
        faults: dict = {}
        w = _wiring(db, store, dataset_sha256, workdir / "daily", exact_fixture, faults)
        per_run = []
        for k in range(n - 1):
            out, secs = _run(w, f"bench-day-{k}", k, k + 1)
            assert out["status"] in ("SUCCEEDED", "NO_CHANGE"), out
            with psycopg.connect(db["admin_dsn"]) as c:
                rows = c.execute("SELECT coalesce(sum(input_rows), 0) FROM control.file_receipts "
                                 "WHERE interval_start = %s", (w.reader().calendar()[k].start,)).fetchone()[0]
            per_run.append({"interval": k, "seconds": secs, "input_rows": int(rows),
                            "stages": _stage_seconds(db["admin_dsn"], out["run_id"])})
        stage_names = sorted({s for r in per_run for s in r["stages"]})
        warm = per_run[1:]
        report["daily_replay"] = {
            "runs": len(per_run), "cold_start_seconds": round(per_run[0]["seconds"], 2),
            "warm_run_seconds": _stats([r["seconds"] for r in warm]),
            "input_rows_per_day": _stats([float(r["input_rows"]) for r in per_run]),
            "stage_seconds_warm": {s: _stats([r["stages"].get(s, 0.0) for r in warm]) for s in stage_names},
            "note": "run 1 is the cold start (empty database, first dbt build); statistics use runs 2..n; "
                    "each day's build is complete over all data so far, so later days cost more"}
        with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
            before = serving_checksum(c)

        faults["publish"] = "before_commit"
        t0 = time.perf_counter()
        failed, f_secs = _run(w, "bench-recover", n - 1, n)
        faults.clear()
        with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
            during = serving_checksum(c)
        resumed, r_secs = _run(w, "bench-recover", n - 1, n)
        restored_total = time.perf_counter() - t0
        with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
            after = serving_checksum(c)
        report["recovery"] = {
            "scenario": "publication transaction aborted before commit on the last interval, same run resumed",
            "failed_run_status": failed["status"], "failed_run_seconds": round(f_secs, 2),
            "serving_unchanged_while_failed": during["sha256"] == before["sha256"],
            "resume_status": resumed["status"], "resume_seconds": round(r_secs, 2),
            "fault_to_restored_seconds": round(restored_total, 2),
            "serving_changed_after_resume": after["sha256"] != before["sha256"],
            "generation_before": before["generation"], "generation_during_failure": during["generation"],
            "generation_after_resume": after["generation"]}

        with psycopg.connect(db["admin_dsn"]) as c:
            ev0 = c.execute("SELECT count(*) FROM bronze.source_events").fetchone()[0]
            rec0 = c.execute("SELECT count(*) FROM control.file_receipts").fetchone()[0]
        out, d_secs = _run(w, "bench-duplicate", n - 1, n)
        with psycopg.connect(db["admin_dsn"]) as c:
            ev1 = c.execute("SELECT count(*) FROM bronze.source_events").fetchone()[0]
            rec1 = c.execute("SELECT count(*) FROM control.file_receipts").fetchone()[0]
        with psycopg.connect(db["role_dsn"]("commerce_dashboard")) as c:
            dup_after = serving_checksum(c)
        report["duplicate_replay"] = {
            "status": out["status"], "seconds": round(d_secs, 2), "new_bronze_events": ev1 - ev0,
            "new_receipts": rec1 - rec0, "checksum_unchanged": dup_after["sha256"] == after["sha256"]}

        with psycopg.connect(db["admin_dsn"]) as c:
            schema = c.execute("SELECT schema_name FROM control.releases JOIN control.current_release "
                               "USING (release_id)").fetchone()[0]
            daily_canonical = canonical_report(c, schema)
        report["equality_daily_vs_baseline"] = {
            "canonical_sha256_equal": daily_canonical["overall_sha256"] == base_canonical["overall_sha256"],
            "serving_checksum_equal": after["sha256"] == base_checksum["sha256"],
            "baseline_canonical_sha256": base_canonical["overall_sha256"],
            "daily_canonical_sha256": daily_canonical["overall_sha256"],
            "tables": {t: v["rows"] for t, v in daily_canonical["tables"].items()}}
        report["queries"] = _explain(db["role_dsn"]("commerce_dashboard"))
        report["daily_sizes"] = _sizes(db["admin_dsn"])
    sampler.__exit__()
    report["peak_memory_mb"] = sampler.result()
    report["total_wall_seconds"] = round(time.perf_counter() - t_all, 1)
    return report


def write(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
