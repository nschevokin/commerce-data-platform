from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import psycopg
from psycopg import sql

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "src"), str(REPO / "tests" / "unit")]

from replay_helpers import copy_fixture  # noqa: E402

from commerce.recovery.demo_db import demo_database  # noqa: E402
from commerce.replay.dataset import DirSource  # noqa: E402
from commerce.replay.generator import generate_baseline  # noqa: E402
from commerce.replay.reader import ReplayReader  # noqa: E402
from commerce.replay.store import RawStore  # noqa: E402

PORT = 18080


def airflow(env, *args, check=True):
    exe = Path(sys.executable).parent / "airflow"
    return subprocess.run([str(exe), *args], env=env, capture_output=True, text=True, check=check)


def wait_for(fn, timeout, every=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(every)
    return None


def run_state(meta_dsn, dag_run_id):
    with psycopg.connect(meta_dsn) as c:
        row = c.execute("SELECT state FROM dag_run WHERE dag_id = 'commerce_pipeline' AND run_id = %s",
                        (dag_run_id,)).fetchone()
    return row[0] if row else None


def task_rows(meta_dsn, dag_run_id):
    with psycopg.connect(meta_dsn) as c:
        return c.execute(
            "SELECT task_id, map_index, state, try_number, start_date, end_date FROM task_instance "
            "WHERE dag_id = 'commerce_pipeline' AND run_id = %s ORDER BY start_date", (dag_run_id,)).fetchall()


def max_overlap(rows, task_id):
    spans = [(r[4], r[5]) for r in rows if r[0] == task_id and r[4] and r[5]]
    return max((sum(1 for s2, e2 in spans if s2 < e and e2 > s) for s, e in spans), default=0)


def trigger(env, meta_dsn, conf):
    airflow(env, "dags", "trigger", "commerce_pipeline", "--conf", json.dumps(conf))
    with psycopg.connect(meta_dsn) as c:
        return c.execute("SELECT run_id FROM dag_run WHERE dag_id = 'commerce_pipeline' "
                         "ORDER BY id DESC LIMIT 1").fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=REPO / "evidence" / "orchestration" / "scheduler_check.json")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    server = os.environ["COMMERCE_TEST_SERVER_DSN"]
    work = Path(tempfile.mkdtemp(prefix="af_sched_"))
    meta_db = f"commerce_demo_af_{uuid.uuid4().hex[:8]}"
    procs: list[subprocess.Popen] = []
    report: dict = {"airflow": None, "checks": {}}
    with demo_database(server) as wh:
        with psycopg.connect(server, dbname="postgres", autocommit=True) as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(meta_db)))
        info = psycopg.conninfo.conninfo_to_dict(server)
        meta_dsn = psycopg.conninfo.make_conninfo(server, dbname=meta_db)
        csv = DirSource(copy_fixture(work / "csv"))
        store = RawStore(work / "store")
        generate_baseline(csv, store)
        ds = csv.digest()
        cal = ReplayReader(store, ds, "baseline-v1").calendar()
        conf = {"interval_start": cal[0].start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "interval_end": cal[6].end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        env = {**os.environ,
               "AIRFLOW_HOME": str(work / "home"),
               "AIRFLOW__CORE__EXECUTOR": "LocalExecutor", "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
               "AIRFLOW__CORE__DAGS_FOLDER": str(REPO / "dags"),
               "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": f"http://127.0.0.1:{PORT}/execution/",
               "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(
                   user=info["user"], pw=info["password"], host=info.get("host", "127.0.0.1"),
                   port=info.get("port", "5432"), db=meta_db),
               "AIRFLOW__API_AUTH__JWT_SECRET": secrets.token_urlsafe(32),
               "AIRFLOW__API__SECRET_KEY": secrets.token_urlsafe(32),
               "AIRFLOW__API__PORT": str(PORT),
               "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL": "10",
               "PYTHONPATH": str(REPO / "src"),
               "COMMERCE_STORE": str(work / "store"), "COMMERCE_DATASET_SHA256": ds, "COMMERCE_EXACT_FIXTURE": "1",
               "COMMERCE_ARTIFACTS": str(work / "builds"), "COMMERCE_WAREHOUSE_DB": wh["dbname"],
               "COMMERCE_WAREHOUSE_HOST": str(wh["host"]), "COMMERCE_WAREHOUSE_PORT": str(wh["port"]),
               **{f"COMMERCE_{r.upper()}_PASSWORD": wh["passwords"][f"commerce_{r}"]
                  for r in ("ingest", "transform", "publish", "dashboard")}}
        try:
            airflow(env, "db", "migrate")
            airflow(env, "pools", "set", "commerce_ingest", "2", "bounded ingestion")
            airflow(env, "pools", "set", "commerce_transform", "1", "one build at a time")
            report["airflow"] = airflow(env, "version").stdout.strip().splitlines()[-1]
            for name in ("dag-processor", "scheduler", "api-server"):
                log = open(work / f"{name}.log", "w")
                procs.append(subprocess.Popen([str(Path(sys.executable).parent / "airflow"), name], env=env,
                                              stdout=log, stderr=subprocess.STDOUT, start_new_session=True))
            if not wait_for(lambda: "commerce_pipeline" in airflow(env, "dags", "list", "-o", "plain",
                                                                  check=False).stdout, 180):
                raise SystemExit("DAG never appeared (dag-processor)")
            airflow(env, "dags", "unpause", "commerce_pipeline")

            wait_for(lambda: all(r[0] in ("success", "failed") for r in psycopg.connect(meta_dsn).execute(
                "SELECT state FROM dag_run WHERE run_id LIKE 'scheduled__%'").fetchall()), 120)
            with psycopg.connect(meta_dsn) as c:
                sched = c.execute("SELECT state FROM dag_run WHERE run_id LIKE 'scheduled__%'").fetchall()
                sched_ti = c.execute("SELECT try_number FROM task_instance WHERE task_id = 'resolve_run' "
                                     "AND run_id LIKE 'scheduled__%'").fetchall()
            report["checks"]["scheduled_run_outside_calendar"] = {
                "runs": [r[0] for r in sched], "resolve_run_tries": [r[0] for r in sched_ti]}

            rid = trigger(env, meta_dsn, conf)
            state = wait_for(lambda: run_state(meta_dsn, rid) in ("success", "failed") and run_state(meta_dsn, rid), 600)
            rows = task_rows(meta_dsn, rid)
            with psycopg.connect(wh["admin_dsn"]) as c:
                releases = c.execute("SELECT count(*) FROM control.publications WHERE kind = 'PUBLISH'").fetchone()[0]
            report["checks"]["clean_run"] = {
                "dag_run_id": rid, "state": state, "task_states": sorted({r[2] for r in rows}),
                "tasks": len(rows), "published_releases": releases,
                "ingest_max_concurrent": max_overlap(rows, "ingest_source"),
                "ingest_pool_slots": 2,
                "build_max_concurrent": max_overlap(rows, "build_candidate"),
                "max_try_number": max(r[3] for r in rows)}

            cal2 = (cal[7].start, cal[13].end)
            conf2 = {"interval_start": cal2[0].strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "interval_end": cal2[1].strftime("%Y-%m-%dT%H:%M:%SZ")}
            with psycopg.connect(wh["admin_dsn"], autocommit=True) as a:
                a.execute("ALTER ROLE commerce_transform CONNECTION LIMIT 0")
            rid2 = trigger(env, meta_dsn, conf2)

            def failed_once():
                hit = [r for r in task_rows(meta_dsn, rid2) if r[2] in ("up_for_retry", "failed")]
                return hit[0][0] if hit else None
            seen = wait_for(failed_once, 300, 2.0)
            with psycopg.connect(wh["admin_dsn"], autocommit=True) as a:
                a.execute("ALTER ROLE commerce_transform CONNECTION LIMIT -1")
            state2 = wait_for(lambda: run_state(meta_dsn, rid2) in ("success", "failed") and run_state(meta_dsn, rid2), 600)
            rows2 = task_rows(meta_dsn, rid2)
            hit = [r for r in rows2 if r[0] == seen]
            with psycopg.connect(wh["admin_dsn"]) as c:
                releases2 = c.execute("SELECT count(*) FROM control.publications WHERE kind = 'PUBLISH'").fetchone()[0]
            report["checks"]["transient_fault_retry"] = {
                "dag_run_id": rid2, "first_failing_task": seen, "state": state2,
                "failed_task_final_try_number": hit[0][3] if hit else None,
                "published_releases_total": releases2}

            ok1 = report["checks"]["clean_run"]
            ok2 = report["checks"]["transient_fault_retry"]
            ok0 = report["checks"]["scheduled_run_outside_calendar"]
            report["pass"] = bool(
                ok0["runs"] == ["failed"] and ok0["resolve_run_tries"] == [1]
                and ok1["state"] == "success" and ok1["task_states"] == ["success"] and ok1["published_releases"] >= 1
                and ok1["ingest_max_concurrent"] <= 2 and ok1["build_max_concurrent"] <= 1
                and ok2["state"] == "success" and bool(ok2["first_failing_task"])
                and (ok2["failed_task_final_try_number"] or 0) >= 2 and ok2["published_releases_total"] == ok1["published_releases"] + 1)
        finally:
            for p in procs:
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            time.sleep(3)
            for p in procs:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            report["process_logs"] = str(work) if args.keep else "removed"
            with psycopg.connect(server, dbname="postgres", autocommit=True) as c:
                c.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(meta_db)))
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(report, indent=2, default=str) + "\n")
            print(json.dumps(report, indent=2, default=str))
            if not args.keep:
                shutil.rmtree(work, ignore_errors=True)
    return 0 if report.get("pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
