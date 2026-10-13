from __future__ import annotations

import argparse
import json
import sys

from commerce import config


def _migrate(_: argparse.Namespace) -> int:
    from commerce.migrate import apply_all

    applied = apply_all(config.admin_dsn())
    print(json.dumps({"event": "migrations_applied", "applied": applied}))
    return 0


def _bootstrap_roles(_: argparse.Namespace) -> int:
    from commerce.roles import bootstrap_logins

    roles = bootstrap_logins(config.admin_dsn())
    print(json.dumps({"event": "role_logins_bootstrapped", "roles": roles}))
    return 0


def _fixture_expected(args: argparse.Namespace) -> int:
    from commerce.fixture.reference import compute_from_dir

    print(json.dumps(compute_from_dir(args.fixture_dir), indent=2, sort_keys=True))
    return 0


def _fixture_slice(args: argparse.Namespace) -> int:
    import os
    from pathlib import Path

    from commerce.fixture.harness import run_slice

    repo = Path(__file__).resolve().parents[2]
    fixture_dir = Path(args.fixture_dir).resolve()
    transform_env = {
        "COMMERCE_TRANSFORM_PASSWORD": config.require("COMMERCE_TRANSFORM_PASSWORD"),
        **{k: v for k, v in os.environ.items() if k.startswith("COMMERCE_WAREHOUSE_")},
    }
    result = run_slice(admin_dsn=config.admin_dsn(), transform_env=transform_env,
                       fixture_dir=fixture_dir, artifacts_root=repo / "artifacts" / "builds")
    out = repo / "evidence" / "fixture" / f"{fixture_dir.name}_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = {k: result[k] for k in ("passed", "candidate_schema", "dbt",
                                      "sql_vs_oracle_diffs", "python_reference_vs_oracle_diffs")}
    summary["eligible"] = result["observed"]["eligible"]
    summary["evidence"] = str(out.relative_to(repo))
    print(json.dumps(summary, indent=2))
    return 0 if result["passed"] else 1


def _dump(obj) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False, default=str))


def _import_data(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.contracts.canonical import canonical_json
    from commerce.replay.dataset import ZipSource, inventory
    from commerce.replay.profile import profile_dataset
    from commerce.replay.store import RawStore

    src = ZipSource(args.archive)
    store = RawStore(args.store)
    inv = inventory(src, retrieved_on=args.retrieved_on, dataset_url=args.dataset_url,
                    licence=args.licence)
    prof = profile_dataset(src)
    prefix = f"archive/dataset={inv['dataset_sha256']}"
    original = f"{prefix}/original/{Path(args.archive).name}"
    created = store.put_bytes(original, Path(args.archive).read_bytes())
    store.put_bytes(f"{prefix}/inventory.json", canonical_json(inv))
    store.put_bytes(f"{prefix}/profile.json", canonical_json(prof))
    _dump({"dataset_sha256": inv["dataset_sha256"], "original_stored": created, "files": len(inv["files"]),
           "deferred_files": [f["name"] for f in inv["files"] if f["role"] == "deferred"],
           "store_prefix": prefix, "profile_checks": prof["checks"]})
    return 0


def _profile(args: argparse.Namespace) -> int:
    from commerce.replay.dataset import open_source
    from commerce.replay.profile import profile_dataset

    _dump(profile_dataset(open_source(args.source)))
    return 0


def _replay_baseline(args: argparse.Namespace) -> int:
    from datetime import date

    from commerce.replay.dataset import open_source
    from commerce.replay.generator import ReplayConfig, generate_baseline
    from commerce.replay.store import RawStore

    cfg = ReplayConfig(replay_version=args.version, n_days=args.days,
                       start_date=date.fromisoformat(args.start_date), allow_orphans=args.allow_orphans)
    _dump(generate_baseline(open_source(args.source), RawStore(args.store), cfg))
    return 0


def _replay_scenario(args: argparse.Namespace) -> int:
    from commerce.replay.scenarios import build_scenario
    from commerce.replay.store import RawStore

    kind, _, variant = args.kind.partition(":")
    _dump(build_scenario(RawStore(args.store), dataset_sha256=args.dataset_sha256,
                         base_version=args.base, kind=kind, seed=args.seed, count=args.count,
                         order_ids=args.order_id or None, variant=variant or None,
                         new_version=args.version))
    return 0


def _replay_verify(args: argparse.Namespace) -> int:
    from commerce.replay.reader import ReplayReader
    from commerce.replay.store import RawStore

    _dump(ReplayReader(RawStore(args.store), args.dataset_sha256, args.version).verify())
    return 0


def _ingest(args: argparse.Namespace) -> int:
    import psycopg

    from commerce import runinfo
    from commerce.ingestion.runner import ingest_range
    from commerce.replay.reader import ReplayReader
    from commerce.replay.store import RawStore

    store = RawStore(args.store)
    reader = ReplayReader(store, args.dataset_sha256, args.version)
    with psycopg.connect(config.role_dsn("commerce_ingest")) as conn:
        results = ingest_range(conn, store, reader, code_sha=runinfo.code_sha(),
                               dependency_lock_sha=runinfo.dependency_lock_sha(),
                               start_index=args.from_index, end_index=args.to_index)
    _dump([r.to_obj() for r in results])
    return 0 if results and all(r.status == "INGESTED" for r in results) else 1


def _build_candidate(args: argparse.Namespace) -> int:
    import os
    from pathlib import Path

    from commerce import runinfo
    from commerce.build.candidate import build_candidate, input_digest, summarise_candidate
    from commerce.contracts.canonical import utc_iso
    from commerce.replay.reader import ReplayReader
    from commerce.replay.store import RawStore

    repo = Path(__file__).resolve().parents[2]
    reader = ReplayReader(RawStore(args.store), args.dataset_sha256, args.version)
    cal = reader.calendar()
    idx = len(cal) - 1 if args.frontier_index is None else args.frontier_index
    transform_env = {
        "COMMERCE_TRANSFORM_PASSWORD": config.require("COMMERCE_TRANSFORM_PASSWORD"),
        **{k: v for k, v in os.environ.items() if k.startswith("COMMERCE_WAREHOUSE_")},
    }
    out = build_candidate(transform_dsn=config.role_dsn("commerce_transform"), transform_env=transform_env,
                          reader=reader, frontier_end=cal[idx].end,
                          artifacts_root=repo / "artifacts" / "builds", exact_fixture=args.exact_fixture)
    rep = out["report"]
    summary = {"candidate_schema": out["candidate_schema"], "frontier_end": utc_iso(cal[idx].end),
               "intervals": out["frontier"].intervals,
               "input_digest": input_digest(dataset_sha256=args.dataset_sha256, frontier=out["frontier"],
                                            code_sha=runinfo.code_sha(),
                                            dependency_lock_sha=runinfo.dependency_lock_sha()),
               "dbt": {"ok": rep.ok, "executed": rep.executed, "expected": rep.expected,
                       "warnings": rep.warnings, "failures": rep.failures}}
    if rep.ok:
        summary["summary"] = summarise_candidate(config.admin_dsn(), out["candidate_schema"])
    _dump(summary)
    return 0 if rep.ok else 1


def _crosscheck(args: argparse.Namespace) -> int:
    from commerce.build.candidate import crosscheck_archive
    from commerce.replay.dataset import open_source

    result = crosscheck_archive(open_source(args.source), config.admin_dsn(), args.candidate_schema)
    _dump(result)
    return 0 if result["difference_count"] == 0 else 1


def _guard(args: argparse.Namespace) -> int:
    import uuid

    import psycopg

    with psycopg.connect(config.admin_dsn()) as conn:
        if args.action == "status":
            owner, gen = conn.execute("SELECT owner_run_id, fencing_generation FROM control.run_guard"
                                      ).fetchone()
            status = conn.execute("SELECT status FROM control.runs WHERE run_id = %s", (owner,)
                                  ).fetchone() if owner else None
            _dump({"owner_run_id": str(owner) if owner else None, "fencing_generation": gen,
                   "owner_run_status": status[0] if status else None})
            return 0
        if not (args.operator and args.reason):
            print("takeover needs --operator and --reason (confirm the old run is terminal first)",
                  file=sys.stderr)
            return 2
        run_id = uuid.uuid4()
        with conn.transaction():
            conn.execute(
                "INSERT INTO control.runs (run_id, mode, replay_version, interval_start, interval_end, "
                "code_sha, dependency_lock_sha, status) VALUES (%s, 'audit', 'operator-takeover', "
                "now(), now() + interval '1 second', 'operator', 'operator', 'CREATED')", (run_id,))
            gen = conn.execute("SELECT control.takeover_guard(%s, %s, %s)",
                               (run_id, args.operator, args.reason)).fetchone()[0]
            conn.execute("SELECT control.release_guard(%s, %s)", (run_id, gen))
        _dump({"takeover_run_id": str(run_id), "released_at_generation": gen})
        return 0


def _release(args: argparse.Namespace) -> int:
    import os
    from pathlib import Path

    from commerce.contracts.canonical import utc_iso
    from commerce.release.pipeline import run_release
    from commerce.replay.reader import ReplayReader
    from commerce.replay.store import RawStore

    repo = Path(__file__).resolve().parents[2]
    reader = ReplayReader(RawStore(args.store), args.dataset_sha256, args.version)
    cal = reader.calendar()
    idx = len(cal) - 1 if args.frontier_index is None else args.frontier_index
    transform_env = {
        "COMMERCE_TRANSFORM_PASSWORD": config.require("COMMERCE_TRANSFORM_PASSWORD"),
        **{k: v for k, v in os.environ.items() if k.startswith("COMMERCE_WAREHOUSE_")},
    }
    out = run_release(
        publish_dsn=config.role_dsn("commerce_publish"), transform_dsn=config.role_dsn("commerce_transform"),
        dashboard_dsn=config.role_dsn("commerce_dashboard"), transform_env=transform_env, reader=reader,
        frontier_end=cal[idx].end, artifacts_root=repo / "artifacts" / "builds",
        exact_fixture=args.exact_fixture, dataset_sha256=args.dataset_sha256)
    _dump({**out.to_obj(), "frontier_end": utc_iso(cal[idx].end)})
    return 0 if out.status in ("SUCCEEDED", "NO_CHANGE") else 1


def _rollback(args: argparse.Namespace) -> int:
    import psycopg

    from commerce.release.pipeline import run_rollback
    from commerce.release.publisher import previous_validated_release

    target = args.to_release
    if target is None:
        with psycopg.connect(config.role_dsn("commerce_publish")) as conn:
            target = previous_validated_release(conn)
        if target is None:
            print("no previous validated release to roll back to", file=sys.stderr)
            return 2
    res = run_rollback(publish_dsn=config.role_dsn("commerce_publish"), target_release_id=target,
                       operator=args.operator, reason=args.reason, incident=args.incident)
    _dump({"outcome": res.outcome.value, "release_id": res.release_id, "generation": res.generation,
           "previous_release_id": res.previous_release_id, "kind": res.kind})
    return 0


def _serving_status(_: argparse.Namespace) -> int:
    import psycopg

    from commerce.serving.reader import read_snapshot

    with psycopg.connect(config.role_dsn("commerce_dashboard")) as conn:
        snap = read_snapshot(conn)
    _dump({"release": snap.release, "rows": {k: len(v) for k, v in snap.tables.items()}})
    return 0


def _pipeline_run(args: argparse.Namespace) -> int:
    import uuid

    from commerce.orchestration.local import run_pipeline
    from commerce.orchestration.wiring import Wiring

    w = Wiring.from_env(replay_version=args.version)
    cal = w.reader().calendar()
    end = len(cal) if args.to_index is None else args.to_index
    tag = args.run_tag or uuid.uuid4().hex[:8]
    results, i = [], args.from_index
    while i < end:
        j = min(i + 7, end)
        out = run_pipeline(w, dag_run_id=f"cli-{tag}-{i}-{j}", mode="manual",
                           interval_start=cal[i].start, interval_end=cal[j - 1].end)
        results.append({"from": i, "to": j, **out})
        if out["status"] not in ("SUCCEEDED", "NO_CHANGE"):
            break
        i = j
    _dump({"runs": results})
    return 0 if results and results[-1]["status"] in ("SUCCEEDED", "NO_CHANGE") else 1


def _backfill(args: argparse.Namespace) -> int:
    from commerce.orchestration.backfill import inventory, run_backfill
    from commerce.orchestration.window import parse_utc
    from commerce.orchestration.wiring import Wiring

    w = Wiring.from_env(replay_version=args.version)
    start, end = parse_utc(args.start, "start"), parse_utc(args.end, "end")
    if args.dry_run:
        _dump(inventory(w, start, end))
        return 0
    results = run_backfill(w, start, end, mode=args.mode, tag=args.run_tag or "cli")
    _dump({"mode": args.mode, "runs": results})
    return 0 if results and results[-1]["status"] in ("SUCCEEDED", "NO_CHANGE") else 1


def _fault_matrix(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.recovery.matrix import run_matrix, write_report

    report = run_matrix(args.server_dsn, Path(args.workdir), only=args.only or None)
    write_report(report, Path(args.out))
    for c in report["cases"]:
        print(f'{c["case"]} {c["outcome"]:4} {c["expectation"]}')
    print(json.dumps(report["summary"]))
    return 0 if report["summary"]["passed"] == report["summary"]["total"] else 1


def _serving_checksum(_: argparse.Namespace) -> int:
    import psycopg

    from commerce.audit.serving_checksum import serving_checksum

    with psycopg.connect(config.role_dsn("commerce_dashboard")) as conn:
        _dump(serving_checksum(conn))
    return 0


def _backup(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.recovery.backup import backup

    _dump(backup(config.admin_dsn(), Path(args.out)))
    return 0


def _restore_drill(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.recovery.backup import restore_drill

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    res = restore_drill(args.server_dsn, Path(args.dump), manifest)
    _dump(res)
    return 0 if res["outcome"] == "PASS" else 1


def _benchmark(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.benchmark.run import run_benchmark, write

    report = run_benchmark(args.server_dsn, args.store, args.dataset_sha256, Path(args.workdir),
                           exact_fixture=args.exact_fixture, daily_intervals=args.intervals)
    write(report, Path(args.out))
    print(json.dumps({k: report[k] for k in ("total_wall_seconds", "equality_daily_vs_baseline")}, default=str))
    return 0


def _candidates_gc(args: argparse.Namespace) -> int:
    from commerce.release.retention import RetentionRefused, run_gc

    try:
        res = run_gc(config.admin_dsn(), keep_published=args.keep_published, failed_days=args.failed_days,
                     holds=set(args.hold or []), dry_run=not args.apply)
    except RetentionRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    _dump(res)
    return 0


def _parity(args: argparse.Namespace) -> int:
    from pathlib import Path

    from commerce.audit.parity import differences, run_parity

    res = run_parity(args.server_dsn, with_restore=not args.no_restore)
    exp_path = Path(args.expected)
    if args.write:
        exp_path.parent.mkdir(parents=True, exist_ok=True)
        exp_path.write_text(json.dumps(res, indent=2, sort_keys=True), encoding="utf-8")
        _dump(res)
        return 0
    diff = differences(res, json.loads(exp_path.read_text(encoding="utf-8")))
    _dump({**res, "differences": diff, "matches_committed": not diff})
    return 1 if diff else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="commerce")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply pending warehouse migrations").set_defaults(fn=_migrate)
    sub.add_parser("bootstrap-roles", help="grant LOGIN to service roles").set_defaults(
        fn=_bootstrap_roles
    )
    fx = sub.add_parser("fixture-expected", help="Decimal reference totals for a fixture dir")
    fx.add_argument("fixture_dir")
    fx.set_defaults(fn=_fixture_expected)
    fs = sub.add_parser("fixture-slice",
                        help="load fixture, dbt-build a candidate, compare with expected.json")
    fs.add_argument("fixture_dir", nargs="?", default="tests/fixtures/golden_v1")
    fs.set_defaults(fn=_fixture_slice)

    imp = sub.add_parser("import-data", help="inventory + profile the Olist archive, keep the original")
    imp.add_argument("--archive", required=True)
    imp.add_argument("--retrieved-on", required=True, help="YYYY-MM-DD you downloaded it")
    imp.add_argument("--licence", required=True, help="licence text exactly as shown on the dataset page")
    imp.add_argument("--dataset-url", default="https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
    imp.add_argument("--store", default="data/store")
    imp.set_defaults(fn=_import_data)

    pr = sub.add_parser("profile", help="print the source profile of a zip or directory")
    pr.add_argument("source")
    pr.set_defaults(fn=_profile)

    rp = sub.add_parser("replay", help="deterministic replay generation and verification")
    rsub = rp.add_subparsers(dest="rcmd", required=True)
    b = rsub.add_parser("baseline", help="partition a dataset into bootstrap + N replay days")
    b.add_argument("--source", required=True, help="Olist zip or a directory of CSVs")
    b.add_argument("--store", default="data/store")
    b.add_argument("--version", default="baseline-v1")
    b.add_argument("--days", type=int, default=30)
    b.add_argument("--start-date", default="2026-01-02")
    b.add_argument("--allow-orphans", action="store_true")
    b.set_defaults(fn=_replay_baseline)
    sc = rsub.add_parser("scenario", help="derive a duplicate/late/correction/fault replay")
    sc.add_argument("--store", default="data/store")
    sc.add_argument("--dataset-sha256", required=True)
    sc.add_argument("--base", default="baseline-v1")
    sc.add_argument("--kind", required=True,
                    help="duplicate | late | correction | fault:<malformed_money|malformed_timestamp|"
                         "unknown_status|conflicting_payload|orphan_item>")
    sc.add_argument("--seed", type=int, default=42)
    sc.add_argument("--count", type=int, default=1)
    sc.add_argument("--order-id", action="append", help="explicit order (repeatable); overrides seeded choice")
    sc.add_argument("--version", default=None)
    sc.set_defaults(fn=_replay_scenario)
    vf = rsub.add_parser("verify", help="re-verify every manifest, checksum and row count")
    vf.add_argument("--store", default="data/store")
    vf.add_argument("--dataset-sha256", required=True)
    vf.add_argument("--version", default="baseline-v1")
    vf.set_defaults(fn=_replay_verify)

    ig = sub.add_parser("ingest", help="ingest replay intervals into the bronze ledger (writer role)")
    ig.add_argument("--store", default="data/store")
    ig.add_argument("--dataset-sha256", required=True)
    ig.add_argument("--version", default="baseline-v1")
    ig.add_argument("--from-index", type=int, default=0, help="first calendar interval (0 = bootstrap)")
    ig.add_argument("--to-index", type=int, default=None, help="end (exclusive)")
    ig.set_defaults(fn=_ingest)

    bc = sub.add_parser("build-candidate", help="dbt-build an isolated candidate from bronze")
    bc.add_argument("--store", default="data/store")
    bc.add_argument("--dataset-sha256", required=True)
    bc.add_argument("--version", default="baseline-v1")
    bc.add_argument("--frontier-index", type=int, default=None,
                    help="calendar interval whose end is the frontier (default: last)")
    bc.add_argument("--exact-fixture", action="store_true",
                    help="payment/item gaps block (golden fixture only)")
    bc.set_defaults(fn=_build_candidate)

    cc = sub.add_parser("crosscheck", help="compare a candidate with the independent reference from the CSVs")
    cc.add_argument("--source", required=True, help="Olist zip or directory")
    cc.add_argument("--candidate-schema", required=True)
    cc.set_defaults(fn=_crosscheck)

    rl = sub.add_parser("release", help="build, validate, publish and verify one release")
    rl.add_argument("--store", default="data/store")
    rl.add_argument("--dataset-sha256", required=True)
    rl.add_argument("--version", default="baseline-v1")
    rl.add_argument("--frontier-index", type=int, default=None)
    rl.add_argument("--exact-fixture", action="store_true")
    rl.set_defaults(fn=_release)

    rb = sub.add_parser("rollback", help="operator rollback to a previously validated release")
    rb.add_argument("--to-release", default=None, help="release id (default: previous validated)")
    rb.add_argument("--operator", required=True)
    rb.add_argument("--reason", required=True)
    rb.add_argument("--incident", default=None)
    rb.set_defaults(fn=_rollback)

    sub.add_parser("serving-status", help="what the dashboard role currently sees").set_defaults(
        fn=_serving_status)

    pr = sub.add_parser("pipeline", help="run the DAG's stages in-process over serial ranges (<= 7 intervals)")
    pr.add_argument("action", choices=["run"])
    pr.add_argument("--version", default=None, help="replay version (default COMMERCE_REPLAY_VERSION)")
    pr.add_argument("--from-index", type=int, default=0)
    pr.add_argument("--to-index", type=int, default=None, help="end (exclusive)")
    pr.add_argument("--run-tag", default=None, help="stable tag so a re-invocation resumes the same runs")
    pr.set_defaults(fn=_pipeline_run)

    bf = sub.add_parser("backfill", help="bounded backfill: UTC start/end, publish or audit-only, --dry-run")
    bf.add_argument("--start", required=True, help="UTC, ends in Z")
    bf.add_argument("--end", required=True, help="UTC, ends in Z")
    bf.add_argument("--version", default=None)
    bf.add_argument("--mode", choices=["publish", "audit"], default="publish")
    bf.add_argument("--dry-run", action="store_true", help="inventory only; writes nothing")
    bf.add_argument("--run-tag", default=None, help="stable tag so re-invocation resumes the same runs")
    bf.set_defaults(fn=_backfill)

    fm = sub.add_parser("fault-matrix", help="run the §26 fault-injection matrix in a throwaway "
                                             "commerce_demo_* database (never touches other databases)")
    fm.add_argument("--server-dsn", required=True, help="superuser DSN (creates/drops the demo DB)")
    fm.add_argument("--workdir", default="artifacts/fault-matrix")
    fm.add_argument("--out", default="evidence/recovery/fault_matrix.json")
    fm.add_argument("--only", nargs="*",
                    help="report only these case ids, e.g. C01 C08 (all cases still run: they share state)")
    fm.set_defaults(fn=_fault_matrix)

    sub.add_parser("serving-checksum", help="checksum of what the dashboard role sees").set_defaults(
        fn=_serving_checksum)
    bk = sub.add_parser("backup", help="consistent dump + manifest of the warehouse (needs pg_dump)")
    bk.add_argument("--out", default="artifacts/backups")
    bk.set_defaults(fn=_backup)
    rd = sub.add_parser("restore-drill", help="restore a dump into a separate demo database and verify it")
    rd.add_argument("--dump", required=True)
    rd.add_argument("--manifest", required=True)
    rd.add_argument("--server-dsn", required=True)
    rd.set_defaults(fn=_restore_drill)
    bm = sub.add_parser("benchmark", help="measured local experiments in a throwaway demo database")
    bm.add_argument("--server-dsn", required=True)
    bm.add_argument("--store", required=True)
    bm.add_argument("--dataset-sha256", required=True)
    bm.add_argument("--workdir", default="artifacts/benchmark")
    bm.add_argument("--out", default="evidence/benchmark/benchmark.json")
    bm.add_argument("--exact-fixture", action="store_true")
    bm.add_argument("--intervals", type=int, default=None, help="limit the replay to the first N intervals")
    bm.set_defaults(fn=_benchmark)

    gc = sub.add_parser("candidates-gc", help="drop old candidate schemas (dry run unless --apply)")
    gc.add_argument("--keep-published", type=int, default=2, help="previous published releases to keep")
    gc.add_argument("--failed-days", type=int, default=7)
    gc.add_argument("--hold", action="append", help="release id kept for an open incident (repeatable)")
    gc.add_argument("--apply", action="store_true")
    gc.set_defaults(fn=_candidates_gc)

    pa = sub.add_parser("parity", help="replay the golden fixture; compare checksums with the committed ones")
    pa.add_argument("--server-dsn", required=True)
    pa.add_argument("--expected", default="evidence/parity/golden_v1_checksums.json")
    pa.add_argument("--write", action="store_true", help="(re)write the expected file from this machine")
    pa.add_argument("--no-restore", action="store_true")
    pa.set_defaults(fn=_parity)

    gd = sub.add_parser("guard", help="inspect or (operator only) take over the single-writer guard")
    gd.add_argument("action", choices=["status", "takeover"])
    gd.add_argument("--operator")
    gd.add_argument("--reason")
    gd.set_defaults(fn=_guard)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
