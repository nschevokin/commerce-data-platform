from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg

from commerce import dbt_runner
from commerce.contracts.canonical import canonical_json, sha256_hex, utc_iso
from commerce.contracts.manifest import ContractError
from commerce.contracts.payloads import PARSER_CONTRACT_VERSION
from commerce.replay.reader import ReplayReader


@dataclass(frozen=True)
class Frontier:
    replay_version: str
    frontier_end: datetime
    intervals: int
    manifest_sha256s: tuple[str, ...]


class FrontierNotReady(ContractError):
    pass


def resolve_frontier(conn: psycopg.Connection, reader: ReplayReader, frontier_end: datetime) -> Frontier:
    calendar = reader.calendar()
    ends = [iv.end for iv in calendar]
    if frontier_end not in ends:
        raise ContractError(f"{utc_iso(frontier_end)} is not the end of a replay interval")
    wanted = calendar[:ends.index(frontier_end) + 1]
    rows = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT interval_start, manifest_sha256, completeness FROM control.intervals "
        "WHERE replay_version = %s", (reader.replay_version,)).fetchall()}
    hashes: list[str] = []
    for iv in wanted:
        declared = reader.manifest(iv.start).sha256
        got = rows.get(iv.start)
        if got is None or got[1] != "COMPLETE":
            raise FrontierNotReady(f"interval {iv.label} is not COMPLETE; frontier cannot advance past it")
        if got[0] != declared:
            raise FrontierNotReady(f"interval {iv.label} was completed under a different manifest")
        hashes.append(declared)
    return Frontier(reader.replay_version, frontier_end, len(wanted), tuple(hashes))


def input_digest(*, dataset_sha256: str, frontier: Frontier, code_sha: str, dependency_lock_sha: str) -> str:
    return sha256_hex(canonical_json({
        "dataset_sha256": dataset_sha256, "replay_version": frontier.replay_version,
        "manifest_sha256s": list(frontier.manifest_sha256s),
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "code_sha": code_sha, "dependency_lock_sha": dependency_lock_sha}))


def build_candidate(*, transform_dsn: str, transform_env: dict[str, str], reader: ReplayReader,
                    frontier_end: datetime, artifacts_root: Path, exact_fixture: bool,
                    run_id: str | None = None, release_id: str | None = None,
                    candidate_schema_name: str | None = None) -> dict:
    with psycopg.connect(transform_dsn) as conn:
        frontier = resolve_frontier(conn, reader, frontier_end)
    candidate_schema = f"cand_{uuid.uuid4().hex}"
    run_id = run_id or str(uuid.uuid4())
    release_id = release_id or str(uuid.uuid4())
    if candidate_schema_name is not None:
        candidate_schema = candidate_schema_name
    report = dbt_runner.build(
        candidate_schema=candidate_schema, run_id=run_id, exact_fixture=exact_fixture,
        target_path=artifacts_root / candidate_schema, env=transform_env,
        replay_version=reader.replay_version, frontier_end=utc_iso(frontier_end), release_id=release_id)
    return {"candidate_schema": candidate_schema, "run_id": run_id, "release_id": release_id,
            "frontier": frontier, "report": report}


def summarise_candidate(dsn: str, schema: str) -> dict:
    import re

    if not re.match(r"^cand_[0-9a-f]{32}$", schema):
        raise ValueError("not a candidate schema name")
    with psycopg.connect(dsn) as conn:
        def one(sql: str):
            return conn.execute(sql.replace("{s}", schema)).fetchone()

        counts = {t: one(f"SELECT count(*) FROM {{s}}.{t}")[0] for t in (
            "stg_orders", "stg_customers", "stg_order_items", "stg_order_payments", "stg_products",
            "stg_sellers", "dim_customers", "dim_products", "dim_sellers", "fct_orders", "fct_order_items",
            "fct_payments", "mart_sales_daily")}
        r = one("SELECT sum(eligible_orders), sum(excluded_orders), sum(eligible_orders_with_items), "
                "sum(merchandise_value_brl), sum(freight_value_brl), sum(recorded_payment_brl), "
                "round(sum(merchandise_value_brl) / nullif(sum(eligible_orders_with_items), 0), 2), "
                "min(purchase_date), max(purchase_date) FROM {s}.mart_sales_daily")
        d = one("SELECT sum(delivered_orders), sum(delivered_with_both_dates), sum(on_time_orders) "
                "FROM {s}.mart_delivery_cohorts")
    return {"row_counts": counts,
            "eligible": {"eligible_orders": int(r[0]), "excluded_orders": int(r[1]),
                         "eligible_orders_with_items": int(r[2]), "merchandise_value_brl": str(r[3]),
                         "freight_value_brl": str(r[4]), "recorded_payment_brl": str(r[5]),
                         "average_order_value_brl": str(r[6]),
                         "first_purchase_date": r[7].isoformat(), "last_purchase_date": r[8].isoformat()},
            "delivery": {"delivered_orders": int(d[0]), "delivered_with_both_dates": int(d[1]),
                         "on_time_orders": int(d[2])}}


def crosscheck_archive(source, admin_dsn: str, schema: str) -> dict:
    import tempfile

    from commerce.contracts.sources import CORE_SOURCES
    from commerce.fixture.harness import diff, observe_candidate
    from commerce.fixture.reference import compute_from_dir

    with tempfile.TemporaryDirectory() as tmp:
        for spec in CORE_SOURCES:
            (Path(tmp) / spec.csv_name).write_bytes(source.read_bytes(spec.csv_name))
        ref = compute_from_dir(tmp)
    obs = observe_candidate(admin_dsn, schema)
    diffs = diff(ref, {k: obs[k] for k in ref})
    return {"compared": {"orders": len(ref["orders"]), "sales_days": len(ref["sales_daily"]),
                         "categories": len(ref["category"])},
            "difference_count": len(diffs), "differences": diffs[:20]}
