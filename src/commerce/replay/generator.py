from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

from commerce.contracts.manifest import ContractError
from commerce.contracts.sources import ORDER_ITEMS, ORDER_PAYMENTS, REFERENCE_SOURCES
from commerce.replay.dataset import DatasetSource, load_core_rows, payload_of
from commerce.replay.events import SourceEvent, build_calendar
from commerce.replay.store import RawStore
from commerce.replay.writer import write_replay

_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


@dataclass(frozen=True)
class ReplayConfig:
    replay_version: str = "baseline-v1"
    n_days: int = 30
    start_date: date = date(2026, 1, 2)
    allow_orphans: bool = False

    def parameters(self) -> dict:
        return {"n_days": self.n_days, "start_date": self.start_date.isoformat(),
                "partition": "rank(purchase_timestamp, order_id) * n_days // n_orders",
                "arrival_offset": "rank_in_day * 86400 // orders_in_day seconds",
                "allow_orphans": self.allow_orphans, "uses_randomness": False}


def build_baseline(source: DatasetSource, config: ReplayConfig) -> tuple[dict[str, list[SourceEvent]], dict]:
    rows = load_core_rows(source)
    calendar = build_calendar(config.start_date, config.n_days)
    by_day: dict[str, list[SourceEvent]] = {iv.day: [] for iv in calendar}
    report: dict = {"orphans_excluded": {}, "orders_without_customer": 0}

    boot = calendar[0]
    for spec in REFERENCE_SOURCES:
        for key, _, row in rows[spec.name]:
            by_day[boot.day].append(SourceEvent(spec.name, key, 1, boot.start, payload_of(row)))

    orders = []
    for key, line, row in rows["orders"]:
        ts = row["order_purchase_timestamp"].strip()
        if not _TS.match(ts):
            raise ContractError(f"orders line {line}: order_purchase_timestamp {ts!r} is not "
                                "YYYY-MM-DD HH:MM:SS (needed to partition the replay)")
        orders.append((ts, row["order_id"].strip(), key, row))
    orders.sort(key=lambda t: (t[0], t[1]))
    n = len(orders)

    order_ids = {t[1] for t in orders}
    customer_rows = {row["customer_id"].strip(): (k, row) for k, _, row in rows["customers"]}
    children: dict[str, list[tuple[str, str, dict]]] = {o: [] for o in order_ids}
    for spec in (ORDER_ITEMS, ORDER_PAYMENTS):
        orphans = 0
        for key, _, row in rows[spec.name]:
            oid = row["order_id"].strip()
            if oid in order_ids:
                children[oid].append((spec.name, key, row))
            else:
                orphans += 1
        if orphans:
            report["orphans_excluded"][spec.name] = orphans
    referenced = {t[3]["customer_id"].strip() for t in orders}
    unreferenced = sum(1 for c in customer_rows if c not in referenced)
    if unreferenced:
        report["orphans_excluded"]["customers"] = unreferenced
    if report["orphans_excluded"] and not config.allow_orphans:
        raise ContractError(
            f"orphan rows found {report['orphans_excluded']}; refusing to drop them silently "
            "(re-run with --allow-orphans to exclude and record them)")

    day_index = [r * config.n_days // n for r in range(n)] if n else []
    per_day_total: dict[int, int] = {}
    for d in day_index:
        per_day_total[d] = per_day_total.get(d, 0) + 1
    seen_in_day: dict[int, int] = {}
    claimed_customers: set[str] = set()
    for rank, (_, oid, okey, orow) in enumerate(orders):
        d = day_index[rank]
        iv = calendar[1 + d]
        k = seen_in_day.get(d, 0)
        seen_in_day[d] = k + 1
        at = iv.start + timedelta(seconds=k * 86400 // per_day_total[d])
        out = by_day[iv.day]
        out.append(SourceEvent("orders", okey, 1, at, payload_of(orow)))
        cid = orow["customer_id"].strip()
        if cid in customer_rows:
            if cid not in claimed_customers:
                claimed_customers.add(cid)
                ckey, crow = customer_rows[cid]
                out.append(SourceEvent("customers", ckey, 1, at, payload_of(crow)))
        else:
            report["orders_without_customer"] += 1
        for name, ckey, crow in children[oid]:
            out.append(SourceEvent(name, ckey, 1, at, payload_of(crow)))

    report["orders"] = n
    return by_day, report


def generate_baseline(source: DatasetSource, store: RawStore, config: ReplayConfig | None = None) -> dict:
    config = config or ReplayConfig()
    by_day, report = build_baseline(source, config)
    summary = write_replay(
        store, dataset_sha256=source.digest(), dataset_kind=source.kind,
        replay_version=config.replay_version, kind="baseline", base_replay_version=None,
        parameters=config.parameters(), scenario=None,
        calendar=build_calendar(config.start_date, config.n_days), events_by_day=by_day)
    summary["dataset_sha256"] = source.digest()
    summary["report"] = report
    return summary
