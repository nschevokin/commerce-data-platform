from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from commerce.contracts.canonical import canonical_json, sha256_hex
from commerce.contracts.manifest import ContractError
from commerce.contracts.money import fmt_money, parse_money
from commerce.replay.events import Interval, SourceEvent
from commerce.replay.reader import ReplayReader
from commerce.replay.store import RawStore
from commerce.replay.writer import write_replay

SCENARIO_KINDS = ("duplicate", "late", "correction")
FAULT_VARIANTS = ("malformed_money", "malformed_timestamp", "unknown_status",
                  "conflicting_payload", "orphan_item")
LATE_SHIFT_DAYS = 2
REDELIVERY_LAG_DAYS = 3
CORRECTION_LAG_DAYS = 3
CORRECTION_DELTA = Decimal("5.00")


def select_keys(candidates: list[str], *, seed: int, scenario: str, count: int) -> list[str]:
    ranked = sorted(set(candidates), key=lambda k: sha256_hex(f"{seed}|{scenario}|{k}".encode()))
    return sorted(ranked[:count])


class Bundle:

    def __init__(self, order: SourceEvent, day: str):
        self.order = order
        self.day = day
        self.others: list[SourceEvent] = []

    @property
    def order_id(self) -> str:
        return json.loads(self.order.entity_key)[0]

    @property
    def events(self) -> list[SourceEvent]:
        return [self.order, *self.others]


def _bundles(events_by_day: dict[str, list[SourceEvent]]) -> dict[str, Bundle]:
    bundles: dict[str, Bundle] = {}
    customers: dict[str, tuple[SourceEvent, str]] = {}
    for day, evs in events_by_day.items():
        for e in evs:
            if e.source == "orders":
                bundles[json.loads(e.entity_key)[0]] = Bundle(e, day)
            elif e.source == "customers":
                customers[json.loads(e.entity_key)[0]] = (e, day)
    for evs in events_by_day.values():
        for e in evs:
            if e.source in ("order_items", "order_payments"):
                oid = json.loads(e.entity_key)[0]
                if oid in bundles:
                    bundles[oid].others.append(e)
    for b in bundles.values():
        cid = (b.order.payload["customer_id"] or "").strip()
        if cid in customers:
            cust, cust_day = customers[cid]
            if cust_day == b.day and cust.available_at == b.order.available_at:
                b.others.append(cust)
    return bundles


def _day_index(calendar: list[Interval]) -> dict[str, int]:
    return {iv.day: i for i, iv in enumerate(calendar)}


def _pick(bundles: dict[str, Bundle], eligible: list[str], *, kind: str, seed: int,
          order_ids: list[str] | None, count: int) -> list[str]:
    if order_ids:
        unknown = sorted(set(order_ids) - set(bundles))
        bad = sorted(set(order_ids) - set(eligible)) if not unknown else []
        if unknown:
            raise ContractError(f"{kind}: orders not in the baseline: {unknown}")
        if bad:
            raise ContractError(f"{kind}: orders not eligible for this scenario: {bad}")
        return sorted(order_ids)
    chosen = select_keys(eligible, seed=seed, scenario=kind, count=count)
    if len(chosen) < count:
        raise ContractError(f"{kind}: only {len(chosen)} eligible orders for count={count}")
    return chosen


def _target(calendar: list[Interval], src_day: str, lag: int) -> Interval:
    idx = min(_day_index(calendar)[src_day] + lag, len(calendar) - 1)
    return calendar[idx]


def build_scenario(store: RawStore, *, dataset_sha256: str, base_version: str, kind: str,
                   seed: int = 42, count: int = 1, order_ids: list[str] | None = None,
                   variant: str | None = None, new_version: str | None = None) -> dict:
    reader = ReplayReader(store, dataset_sha256, base_version)
    if reader.descriptor["kind"] != "baseline":
        raise ContractError("scenarios are derived from a baseline replay")
    calendar = reader.calendar()
    events_by_day = {d: list(v) for d, v in reader.all_events().items()}
    bundles = _bundles(events_by_day)
    dindex = _day_index(calendar)
    last = len(calendar) - 1
    day_of = {oid: dindex[b.day] for oid, b in bundles.items()}
    strict = True
    mutations: list[dict] = []

    if kind == "fault":
        if variant not in FAULT_VARIANTS:
            raise ContractError(f"unknown fault variant {variant!r}; choose from {FAULT_VARIANTS}")
        scenario_name, strict = f"fault_{variant}", False
        version = new_version or f"fault-{variant.replace('_', '-')}-v1"
        eligible = sorted(o for o, b in bundles.items()
                          if variant != "malformed_money" or any(e.source == "order_items" for e in b.others))
        chosen = _pick(bundles, eligible, kind=scenario_name, seed=seed, order_ids=order_ids, count=count)
        _apply_fault(variant, chosen, bundles, events_by_day, calendar, mutations)
    elif kind in SCENARIO_KINDS:
        scenario_name = kind
        version = new_version or f"{kind}-v1-seed-{seed}"
        if kind == "duplicate":
            eligible = sorted(o for o in bundles if day_of[o] + 1 <= last)
        elif kind == "late":
            eligible = sorted(o for o in bundles if day_of[o] + LATE_SHIFT_DAYS <= last)
        else:
            eligible = sorted(o for o, b in bundles.items() if day_of[o] + 1 <= last and
                              any(e.source == "order_items" for e in b.others) and
                              any(e.source == "order_payments" for e in b.others))
        chosen = _pick(bundles, eligible, kind=scenario_name, seed=seed, order_ids=order_ids, count=count)
        for oid in chosen:
            b = bundles[oid]
            if kind == "duplicate":
                tgt = _target(calendar, b.day, REDELIVERY_LAG_DAYS)
                events_by_day[tgt.day].extend(b.events)
                mutations.append({"order_id": oid, "action": "redeliver_identical",
                                  "from_day": b.day, "to_day": tgt.day, "events": len(b.events)})
            elif kind == "late":
                tgt = calendar[dindex[b.day] + LATE_SHIFT_DAYS]
                moved = {e.event_key for e in b.events}
                events_by_day[b.day] = [e for e in events_by_day[b.day] if e.event_key not in moved]
                events_by_day[tgt.day].extend(
                    replace(e, available_at=e.available_at + timedelta(days=LATE_SHIFT_DAYS))
                    for e in b.events)
                mutations.append({"order_id": oid, "action": "shift_two_replay_days",
                                  "from_day": b.day, "to_day": tgt.day, "events": len(b.events)})
            else:
                tgt = _target(calendar, b.day, CORRECTION_LAG_DAYS)
                at = tgt.start + timedelta(hours=1)
                item = min((e for e in b.others if e.source == "order_items"),
                           key=lambda e: json.loads(e.entity_key)[1])
                pay = min((e for e in b.others if e.source == "order_payments"),
                          key=lambda e: json.loads(e.entity_key)[1])
                new_item = dict(item.payload)
                new_item["price"] = fmt_money(parse_money(item.payload["price"] or "") + CORRECTION_DELTA)
                new_pay = dict(pay.payload)
                new_pay["payment_value"] = fmt_money(
                    parse_money(pay.payload["payment_value"] or "") + CORRECTION_DELTA)
                events_by_day[tgt.day].append(replace(item, revision=2, available_at=at, payload=new_item))
                events_by_day[tgt.day].append(replace(pay, revision=2, available_at=at, payload=new_pay))
                mutations.append({
                    "order_id": oid, "action": "revision_2_item_price_and_payment", "to_day": tgt.day,
                    "delta_brl": fmt_money(CORRECTION_DELTA),
                    "item": item.entity_key, "payment": pay.entity_key})
    else:
        raise ContractError(f"unknown scenario kind {kind!r}")

    scenario = {
        "scenario": scenario_name, "seed": seed,
        "selection": "explicit" if order_ids else f"seeded count={count} by sha256(seed|scenario|key)",
        "selected_order_ids": chosen, "mutations": mutations,
        "redelivery_rule": "D-005: an event before its interval is accepted only as an exact redelivery",
        "baseline_descriptor_sha256": sha256_hex(canonical_json(reader.descriptor)),
    }
    summary = write_replay(
        store, dataset_sha256=dataset_sha256, dataset_kind=reader.descriptor["dataset_kind"],
        replay_version=version, kind="scenario", base_replay_version=base_version,
        parameters={**reader.descriptor["parameters"], "scenario_kind": kind}, scenario=scenario,
        calendar=calendar, events_by_day=defaultdict(list, events_by_day), strict=strict)
    summary["replay_version"] = version
    summary["selected_order_ids"] = chosen
    return summary


def _replace_event(events_by_day, day: str, old: SourceEvent, new: SourceEvent) -> None:
    lst = events_by_day[day]
    lst[lst.index(old)] = new


def _apply_fault(variant, chosen, bundles, events_by_day, calendar, mutations) -> None:
    for oid in chosen:
        b = bundles[oid]
        if variant == "malformed_money":
            item = min((e for e in b.others if e.source == "order_items"),
                       key=lambda e: json.loads(e.entity_key)[1])
            p = dict(item.payload)
            p["price"] = "12.345"
            _replace_event(events_by_day, b.day, item, replace(item, payload=p))
            mutations.append({"order_id": oid, "source": "order_items", "field": "price", "value": "12.345"})
        elif variant == "malformed_timestamp":
            p = dict(b.order.payload)
            p["order_purchase_timestamp"] = "10/03/2017 14:20"
            _replace_event(events_by_day, b.day, b.order, replace(b.order, payload=p))
            mutations.append({"order_id": oid, "source": "orders",
                              "field": "order_purchase_timestamp", "value": "10/03/2017 14:20"})
        elif variant == "unknown_status":
            p = dict(b.order.payload)
            p["order_status"] = "returned"
            _replace_event(events_by_day, b.day, b.order, replace(b.order, payload=p))
            mutations.append({"order_id": oid, "source": "orders", "field": "order_status",
                              "value": "returned"})
        elif variant == "conflicting_payload":
            p = dict(b.order.payload)
            p["order_status"] = "shipped" if p["order_status"] != "shipped" else "delivered"
            events_by_day[b.day].append(replace(b.order, payload=p))
            mutations.append({"order_id": oid, "source": "orders", "field": "order_status",
                              "value": p["order_status"],
                              "note": "second event with the same key and revision"})
        elif variant == "orphan_item":
            item = min((e for e in b.others if e.source == "order_items"),
                       key=lambda e: json.loads(e.entity_key)[1], default=None)
            if item is None:
                raise ContractError(f"orphan_item needs an order with items: {oid}")
            ghost = f"orphan-of-{oid}"
            p = dict(item.payload)
            p["order_id"] = ghost
            events_by_day[b.day].append(replace(
                item, entity_key=canonical_json([ghost, json.loads(item.entity_key)[1]]).decode(),
                payload=p))
            mutations.append({"order_id": ghost, "source": "order_items",
                              "note": "item whose order is never delivered"})
