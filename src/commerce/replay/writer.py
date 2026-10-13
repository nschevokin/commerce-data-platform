from __future__ import annotations

from collections import defaultdict

from commerce.contracts.canonical import SERIALIZER_VERSION, canonical_json, sha256_hex, utc_iso
from commerce.contracts.manifest import (
    DESCRIPTOR_VERSION,
    ContractError,
    Manifest,
    ManifestSource,
    check_calendar,
)
from commerce.contracts.sources import REQUIRED_SOURCE_NAMES, SOURCE_SCHEMA_VERSION
from commerce.replay.events import Interval, SourceEvent, encode_events
from commerce.replay.store import RawStore


def raw_uri(dataset_sha: str, replay: str, interval: Interval, source: str, file_sha: str) -> str:
    return (f"raw/dataset={dataset_sha}/replay={replay}/day={interval.day}/source={source}/"
            f"sha256={file_sha}.jsonl.gz")


def manifest_uri(dataset_sha: str, replay: str, interval: Interval) -> str:
    return f"manifests/dataset={dataset_sha}/replay={replay}/day={interval.day}.json"


def descriptor_uri(dataset_sha: str, replay: str) -> str:
    return f"replays/dataset={dataset_sha}/replay={replay}/replay.json"


def check_event_consistency(calendar: list[Interval],
                            events_by_day: dict[str, list[SourceEvent]]) -> None:
    seen: dict[tuple[str, str, int], SourceEvent] = {}
    for iv in calendar:
        for e in events_by_day.get(iv.day, []):
            if e.available_at >= iv.end:
                raise ContractError(f"{e.event_key} available_at {utc_iso(e.available_at)} is after "
                                    f"interval end {utc_iso(iv.end)}")
            prior = seen.get(e.event_key)
            if prior is not None:
                if prior.payload != e.payload or prior.available_at != e.available_at:
                    raise ContractError(f"conflicting events for {e.event_key}")
            elif e.available_at < iv.start:
                raise ContractError(f"{e.event_key} is before its interval and is not a redelivery")
            seen.setdefault(e.event_key, e)


def write_replay(store: RawStore, *, dataset_sha256: str, dataset_kind: str, replay_version: str,
                 kind: str, base_replay_version: str | None, parameters: dict,
                 scenario: dict | None, calendar: list[Interval],
                 events_by_day: dict[str, list[SourceEvent]], strict: bool = True) -> dict:
    check_calendar([(iv.start, iv.end) for iv in calendar[1:]])
    if calendar[0].end != calendar[1].start:
        raise ContractError("bootstrap interval must end where day 1 starts")
    if strict:
        check_event_consistency(calendar, events_by_day)

    summary_rows: dict[str, int] = defaultdict(int)
    manifests: list[Manifest] = []
    for iv in calendar:
        by_source: dict[str, list[SourceEvent]] = defaultdict(list)
        for e in events_by_day.get(iv.day, []):
            if e.source not in REQUIRED_SOURCE_NAMES:
                raise ContractError(f"unknown source {e.source!r}")
            by_source[e.source].append(e)
        entries = []
        for name in REQUIRED_SOURCE_NAMES:
            evs = by_source.get(name, [])
            content, gz = encode_events(evs)
            file_sha = sha256_hex(gz)
            uri = raw_uri(dataset_sha256, replay_version, iv, name, file_sha)
            store.put_bytes(uri, gz)
            store.read_bytes_verified(uri, file_sha)
            entries.append(ManifestSource(name, SOURCE_SCHEMA_VERSION, uri, file_sha,
                                          sha256_hex(content), len(gz), len(evs)))
            summary_rows[name] += len(evs)
        manifests.append(Manifest(dataset_sha256, replay_version, iv.start, iv.end,
                                  tuple(entries)).validate())

    for m, iv in zip(manifests, calendar, strict=True):
        store.put_bytes(manifest_uri(dataset_sha256, replay_version, iv), m.to_bytes())

    descriptor = {
        "descriptor_version": DESCRIPTOR_VERSION, "serializer_version": SERIALIZER_VERSION,
        "dataset_sha256": dataset_sha256, "dataset_kind": dataset_kind,
        "replay_version": replay_version, "kind": kind,
        "base_replay_version": base_replay_version, "strict_event_checks": strict,
        "parameters": parameters, "scenario": scenario,
        "intervals": [
            {"label": iv.label, "interval_start": utc_iso(iv.start), "interval_end": utc_iso(iv.end),
             "manifest_uri": manifest_uri(dataset_sha256, replay_version, iv),
             "manifest_sha256": m.sha256, "row_count": m.total_rows}
            for m, iv in zip(manifests, calendar, strict=True)],
    }
    data = canonical_json(descriptor)
    uri = descriptor_uri(dataset_sha256, replay_version)
    created = store.put_bytes(uri, data)
    return {"descriptor_uri": uri, "descriptor_sha256": sha256_hex(data), "created": created,
            "intervals": len(calendar), "rows_by_source": dict(sorted(summary_rows.items())),
            "total_rows": sum(summary_rows.values())}
