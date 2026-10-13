from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from datetime import datetime

from commerce.contracts.canonical import parse_utc, sha256_hex, utc_iso
from commerce.contracts.manifest import ContractError, Manifest
from commerce.replay.events import Interval, SourceEvent
from commerce.replay.store import RawStore
from commerce.replay.writer import descriptor_uri


class IntervalIncomplete(ContractError):
    pass


class ReplayReader:
    def __init__(self, store: RawStore, dataset_sha256: str, replay_version: str,
                 expected_descriptor_sha256: str | None = None):
        self.store = store
        self.dataset_sha256 = dataset_sha256
        self.replay_version = replay_version
        raw = store.read_bytes(descriptor_uri(dataset_sha256, replay_version))
        if expected_descriptor_sha256 and sha256_hex(raw) != expected_descriptor_sha256:
            raise ContractError("replay descriptor does not match the expected checksum")
        self.descriptor = json.loads(raw)
        if (self.descriptor["dataset_sha256"], self.descriptor["replay_version"]) != (
                dataset_sha256, replay_version):
            raise ContractError("replay descriptor identity does not match its location")
        self._by_start = {i["interval_start"]: i for i in self.descriptor["intervals"]}

    def calendar(self) -> list[Interval]:
        return [Interval(parse_utc(i["interval_start"]), parse_utc(i["interval_end"]), i["label"])
                for i in self.descriptor["intervals"]]

    def manifest(self, interval_start: datetime) -> Manifest:
        entry = self._by_start.get(utc_iso(interval_start))
        if entry is None:
            raise IntervalIncomplete(
                f"no manifest for interval starting {utc_iso(interval_start)} in {self.replay_version}")
        data = self.store.read_bytes_verified(entry["manifest_uri"], entry["manifest_sha256"])
        m = Manifest.from_bytes(data)
        if (m.dataset_sha256, m.replay_version, m.interval_start) != (
                self.dataset_sha256, self.replay_version, interval_start):
            raise ContractError("manifest identity does not match its descriptor entry")
        return m

    def events(self, manifest: Manifest, source: str) -> Iterator[SourceEvent]:
        entry = manifest.source(source)
        with self.store.read_verified(entry.uri, entry.sha256) as fh:
            content = gzip.decompress(fh.read())
        if sha256_hex(content) != entry.content_sha256:
            raise ContractError(f"{entry.uri}: decompressed content does not match content_sha256")
        count = 0
        for line in content.splitlines():
            if line:
                count += 1
                yield SourceEvent.from_line(line)
        if count != entry.row_count:
            raise ContractError(f"{entry.uri}: manifest says {entry.row_count} rows, file has {count}")

    def all_events(self) -> dict[str, list[SourceEvent]]:
        out: dict[str, list[SourceEvent]] = {}
        for iv in self.calendar():
            m = self.manifest(iv.start)
            out[iv.day] = [e for s in m.sources for e in self.events(m, s.name)]
        return out

    def verify(self) -> dict:
        totals: dict[str, int] = {}
        for iv in self.calendar():
            m = self.manifest(iv.start)
            for s in m.sources:
                for e in self.events(m, s.name):
                    if e.source != s.name:
                        raise ContractError(f"{s.uri}: event of source {e.source} in a {s.name} file")
                    if e.available_at >= iv.end:
                        raise ContractError(f"{s.uri}: event after the end of its interval")
                totals[s.name] = totals.get(s.name, 0) + s.row_count
        return {"replay_version": self.replay_version, "intervals": len(self._by_start),
                "rows_by_source": dict(sorted(totals.items())), "total_rows": sum(totals.values())}
