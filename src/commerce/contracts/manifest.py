from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from commerce.contracts.canonical import (
    SERIALIZER_VERSION,
    canonical_json,
    parse_utc,
    sha256_hex,
    utc_iso,
)
from commerce.contracts.sources import REQUIRED_SOURCE_NAMES, SOURCE_SCHEMA_VERSION

MANIFEST_VERSION = 1
DESCRIPTOR_VERSION = 1
_SHA = re.compile(r"^[0-9a-f]{64}$")
_NAME = re.compile(r"^[A-Za-z0-9._-]{1,80}$")


class ContractError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManifestSource:
    name: str
    schema_version: int
    uri: str
    sha256: str
    content_sha256: str
    size_bytes: int
    row_count: int

    def to_dict(self) -> dict:
        return {
            "name": self.name, "schema_version": self.schema_version, "uri": self.uri,
            "sha256": self.sha256, "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes, "row_count": self.row_count,
        }


@dataclass(frozen=True)
class Manifest:
    dataset_sha256: str
    replay_version: str
    interval_start: datetime
    interval_end: datetime
    sources: tuple[ManifestSource, ...]

    def to_dict(self) -> dict:
        return {
            "manifest_version": MANIFEST_VERSION,
            "serializer_version": SERIALIZER_VERSION,
            "dataset_sha256": self.dataset_sha256,
            "replay_version": self.replay_version,
            "interval_start": utc_iso(self.interval_start),
            "interval_end": utc_iso(self.interval_end),
            "sources": [s.to_dict() for s in self.sources],
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_dict())

    @property
    def sha256(self) -> str:
        return sha256_hex(self.to_bytes())

    @property
    def total_rows(self) -> int:
        return sum(s.row_count for s in self.sources)

    def source(self, name: str) -> ManifestSource:
        for s in self.sources:
            if s.name == name:
                return s
        raise ContractError(f"manifest has no source {name!r}")

    def validate(self) -> Manifest:
        if not _SHA.match(self.dataset_sha256):
            raise ContractError("manifest dataset_sha256 is not a SHA-256 hex digest")
        if not _NAME.match(self.replay_version):
            raise ContractError(f"invalid replay_version {self.replay_version!r}")
        if self.interval_end <= self.interval_start:
            raise ContractError("manifest interval must be non-empty (start < end)")
        names = [s.name for s in self.sources]
        if names != sorted(set(names)):
            raise ContractError("manifest sources must be unique and sorted by name")
        if tuple(names) != REQUIRED_SOURCE_NAMES:
            missing = sorted(set(REQUIRED_SOURCE_NAMES) - set(names))
            extra = sorted(set(names) - set(REQUIRED_SOURCE_NAMES))
            raise ContractError(f"manifest sources incomplete: missing={missing} unexpected={extra}")
        for s in self.sources:
            if s.schema_version != SOURCE_SCHEMA_VERSION:
                raise ContractError(f"{s.name}: unsupported schema_version {s.schema_version}")
            if not (_SHA.match(s.sha256) and _SHA.match(s.content_sha256)):
                raise ContractError(f"{s.name}: malformed checksum")
            if s.row_count < 0 or s.size_bytes < 0:
                raise ContractError(f"{s.name}: negative row_count or size")
            if not s.uri.startswith("raw/") or f"sha256={s.sha256}" not in s.uri:
                raise ContractError(f"{s.name}: uri must be a raw/ key containing sha256={s.sha256}")
        return self

    @staticmethod
    def from_bytes(data: bytes) -> Manifest:
        import json

        try:
            obj = json.loads(data)
            if obj["manifest_version"] != MANIFEST_VERSION:
                raise ContractError(f"unsupported manifest_version {obj['manifest_version']}")
            if obj["serializer_version"] != SERIALIZER_VERSION:
                raise ContractError(f"unsupported serializer_version {obj['serializer_version']}")
            manifest = Manifest(
                dataset_sha256=obj["dataset_sha256"], replay_version=obj["replay_version"],
                interval_start=parse_utc(obj["interval_start"]),
                interval_end=parse_utc(obj["interval_end"]),
                sources=tuple(ManifestSource(**s) for s in obj["sources"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractError(f"malformed manifest: {exc}") from exc
        if canonical_json(obj) != data:
            raise ContractError("manifest bytes are not in canonical form")
        return manifest.validate()


def check_calendar(intervals: list[tuple[datetime, datetime]]) -> None:
    previous_end = None
    for start, end in intervals:
        if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
            raise ContractError("replay intervals must be UTC")
        if previous_end is not None and start != previous_end:
            raise ContractError(f"replay calendar has a gap or overlap at {utc_iso(start)}")
        if end - start != timedelta(days=1):
            raise ContractError(f"replay interval {utc_iso(start)} is not exactly one day")
        previous_end = end
