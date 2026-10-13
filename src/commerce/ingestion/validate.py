from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from commerce.contracts.payloads import PayloadError, canonicalise, entity_key_of, payload_hash
from commerce.contracts.sources import SourceSpec
from commerce.replay.events import SourceEvent


@dataclass(frozen=True)
class RowIssue:
    row: int
    entity_key: str | None
    reason: str

    def to_obj(self) -> dict:
        return {"row": self.row, "entity_key": self.entity_key, "reason": self.reason}


@dataclass(frozen=True)
class CleanEvent:
    entity_key: str
    revision: int
    available_at: datetime
    payload: dict
    payload_sha256: str


@dataclass
class ValidatedFile:
    input_rows: int
    events: list[CleanEvent]
    within_file_duplicates: int
    issues: list[RowIssue]


MAX_ISSUES = 200


def validate_file(spec: SourceSpec, events: list[SourceEvent], *, interval_end: datetime) -> ValidatedFile:
    issues: list[RowIssue] = []
    clean: list[CleanEvent] = []
    seen: dict[tuple[str, int], CleanEvent] = {}
    dups = 0

    def bad(row: int, key: str | None, reason: str) -> None:
        issues.append(RowIssue(row, key, reason))

    for row, e in enumerate(events, start=1):
        if e.source != spec.name:
            bad(row, e.entity_key, f"event of source {e.source!r} in a {spec.name} file")
            continue
        if e.operation != "UPSERT":
            bad(row, e.entity_key, f"operation {e.operation!r} is not supported (no deletes in core)")
            continue
        if e.revision < 1:
            bad(row, e.entity_key, f"revision must be positive, got {e.revision}")
            continue
        if e.available_at >= interval_end:
            bad(row, e.entity_key, "available_at is at or after the end of the manifest interval")
            continue
        try:
            canonical = canonicalise(spec, e.payload)
        except PayloadError as exc:
            bad(row, e.entity_key, str(exc))
            continue
        derived = entity_key_of(spec, canonical)
        if derived != e.entity_key:
            bad(row, e.entity_key, f"entity_key does not match the payload key {derived}")
            continue
        ce = CleanEvent(e.entity_key, e.revision, e.available_at, canonical, payload_hash(canonical))
        prior = seen.get((ce.entity_key, ce.revision))
        if prior is None:
            seen[(ce.entity_key, ce.revision)] = ce
            clean.append(ce)
        elif prior.payload_sha256 == ce.payload_sha256 and prior.available_at == ce.available_at:
            dups += 1
        else:
            bad(row, e.entity_key,
                f"revision {ce.revision} appears twice in the file with different content "
                f"(hash {prior.payload_sha256[:12]} vs {ce.payload_sha256[:12]})")
    return ValidatedFile(len(events), clean, dups, issues)
