from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from commerce.contracts.canonical import canonical_json, parse_utc, utc_iso
from commerce.contracts.manifest import ContractError


@dataclass(frozen=True)
class SourceEvent:
    source: str
    entity_key: str
    revision: int
    available_at: datetime
    payload: dict[str, str | None]
    operation: str = "UPSERT"

    @property
    def event_key(self) -> tuple[str, str, int]:
        return (self.source, self.entity_key, self.revision)

    def to_obj(self) -> dict:
        return {
            "source": self.source, "entity_key": self.entity_key, "revision": self.revision,
            "available_at": utc_iso(self.available_at), "operation": self.operation,
            "payload": self.payload,
        }

    def line(self) -> bytes:
        return canonical_json(self.to_obj()) + b"\n"

    @staticmethod
    def from_line(line: bytes) -> SourceEvent:
        try:
            o = json.loads(line)
            return SourceEvent(o["source"], o["entity_key"], int(o["revision"]),
                               parse_utc(o["available_at"]), o["payload"], o["operation"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractError(f"malformed event line: {exc}") from exc

    def sort_key(self) -> tuple[datetime, str, int]:
        return (self.available_at, self.entity_key, self.revision)


def encode_events(events: list[SourceEvent]) -> tuple[bytes, bytes]:
    content = b"".join(e.line() for e in sorted(events, key=SourceEvent.sort_key))
    return content, gzip.compress(content, compresslevel=6, mtime=0)


def decode_events(gz: bytes) -> list[SourceEvent]:
    content = gzip.decompress(gz)
    return [SourceEvent.from_line(line) for line in content.splitlines() if line]


@dataclass(frozen=True)
class Interval:
    start: datetime
    end: datetime
    label: str

    @property
    def day(self) -> str:
        return self.start.strftime("%Y-%m-%d")


def build_calendar(start_date: date, n_days: int) -> list[Interval]:
    if n_days < 1:
        raise ContractError("a replay needs at least one day")
    first = datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC)
    cal = [Interval(first - timedelta(days=1), first, "bootstrap")]
    for i in range(n_days):
        s = first + timedelta(days=i)
        cal.append(Interval(s, s + timedelta(days=1), f"day-{i + 1:02d}"))
    return cal
