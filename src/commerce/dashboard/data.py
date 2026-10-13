from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg

from commerce.serving.reader import read_snapshot

FRESH, STALE, UNAVAILABLE, EMPTY = "FRESH", "STALE", "UNAVAILABLE", "NO_RELEASE"


@dataclass(frozen=True)
class Bundle:
    release: dict[str, Any]
    tables: dict[str, list[dict[str, Any]]]
    fetched_at: datetime
    read_seconds: float

    @property
    def release_id(self) -> str:
        return str(self.release["release_id"])


@dataclass(frozen=True)
class Status:
    state: str
    last_success_at: datetime | None
    error: str | None = None


def fetch_bundle(dsn: str, *, timeout_seconds: int = 10) -> Bundle | None:
    started = time.perf_counter()
    with psycopg.connect(dsn, connect_timeout=timeout_seconds,
                         options="-c statement_timeout=15000") as conn:
        snap = read_snapshot(conn)
    elapsed = time.perf_counter() - started
    if snap.release is None or snap.release.get("release_id") is None:
        return None
    return Bundle(snap.release, snap.tables, datetime.now(UTC), elapsed)


@dataclass
class Refresher:
    dsn: str
    fetch: Any = fetch_bundle
    last: Bundle | None = field(default=None)
    last_result: tuple[Bundle | None, Status] | None = field(default=None)
    last_attempt: float = field(default=-1e9)

    def refresh(self) -> tuple[Bundle | None, Status]:
        try:
            bundle = self.fetch(self.dsn)
        except (psycopg.Error, OSError) as exc:
            msg = f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}"
            if self.last is not None:
                return self.last, Status(STALE, self.last.fetched_at, msg)
            return None, Status(UNAVAILABLE, None, msg)
        if bundle is None:
            return None, Status(EMPTY, None)
        self.last = bundle
        return bundle, Status(FRESH, bundle.fetched_at)

    def refresh_if_due(self, ttl_seconds: float = 30.0, clock=time.monotonic) -> tuple[Bundle | None, Status]:
        now = clock()
        if self.last_result is None or now - self.last_attempt >= ttl_seconds:
            self.last_result = self.refresh()
            self.last_attempt = now
        return self.last_result
