from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

from commerce import config
from commerce.replay.reader import ReplayReader
from commerce.replay.store import RawStore

TIMEOUTS_MS = {"commerce_publish": (10_000, 60_000)}


@dataclass(frozen=True)
class Wiring:
    store_root: str
    dataset_sha256: str
    replay_version: str
    artifacts_root: Path
    dsn_for: Callable[[str], str]
    dbt_env: Mapping[str, str] = field(default_factory=dict)
    exact_fixture: bool = False
    faults: Mapping[str, object] = field(default_factory=dict)

    def reader(self) -> ReplayReader:
        return ReplayReader(RawStore(self.store_root), self.dataset_sha256, self.replay_version)

    def store(self) -> RawStore:
        return RawStore(self.store_root)

    def connect(self, role: str) -> psycopg.Connection:
        conn = psycopg.connect(self.dsn_for(role), autocommit=True)
        limits = TIMEOUTS_MS.get(role)
        if limits:
            conn.execute(f"SET lock_timeout = {int(limits[0])}")
            conn.execute(f"SET statement_timeout = {int(limits[1])}")
        return conn

    @classmethod
    def from_env(cls, *, replay_version: str | None = None) -> Wiring:
        repo = Path(__file__).resolve().parents[3]
        return cls(
            store_root=config.require("COMMERCE_STORE"),
            dataset_sha256=config.require("COMMERCE_DATASET_SHA256"),
            replay_version=replay_version or os.environ.get("COMMERCE_REPLAY_VERSION", "baseline-v1"),
            artifacts_root=Path(os.environ.get("COMMERCE_ARTIFACTS", repo / "artifacts" / "builds")),
            dsn_for=config.role_dsn,
            dbt_env={"COMMERCE_TRANSFORM_PASSWORD": config.require("COMMERCE_TRANSFORM_PASSWORD"),
                     **{k: v for k, v in os.environ.items() if k.startswith("COMMERCE_WAREHOUSE_")}},
            exact_fixture=os.environ.get("COMMERCE_EXACT_FIXTURE") == "1")
