from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

SERIALIZER_VERSION = "canon-v1"
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _check(obj: object) -> None:
    if obj is None or isinstance(obj, (str, int, bool)):
        return
    if isinstance(obj, float):
        raise TypeError("floats are not allowed in canonical JSON (use strings or integers)")
    if isinstance(obj, dict):
        for key, value in obj.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical JSON keys must be strings, got {type(key).__name__}")
            _check(value)
        return
    if isinstance(obj, (list, tuple)):
        for value in obj:
            _check(value)
        return
    raise TypeError(f"unsupported type in canonical JSON: {type(obj).__name__}")


def canonical_json(obj: object) -> bytes:
    _check(obj)
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("naive datetime: replay clocks are always UTC-aware")
    return value.astimezone(UTC).strftime(_TS_FORMAT)


def parse_utc(text: str) -> datetime:
    return datetime.strptime(text, _TS_FORMAT).replace(tzinfo=UTC)
