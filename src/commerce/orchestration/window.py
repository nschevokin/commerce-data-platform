from __future__ import annotations

from datetime import UTC, datetime

from commerce.orchestration.errors import RunConfigError


def parse_utc(value: str | datetime, name: str) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise RunConfigError(f"{name} must be an ISO-8601 UTC timestamp ending in Z, got {value!r}")
        try:
            dt = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as exc:
            raise RunConfigError(f"{name} is not a valid timestamp: {value!r}") from exc
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise RunConfigError(f"{name} must be UTC")
    return dt.astimezone(UTC)


def run_window(*, run_type: str, data_interval_start=None, data_interval_end=None,
               interval_start=None, interval_end=None) -> tuple[str, datetime, datetime]:
    if run_type == "scheduled":
        if data_interval_start is None or data_interval_end is None:
            raise RunConfigError("scheduled run without a data interval")
        mode, start, end = "scheduled", parse_utc(data_interval_start, "data_interval_start"), \
            parse_utc(data_interval_end, "data_interval_end")
    elif run_type == "manual":
        if not interval_start or not interval_end:
            raise RunConfigError("a manual run needs explicit interval_start and interval_end")
        mode, start, end = "manual", parse_utc(interval_start, "interval_start"), \
            parse_utc(interval_end, "interval_end")
    else:
        raise RunConfigError(f"unsupported run type {run_type!r}: only scheduled and manual runs write")
    if end <= start:
        raise RunConfigError("interval_end must be after interval_start")
    return mode, start, end
