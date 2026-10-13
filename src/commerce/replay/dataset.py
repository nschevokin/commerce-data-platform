from __future__ import annotations

import csv
import io
import re
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

from commerce.contracts.canonical import SERIALIZER_VERSION, canonical_json, sha256_hex
from commerce.contracts.manifest import ContractError
from commerce.contracts.sources import CORE_SOURCES, SPEC_BY_CSV, SourceSpec

INVENTORY_VERSION = 1
_CHUNK = 1024 * 1024
_INT_KEY = re.compile(r"^[0-9]{1,9}$")


class DatasetSource:
    kind = "abstract"

    def digest(self) -> str:
        raise NotImplementedError

    def members(self) -> list[str]:
        raise NotImplementedError

    def open_text(self, member: str) -> TextIO:
        raise NotImplementedError

    def read_bytes(self, member: str) -> bytes:
        raise NotImplementedError


class ZipSource(DatasetSource):

    kind = "zip"

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not self.path.is_file():
            raise ContractError(f"archive not found: {self.path}")
        self._names = self._csv_members()

    def _csv_members(self) -> dict[str, str]:
        with zipfile.ZipFile(self.path) as zf:
            out: dict[str, str] = {}
            for info in zf.infolist():
                base = info.filename.rsplit("/", 1)[-1]
                if (info.is_dir() or not base.lower().endswith(".csv") or base.startswith("._")
                        or info.filename.startswith("__MACOSX/")):
                    continue
                if base in out:
                    raise ContractError(f"archive contains {base} more than once")
                out[base] = info.filename
            return out

    def digest(self) -> str:
        import hashlib

        h = hashlib.sha256()
        with open(self.path, "rb") as fh:
            while chunk := fh.read(_CHUNK):
                h.update(chunk)
        return h.hexdigest()

    @property
    def size_bytes(self) -> int:
        return self.path.stat().st_size

    def members(self) -> list[str]:
        return sorted(self._names)

    def open_text(self, member: str) -> TextIO:
        zf = zipfile.ZipFile(self.path)
        raw = zf.open(self._names[member])
        wrapper = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        wrapper._zf = zf
        return wrapper

    def read_bytes(self, member: str) -> bytes:
        with zipfile.ZipFile(self.path) as zf:
            return zf.read(self._names[member])


class DirSource(DatasetSource):

    kind = "directory"

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not self.path.is_dir():
            raise ContractError(f"dataset directory not found: {self.path}")

    def members(self) -> list[str]:
        return sorted(p.name for p in self.path.glob("*.csv"))

    def digest(self) -> str:
        listing = [[n, sha256_hex(self.read_bytes(n))] for n in self.members()]
        return sha256_hex(canonical_json({"kind": "directory-digest-v1", "files": listing}))

    def open_text(self, member: str) -> TextIO:
        return open(self.path / member, encoding="utf-8-sig", newline="")

    def read_bytes(self, member: str) -> bytes:
        return (self.path / member).read_bytes()


def open_source(path: str | Path) -> DatasetSource:
    p = Path(path)
    return DirSource(p) if p.is_dir() else ZipSource(p)


def iter_rows(source: DatasetSource, spec: SourceSpec) -> Iterator[tuple[int, dict[str, str]]]:
    if spec.csv_name not in source.members():
        raise ContractError(f"required file {spec.csv_name} is not in the dataset")
    with source.open_text(spec.csv_name) as fh:
        reader = csv.DictReader(fh, restkey="__extra__")
        header = reader.fieldnames or []
        if len(set(header)) != len(header):
            raise ContractError(f"{spec.csv_name}: duplicate column names in header")
        missing = [c for c in spec.columns if c not in header]
        if missing:
            raise ContractError(f"{spec.csv_name}: expected columns missing: {missing}")
        for row in reader:
            line = reader.line_num
            if "__extra__" in row:
                raise ContractError(f"{spec.csv_name} line {line}: more fields than the header")
            if any(v is None for v in row.values()):
                raise ContractError(f"{spec.csv_name} line {line}: fewer fields than the header")
            yield line, row


def entity_key(spec: SourceSpec, row: dict[str, str], line: int) -> str:
    values: list[str | int] = []
    for col in spec.key_columns:
        raw = row[col].strip()
        if not raw:
            raise ContractError(f"{spec.csv_name} line {line}: empty key column {col}")
        if col in spec.int_key_columns:
            if not _INT_KEY.match(raw) or int(raw) <= 0:
                raise ContractError(f"{spec.csv_name} line {line}: {col}={raw!r} is not a positive integer")
            values.append(int(raw))
        else:
            values.append(raw)
    return canonical_json(values).decode("utf-8")


def payload_of(row: dict[str, str]) -> dict[str, str | None]:
    return {k: (v if v != "" else None) for k, v in row.items()}


def load_core_rows(source: DatasetSource) -> dict[str, list[tuple[str, int, dict[str, str]]]]:
    out: dict[str, list[tuple[str, int, dict[str, str]]]] = {}
    for spec in CORE_SOURCES:
        seen: dict[str, int] = {}
        rows = []
        for line, row in iter_rows(source, spec):
            key = entity_key(spec, row, line)
            if key in seen:
                raise ContractError(
                    f"{spec.csv_name}: duplicate key {key} at lines {seen[key]} and {line}")
            seen[key] = line
            rows.append((key, line, row))
        out[spec.name] = rows
    return out


def _count_rows(source: DatasetSource, member: str) -> tuple[list[str], int]:
    with source.open_text(member) as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        return header, sum(1 for _ in reader)


def inventory(source: DatasetSource, *, retrieved_on: str, dataset_url: str, licence: str) -> dict:
    if not (retrieved_on and dataset_url and licence):
        raise ContractError("retrieved_on, dataset_url and licence are required provenance fields")
    present = set(source.members())
    missing = sorted(s.csv_name for s in CORE_SOURCES if s.csv_name not in present)
    if missing:
        raise ContractError(f"core files missing from the dataset: {missing}")
    files = []
    for member in source.members():
        data = source.read_bytes(member)
        header, rows = _count_rows(source, member)
        spec = SPEC_BY_CSV.get(member)
        files.append({
            "name": member, "size_bytes": len(data), "sha256": sha256_hex(data),
            "row_count": rows, "columns": header,
            "role": f"core:{spec.name}" if spec else "deferred",
        })
    return {
        "inventory_version": INVENTORY_VERSION,
        "serializer_version": SERIALIZER_VERSION,
        "dataset_sha256": source.digest(),
        "dataset_kind": source.kind,
        "archive_size_bytes": getattr(source, "size_bytes", None),
        "retrieved_on": retrieved_on, "dataset_url": dataset_url, "licence": licence,
        "files": files,
    }
