from __future__ import annotations

import contextlib
import hashlib
import os
import uuid
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from commerce.contracts.canonical import sha256_hex
from commerce.contracts.manifest import ContractError

_CHUNK = 1024 * 1024
TEMP_PREFIX = ".tmp-"


class RawStore:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()

    def path(self, uri: str) -> Path:
        p = PurePosixPath(uri)
        if p.is_absolute() or ".." in p.parts or not p.parts:
            raise ContractError(f"unsafe store key: {uri!r}")
        return self.root.joinpath(*p.parts)

    def exists(self, uri: str) -> bool:
        return self.path(uri).is_file()

    def put_bytes(self, uri: str, data: bytes) -> bool:
        dest = self.path(uri)
        if dest.exists():
            return self._same_or_fail(uri, dest, data)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.parent / f"{TEMP_PREFIX}{uuid.uuid4().hex}-{dest.name}"
        try:
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            try:
                os.link(tmp, dest)
            except FileExistsError:
                return self._same_or_fail(uri, dest, data)
            except OSError:
                if dest.exists():
                    return self._same_or_fail(uri, dest, data)
                os.replace(tmp, dest)
            self._fsync_dir(dest.parent)
            return True
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    @staticmethod
    def _same_or_fail(uri: str, dest: Path, data: bytes) -> bool:
        if sha256_hex(dest.read_bytes()) == sha256_hex(data):
            return False
        raise ContractError(f"store key {uri} already exists with different content")

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        with contextlib.suppress(OSError):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def read_verified(self, uri: str, sha256: str) -> BinaryIO:
        path = self.path(uri)
        digest = hashlib.sha256()
        try:
            with open(path, "rb") as fh:
                while chunk := fh.read(_CHUNK):
                    digest.update(chunk)
        except FileNotFoundError as exc:
            raise ContractError(f"store object missing: {uri}") from exc
        if digest.hexdigest() != sha256:
            raise ContractError(
                f"checksum mismatch for {uri}: expected {sha256[:12]}…, found {digest.hexdigest()[:12]}…")
        return open(path, "rb")

    def read_bytes_verified(self, uri: str, sha256: str) -> bytes:
        with self.read_verified(uri, sha256) as fh:
            return fh.read()

    def read_bytes(self, uri: str) -> bytes:
        try:
            return self.path(uri).read_bytes()
        except FileNotFoundError as exc:
            raise ContractError(f"store object missing: {uri}") from exc

    def stale_temp_files(self) -> list[Path]:
        return sorted(p for p in self.root.rglob(f"{TEMP_PREFIX}*") if p.is_file())
