from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from commerce.contracts.canonical import canonical_json, sha256_hex

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden_v1"
A, B, C = ("fx0000000000000000000000000order_a", "fx0000000000000000000000000order_b",
           "fx0000000000000000000000000order_c")


def tree_hashes(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): sha256_hex(p.read_bytes())
            for p in sorted(root.rglob("*")) if p.is_file() and not p.name.startswith(".tmp-")}


def content_digest(reader) -> str:
    out = {}
    for iv in reader.calendar():
        m = reader.manifest(iv.start)
        out[iv.label] = {s.name: [s.row_count, s.content_sha256] for s in m.sources}
    return sha256_hex(canonical_json(out))


def copy_fixture(dest: Path) -> Path:
    shutil.copytree(GOLDEN, dest, ignore=shutil.ignore_patterns("expected.json"))
    return dest


def make_zip(dest: Path, extra: dict[str, str] | None = None, skip: set[str] | None = None) -> Path:
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(GOLDEN.glob("*.csv")):
            if skip and p.name in skip:
                continue
            info = zipfile.ZipInfo(p.name, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, p.read_bytes())
        for name, text in (extra or {}).items():
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, text.encode("utf-8"))
    return dest
