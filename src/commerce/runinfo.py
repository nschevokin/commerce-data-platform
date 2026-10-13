from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def code_sha(repo: Path = REPO) -> str:
    override = os.environ.get("COMMERCE_CODE_SHA")
    if override:
        return override
    try:
        def git(*a: str) -> bytes:
            return subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True).stdout
        head = git("rev-parse", "HEAD").decode().strip()
        dirty = git("status", "--porcelain").strip()
        if not dirty:
            return head
        diff = git("diff", "HEAD") + dirty
        return f"{head}-dirty-{hashlib.sha256(diff).hexdigest()[:12]}"
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def dependency_lock_sha(repo: Path = REPO) -> str:
    h = hashlib.sha256()
    for name in ("uv.lock", "envs/dbt/uv.lock", "docker/airflow/requirements.lock"):
        p = repo / name
        h.update(name.encode() + b"\0" + (p.read_bytes() if p.exists() else b"missing") + b"\0")
    return h.hexdigest()
