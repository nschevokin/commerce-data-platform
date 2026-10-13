from __future__ import annotations

import json
import os
import signal
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DBT_PROJECT = REPO / "dbt"
DBT_EXE = Path(os.environ.get("COMMERCE_DBT_EXE", REPO / "envs" / "dbt" / ".venv" / "bin" / "dbt"))

_OK = {"success", "pass"}
_BUILD_RESOURCES = {"model", "seed", "test"}


class BuildFailed(RuntimeError):
    def __init__(self, message: str, report: BuildReport | None = None):
        super().__init__(message)
        self.report = report


@dataclass
class BuildReport:
    target_path: Path
    returncode: int
    executed: int
    expected: int
    warnings: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.failures and self.executed == self.expected


def build(*, candidate_schema: str, source_schema: str | None = None, run_id: str, exact_fixture: bool,
          target_path: Path, env: dict[str, str], project_dir: Path = DBT_PROJECT,
          replay_version: str | None = None, frontier_end: str | None = None,
          release_id: str | None = None) -> BuildReport:
    if (source_schema is None) == (replay_version is None):
        raise ValueError("pass either source_schema (fixture mode) or replay_version+frontier_end (bronze)")
    if replay_version is not None and frontier_end is None:
        raise ValueError("bronze mode needs frontier_end")
    dbt_vars = {
        "candidate_schema": candidate_schema,
        "run_id": run_id,
        "exact_fixture": exact_fixture,
        **({"source_mode": "bronze", "replay_version": replay_version, "frontier_end": frontier_end,
            "release_id": release_id}
           if replay_version is not None else {"source_schema": source_schema}),
    }
    cmd = [
        str(DBT_EXE), "build",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
        "--target-path", str(target_path),
        "--vars", json.dumps(dbt_vars, sort_keys=True),
        "--no-use-colors",
    ]
    target_path.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(cmd, env={**os.environ, **env}, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, start_new_session=True)
    try:
        stdout, stderr = proc.communicate(timeout=900)
    except BaseException:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        raise
    (target_path / "dbt_stdout.log").write_text(stdout + stderr, encoding="utf-8")
    return judge(target_path, proc.returncode)


def judge(target_path: Path, returncode: int) -> BuildReport:
    rr_path, mf_path = target_path / "run_results.json", target_path / "manifest.json"
    if not rr_path.exists() or not mf_path.exists():
        raise BuildFailed(f"dbt produced no artefacts (exit {returncode}); see {target_path}")
    results = json.loads(rr_path.read_text())["results"]
    manifest = json.loads(mf_path.read_text())
    expected_ids = {
        uid for uid, node in manifest["nodes"].items()
        if node["resource_type"] in _BUILD_RESOURCES
    }
    seen = {r["unique_id"]: r for r in results}
    report = BuildReport(target_path, returncode, len(expected_ids & seen.keys()), len(expected_ids))
    for uid in sorted(expected_ids - seen.keys()):
        report.failures.append(f"{uid}: not executed")
    for uid, r in sorted(seen.items()):
        status = r["status"]
        if status in _OK:
            continue
        if status == "warn":
            report.warnings.append(f"{uid}: {r.get('failures')} failing rows (warn)")
        else:
            report.failures.append(f"{uid}: {status} {r.get('message') or ''}".strip())
    return report
