"""Merge runner verdict + overlay benchmark into results.json.

Runs as the final `alwaysRun` runnable; writes to the GCS-FUSE-mounted
results directory under `<task_index>/<iter>/results.json`. The job_uid
scoping is handled by the Job's `volumes[].remote_path`. When the runner
looped N iterations on this VM, N results are emitted, keyed by iter.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bar_benchmarks import paths
from bar_benchmarks.types import (
    ArtifactNames,
    Result,
    RunnerVerdict,
)

INFOLOG_FILENAME = "infolog.txt"
_ITER_DIR_RE = re.compile(r"^iter-(\d+)$")


def _load_json(p: Path) -> dict[str, Any] | None:
    if not p.is_file():
        return None
    return json.loads(p.read_text())


def _placeholder_verdict() -> RunnerVerdict:
    now = datetime.now(UTC)
    return RunnerVerdict(
        started_at=now,
        ended_at=now,
        engine_exit=-1,
        error="runner_did_not_run",
    )


def _verdict_from(iter_dir: Path) -> RunnerVerdict:
    data = _load_json(iter_dir / "verdict.json")
    if data is None:
        return _placeholder_verdict()
    return RunnerVerdict.model_validate(data)


def _benchmark_from(iter_dir: Path) -> dict[str, Any]:
    data = _load_json(iter_dir / "benchmark.json")
    return data if data is not None else {}


def _discover_iters(run_dir: Path) -> dict[int, Path]:
    """Map iter index → iter dir under run_dir."""
    if not run_dir.is_dir():
        return {}
    out: dict[int, Path] = {}
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        m = _ITER_DIR_RE.match(child.name)
        if m is None:
            continue
        out[int(m.group(1))] = child
    return out


def run() -> list[Result]:
    artifacts = paths.artifacts_dir()
    manifest = json.loads((artifacts / "manifest.json").read_text())
    iterations = int(manifest.get("iterations", 1))
    task_index = paths.batch_task_index()
    task_root = paths.results_dir() / task_index
    task_root.mkdir(parents=True, exist_ok=True)

    iters = _discover_iters(paths.run_dir())
    # Make sure every slot the orchestrator expected gets a blob, even
    # if the runner crashed before producing it.
    expected = set(range(iterations)) | set(iters.keys())

    results: list[Result] = []
    for i in sorted(expected):
        iter_dir = iters.get(i)
        if iter_dir is not None:
            verdict = _verdict_from(iter_dir)
            benchmark = _benchmark_from(iter_dir)
        else:
            verdict = _placeholder_verdict()
            benchmark = {}

        result = Result(
            batch_id=manifest["job_uid"],
            vm_id=f"{task_index}-{i}",
            instance_type=manifest["instance_type"],
            region=manifest["region"],
            artifact_names=ArtifactNames(**manifest["artifact_names"]),
            run=verdict,
            benchmark=benchmark,
            invalid_reason=verdict.error,
        )

        out_dir = task_root / str(i)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "results.json").write_text(
            json.dumps(result.model_dump(mode="json"), indent=2)
        )

        if iter_dir is not None:
            infolog = iter_dir / INFOLOG_FILENAME
            if infolog.is_file():
                shutil.copy2(infolog, out_dir / INFOLOG_FILENAME)

        results.append(result)
    return results


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
