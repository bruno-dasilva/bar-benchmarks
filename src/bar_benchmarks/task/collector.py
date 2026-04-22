"""Merge preflight + verdict + poison + overlay benchmark into results.json.

Runs as the final `alwaysRun` runnable; writes to the GCS-FUSE-mounted
results directory under `<task_index>/results.json`. The job_uid scoping
is handled by the Job's `volumes[].remote_path`.
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

INFOLOG_FILENAME = "infolog.txt"

from bar_benchmarks import paths
from bar_benchmarks.types import (
    ArtifactNames,
    PoisonSummary,
    PreflightResult,
    Result,
    RunnerVerdict,
)


def _load_json(p: Path) -> dict[str, Any] | None:
    if not p.is_file():
        return None
    return json.loads(p.read_text())


def _preflight() -> PreflightResult:
    data = _load_json(paths.run_dir() / "preflight.json")
    if data is None:
        return PreflightResult(passed=False, microbench={})
    return PreflightResult.model_validate(data)


def _verdict() -> RunnerVerdict:
    data = _load_json(paths.run_dir() / "verdict.json")
    if data is None:
        now = datetime.now(UTC)
        return RunnerVerdict(
            started_at=now,
            ended_at=now,
            engine_exit=-1,
            timings={},
            benchmark_output_path=None,
            error="runner_did_not_run",
        )
    return RunnerVerdict.model_validate(data)


def _poison() -> PoisonSummary:
    data = _load_json(paths.run_dir() / "poison.json")
    if data is None:
        return PoisonSummary(tripped=False, signals={})
    return PoisonSummary.model_validate(data)


def _benchmark() -> dict[str, Any]:
    data = _load_json(paths.benchmark_output_path())
    return data if data is not None else {}


def _compute_verdict(
    preflight: PreflightResult, verdict: RunnerVerdict, poison: PoisonSummary
) -> tuple[bool, str | None]:
    if poison.tripped:
        return False, "poisoned"
    if not preflight.passed:
        return False, "preflight_failed"
    if verdict.error:
        return False, verdict.error
    return True, None


def run() -> Result:
    artifacts = paths.artifacts_dir()
    manifest = json.loads((artifacts / "manifest.json").read_text())

    preflight = _preflight()
    verdict = _verdict()
    poison = _poison()
    benchmark = _benchmark()
    valid, reason = _compute_verdict(preflight, verdict, poison)

    result = Result(
        batch_id=manifest["job_uid"],
        vm_id=paths.batch_task_index(),
        instance_type=manifest["instance_type"],
        region=manifest["region"],
        artifact_names=ArtifactNames(**manifest["artifact_names"]),
        preflight=preflight,
        run=verdict,
        benchmark=benchmark,
        poison=poison,
        valid=valid,
        invalid_reason=reason,
    )

    out_dir = paths.results_dir() / paths.batch_task_index()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "results.json"
    out.write_text(json.dumps(result.model_dump(mode="json"), indent=2))

    infolog = paths.data_dir() / INFOLOG_FILENAME
    if infolog.is_file():
        shutil.copy2(infolog, out_dir / INFOLOG_FILENAME)

    return result


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
