from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ArtifactHashes(_Frozen):
    engine: str
    bar_content: str
    overlay: str
    map: str
    startscript: str


class PreflightResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    passed: bool
    microbench: dict[str, Any] = Field(default_factory=dict)


class RunnerVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    started_at: datetime
    ended_at: datetime
    engine_exit: int
    timings: dict[str, float] = Field(default_factory=dict)
    benchmark_output_path: str | None = None
    error: str | None = None


class PoisonSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tripped: bool
    signals: dict[str, Any] = Field(default_factory=dict)


class Result(BaseModel):
    """Schema of the per-task results.json. Mirrors ARCHITECTURE.md § Data shapes."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str
    vm_id: str
    instance_type: str
    region: str
    artifact_hashes: ArtifactHashes
    preflight: PreflightResult
    run: RunnerVerdict
    benchmark: dict[str, Any] = Field(default_factory=dict)
    poison: PoisonSummary
    valid: bool
    invalid_reason: str | None = None


class BatchConfig(_Frozen):
    """Parsed CLI args for one `bar-bench run` invocation. Control-host only."""

    engine: Path
    bar_content: Path
    overlay: Path
    map: Path
    startscript: Path
    count: int
    project: str
    region: str
    artifacts_bucket: str
    results_bucket: str
    machine_type: str
    max_run_duration_s: int
    wheel: Path | None = None


class BatchReport(BaseModel):
    """What `stats` prints. Emitted by stats.aggregate."""

    model_config = ConfigDict(extra="forbid")

    job_uid: str
    submitted: int
    valid: int
    invalid: int
    invalid_reasons: dict[str, int] = Field(default_factory=dict)
    wall_time_mean_s: float | None = None
    wall_time_median_s: float | None = None
    wall_time_p95_s: float | None = None
