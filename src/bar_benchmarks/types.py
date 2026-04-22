from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ArtifactNames(_Frozen):
    """Catalog-assigned names for the three shared artifacts that
    identify what actually ran. Included in results.json for traceability."""

    engine: str
    bar_content: str
    map: str


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
    artifact_names: ArtifactNames
    preflight: PreflightResult
    run: RunnerVerdict
    benchmark: dict[str, Any] = Field(default_factory=dict)
    poison: PoisonSummary
    valid: bool
    invalid_reason: str | None = None


class BatchConfig(_Frozen):
    """Parsed CLI args for one `bar-bench run` invocation. Control-host only.

    Engine / bar-content / map identities are catalog names resolved against
    scripts/artifacts.toml; the scenario folder supplies overlay + startscript.
    """

    engine_name: str
    bar_content_name: str
    map_name: str
    scenario_dir: Path
    run_description: str | None = None
    catalog_path: Path
    count: int
    project: str
    region: str
    artifacts_bucket: str
    results_bucket: str
    machine_type: str
    min_cpu_platform: str | None = None
    max_run_duration_s: int
    service_account: str | None = None
    wheel: Path | None = None


class PerVmSim(_Frozen):
    """Per-VM sim frame-time stats pulled from `benchmark.streams.sim`.

    `spread_ms` is mean-absolute-deviation as produced by the overlay Lua;
    `stddev_ms` is the per-run sample standard deviation (Bessel-corrected)
    used to pool a batch-level stddev across runs.
    """

    vm_id: str
    mean_ms: float
    spread_ms: float | None = None
    stddev_ms: float | None = None
    count: int | None = None


class BatchReport(BaseModel):
    """What `stats` prints. Emitted by stats.aggregate.

    The headline metric is the per-VM sim frame time
    (`benchmark.streams.sim.mean_ms`). Per-VM rows carry each VM's own
    mean / spread / stddev; the aggregate takes mean / median / p95 of
    the per-VM means, and `sim_mean_ms_stddev` is the pooled stddev of
    sim frame times across all valid runs (reconstructed from each
    run's `count`, `mean_ms`, `stddev_ms`).
    """

    model_config = ConfigDict(extra="forbid")

    job_uid: str
    run_description: str | None = None
    submitted: int
    valid: int
    invalid: int
    invalid_reasons: dict[str, int] = Field(default_factory=dict)
    per_vm: list[PerVmSim] = Field(default_factory=list)
    sim_mean_ms_mean: float | None = None
    sim_mean_ms_stddev: float | None = None
    sim_mean_ms_median: float | None = None
    sim_mean_ms_p95: float | None = None
