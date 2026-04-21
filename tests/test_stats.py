from __future__ import annotations

from datetime import UTC, datetime

from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import (
    ArtifactHashes,
    PoisonSummary,
    PreflightResult,
    Result,
    RunnerVerdict,
)


def _r(wall: float, valid: bool = True, reason: str | None = None) -> Result:
    return Result(
        batch_id="job-1",
        vm_id="ix",
        instance_type="n1-standard-8",
        region="us-west4",
        artifact_hashes=ArtifactHashes(
            engine="a" * 64,
            bar_content="b" * 64,
            overlay="c" * 64,
            map="d" * 64,
            startscript="e" * 64,
        ),
        preflight=PreflightResult(passed=True),
        run=RunnerVerdict(
            started_at=datetime(2026, 4, 20, tzinfo=UTC),
            ended_at=datetime(2026, 4, 20, tzinfo=UTC),
            engine_exit=0 if valid else 1,
            timings={"engine_wall_s": wall},
        ),
        benchmark={},
        poison=PoisonSummary(tripped=False),
        valid=valid,
        invalid_reason=reason,
    )


def test_summarize_counts_and_percentiles():
    results = [_r(10.0), _r(20.0), _r(30.0), _r(0.0, valid=False, reason="engine_crash")]
    report = aggregate.summarize(results, submitted=5, job_uid="job-1")
    assert report.submitted == 5
    assert report.valid == 3
    # invalid = 1 returned + 1 missing (infra)
    assert report.invalid == 2
    assert report.invalid_reasons == {"engine_crash": 1, "infrastructure_failure": 1}
    assert report.wall_time_mean_s == 20.0
    assert report.wall_time_median_s == 20.0
    assert report.wall_time_p95_s == 29.0  # linear interp on 3 points


def test_summarize_empty():
    report = aggregate.summarize([], submitted=3, job_uid="job-2")
    assert report.valid == 0
    assert report.invalid == 3
    assert report.invalid_reasons == {"infrastructure_failure": 3}
    assert report.wall_time_mean_s is None
    assert report.wall_time_p95_s is None
