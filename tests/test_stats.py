from __future__ import annotations

from datetime import UTC, datetime

from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import (
    ArtifactNames,
    PoisonSummary,
    PreflightResult,
    Result,
    RunnerVerdict,
)


def _r(sim_mean_ms: float | None, valid: bool = True, reason: str | None = None) -> Result:
    benchmark: dict = {}
    if sim_mean_ms is not None:
        benchmark = {"streams": {"sim": {"mean_ms": sim_mean_ms}}}
    return Result(
        batch_id="job-1",
        vm_id="ix",
        instance_type="n1-standard-8",
        region="us-west4",
        artifact_names=ArtifactNames(
            engine="recoil-abc1234",
            bar_content="bar-test-29871-90f4bc1",
            map="hellas-basin-v1.4",
        ),
        preflight=PreflightResult(passed=True),
        run=RunnerVerdict(
            started_at=datetime(2026, 4, 20, tzinfo=UTC),
            ended_at=datetime(2026, 4, 20, tzinfo=UTC),
            engine_exit=0 if valid else 1,
            timings={},
        ),
        benchmark=benchmark,
        poison=PoisonSummary(tripped=False),
        valid=valid,
        invalid_reason=reason,
    )


def test_summarize_counts_and_percentiles():
    results = [_r(10.0), _r(20.0), _r(30.0), _r(None, valid=False, reason="engine_crash")]
    report = aggregate.summarize(results, submitted=5, job_uid="job-1")
    assert report.submitted == 5
    assert report.valid == 3
    # invalid = 1 returned + 1 missing (infra)
    assert report.invalid == 2
    assert report.invalid_reasons == {"engine_crash": 1, "infrastructure_failure": 1}
    assert report.sim_mean_ms_mean == 20.0
    assert report.sim_mean_ms_median == 20.0
    assert report.sim_mean_ms_p95 == 29.0  # linear interp on 3 points


def test_summarize_empty():
    report = aggregate.summarize([], submitted=3, job_uid="job-2")
    assert report.valid == 0
    assert report.invalid == 3
    assert report.invalid_reasons == {"infrastructure_failure": 3}
    assert report.sim_mean_ms_mean is None
    assert report.sim_mean_ms_p95 is None


def test_summarize_skips_results_missing_sim_metric():
    # Valid result with no benchmark payload => not counted toward sim stats,
    # but still valid in the aggregate (run succeeded overall).
    results = [_r(None), _r(15.0), _r(25.0)]
    report = aggregate.summarize(results, submitted=3, job_uid="job-3")
    assert report.valid == 3
    assert report.sim_mean_ms_mean == 20.0
    assert report.sim_mean_ms_median == 20.0
