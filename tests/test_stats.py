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


def _r(
    sim_mean_ms: float | None,
    valid: bool = True,
    reason: str | None = None,
    vm_id: str = "ix",
    spread_ms: float | None = None,
    count: int | None = None,
) -> Result:
    benchmark: dict = {}
    if sim_mean_ms is not None:
        sim: dict = {"mean_ms": sim_mean_ms}
        if spread_ms is not None:
            sim["spread_ms"] = spread_ms
        if count is not None:
            sim["count"] = count
        benchmark = {"streams": {"sim": sim}}
    return Result(
        batch_id="job-1",
        vm_id=vm_id,
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
    results = [
        _r(10.0, vm_id="0", spread_ms=1.0, count=100),
        _r(20.0, vm_id="1", spread_ms=2.0, count=100),
        _r(30.0, vm_id="2", spread_ms=3.0, count=100),
        _r(None, valid=False, reason="engine_crash"),
    ]
    report = aggregate.summarize(results, submitted=5, job_uid="job-1")
    assert report.submitted == 5
    assert report.valid == 3
    # invalid = 1 returned + 1 missing (infra)
    assert report.invalid == 2
    assert report.invalid_reasons == {"engine_crash": 1, "infrastructure_failure": 1}
    assert report.sim_mean_ms_mean == 20.0
    assert report.sim_mean_ms_stddev == 10.0  # stdev of [10, 20, 30]
    assert report.sim_mean_ms_median == 20.0
    assert report.sim_mean_ms_p95 == 29.0  # linear interp on 3 points
    assert [p.vm_id for p in report.per_vm] == ["0", "1", "2"]
    assert [p.mean_ms for p in report.per_vm] == [10.0, 20.0, 30.0]
    assert [p.spread_ms for p in report.per_vm] == [1.0, 2.0, 3.0]
    assert [p.count for p in report.per_vm] == [100, 100, 100]


def test_summarize_empty():
    report = aggregate.summarize([], submitted=3, job_uid="job-2")
    assert report.valid == 0
    assert report.invalid == 3
    assert report.invalid_reasons == {"infrastructure_failure": 3}
    assert report.per_vm == []
    assert report.sim_mean_ms_mean is None
    assert report.sim_mean_ms_stddev is None
    assert report.sim_mean_ms_p95 is None


def test_summarize_single_vm_has_no_stddev():
    report = aggregate.summarize([_r(20.0, vm_id="0")], submitted=1, job_uid="job-4")
    assert report.sim_mean_ms_mean == 20.0
    assert report.sim_mean_ms_stddev is None
    assert len(report.per_vm) == 1


def test_summarize_skips_results_missing_sim_metric():
    # Valid result with no benchmark payload => not counted toward sim stats,
    # but still valid in the aggregate (run succeeded overall).
    results = [_r(None, vm_id="0"), _r(15.0, vm_id="1"), _r(25.0, vm_id="2")]
    report = aggregate.summarize(results, submitted=3, job_uid="job-3")
    assert report.valid == 3
    assert report.sim_mean_ms_mean == 20.0
    assert report.sim_mean_ms_median == 20.0
    assert [p.vm_id for p in report.per_vm] == ["1", "2"]
