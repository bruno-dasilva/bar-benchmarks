"""Aggregate a batch's results.json objects into a printable BatchReport."""

from __future__ import annotations

import math
import statistics
from collections import Counter
from collections.abc import Iterable

from bar_benchmarks.types import BatchReport, Result


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    rank = 0.95 * (len(s) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return s[lo]
    frac = rank - lo
    return s[lo] + (s[hi] - s[lo]) * frac


def summarize(results: Iterable[Result], *, submitted: int, job_uid: str) -> BatchReport:
    results = list(results)
    valid = [r for r in results if r.valid]
    invalid = [r for r in results if not r.valid]
    reasons: Counter[str] = Counter()
    for r in invalid:
        reasons[r.invalid_reason or "unknown"] += 1
    # Infrastructure failure = submitted but never uploaded.
    missing = submitted - len(results)
    if missing > 0:
        reasons["infrastructure_failure"] += missing

    sim_ms = [ms for r in valid if (ms := _sim_mean_ms(r)) is not None]
    mean = statistics.fmean(sim_ms) if sim_ms else None
    median = statistics.median(sim_ms) if sim_ms else None
    p95 = _p95(sim_ms)

    return BatchReport(
        job_uid=job_uid,
        submitted=submitted,
        valid=len(valid),
        invalid=len(invalid) + missing,
        invalid_reasons=dict(reasons),
        sim_mean_ms_mean=mean,
        sim_mean_ms_median=median,
        sim_mean_ms_p95=p95,
    )


def _sim_mean_ms(result: Result) -> float | None:
    """Pull `benchmark.streams.sim.mean_ms` from a result, or None if absent."""
    streams = result.benchmark.get("streams") if result.benchmark else None
    if not isinstance(streams, dict):
        return None
    sim = streams.get("sim")
    if not isinstance(sim, dict):
        return None
    value = sim.get("mean_ms")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def from_bucket(
    results_bucket: str,
    job_uid: str,
    *,
    submitted: int,
    project: str | None = None,
    client=None,
) -> BatchReport:
    """Pull every results.json under `<results-bucket>/<job_uid>/` and summarize."""
    if client is None:
        from google.cloud import storage

        client = storage.Client(project=project)
    bucket = client.bucket(results_bucket.removeprefix("gs://"))
    prefix = f"{job_uid}/"
    results: list[Result] = []
    for blob in client.list_blobs(bucket, prefix=prefix):
        if not blob.name.endswith("/results.json"):
            continue
        body = blob.download_as_bytes()
        results.append(Result.model_validate_json(body))
    return summarize(results, submitted=submitted, job_uid=job_uid)


def print_report(report: BatchReport) -> None:
    print(f"\n=== Batch {report.job_uid} ===")
    print(f"submitted: {report.submitted}  valid: {report.valid}  invalid: {report.invalid}")
    if report.invalid_reasons:
        print("invalid breakdown:")
        for reason, count in sorted(report.invalid_reasons.items(), key=lambda kv: -kv[1]):
            print(f"  {count:>4}  {reason}")
    if report.sim_mean_ms_mean is not None:
        print(
            f"streams.sim.mean_ms  mean={report.sim_mean_ms_mean:.3f}  "
            f"median={report.sim_mean_ms_median:.3f}  "
            f"p95={report.sim_mean_ms_p95:.3f}"
        )


