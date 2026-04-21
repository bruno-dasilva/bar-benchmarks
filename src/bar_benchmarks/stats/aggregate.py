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

    wall_times = [
        r.run.timings["engine_wall_s"]
        for r in valid
        if "engine_wall_s" in r.run.timings
    ]
    mean = statistics.fmean(wall_times) if wall_times else None
    median = statistics.median(wall_times) if wall_times else None
    p95 = _p95(wall_times)

    return BatchReport(
        job_uid=job_uid,
        submitted=submitted,
        valid=len(valid),
        invalid=len(invalid) + missing,
        invalid_reasons=dict(reasons),
        wall_time_mean_s=mean,
        wall_time_median_s=median,
        wall_time_p95_s=p95,
    )


def from_bucket(results_bucket: str, job_uid: str, *, submitted: int, client=None) -> BatchReport:
    """Pull every results.json under `<results-bucket>/<job_uid>/` and summarize."""
    if client is None:
        from google.cloud import storage

        client = storage.Client()
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
    if report.wall_time_mean_s is not None:
        print(
            f"engine_wall_s  mean={report.wall_time_mean_s:.3f}  "
            f"median={report.wall_time_median_s:.3f}  "
            f"p95={report.wall_time_p95_s:.3f}"
        )


