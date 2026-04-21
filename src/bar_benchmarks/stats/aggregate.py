"""Aggregate a batch's results.json objects into a printable BatchReport."""

from __future__ import annotations

import math
import statistics
from collections import Counter
from collections.abc import Iterable

from bar_benchmarks.types import BatchReport, PerVmSim, Result


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

    per_vm: list[PerVmSim] = []
    for r in valid:
        sim = _sim_stats(r)
        if sim is None:
            continue
        per_vm.append(
            PerVmSim(
                vm_id=r.vm_id,
                mean_ms=sim["mean_ms"],
                spread_ms=sim.get("spread_ms"),
                count=sim.get("count"),
            )
        )
    per_vm.sort(key=lambda p: p.vm_id)

    sim_ms = [p.mean_ms for p in per_vm]
    mean = statistics.fmean(sim_ms) if sim_ms else None
    stddev = statistics.stdev(sim_ms) if len(sim_ms) >= 2 else None
    median = statistics.median(sim_ms) if sim_ms else None
    p95 = _p95(sim_ms)

    return BatchReport(
        job_uid=job_uid,
        submitted=submitted,
        valid=len(valid),
        invalid=len(invalid) + missing,
        invalid_reasons=dict(reasons),
        per_vm=per_vm,
        sim_mean_ms_mean=mean,
        sim_mean_ms_stddev=stddev,
        sim_mean_ms_median=median,
        sim_mean_ms_p95=p95,
    )


def _sim_stats(result: Result) -> dict[str, float | int] | None:
    """Pull `benchmark.streams.sim` fields from a result, or None if absent."""
    streams = result.benchmark.get("streams") if result.benchmark else None
    if not isinstance(streams, dict):
        return None
    sim = streams.get("sim")
    if not isinstance(sim, dict):
        return None
    mean = sim.get("mean_ms")
    if not isinstance(mean, (int, float)):
        return None
    out: dict[str, float | int] = {"mean_ms": float(mean)}
    spread = sim.get("spread_ms")
    if isinstance(spread, (int, float)):
        out["spread_ms"] = float(spread)
    count = sim.get("count")
    if isinstance(count, int):
        out["count"] = count
    return out


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
    if report.per_vm:
        print("per-VM sim.mean_ms:")
        vm_width = max(len(p.vm_id) for p in report.per_vm)
        for p in report.per_vm:
            spread = f"{p.spread_ms:.3f}ms" if p.spread_ms is not None else "?"
            n = f"n={p.count}" if p.count is not None else ""
            print(
                f"  {p.vm_id:<{vm_width}}  mean={p.mean_ms:.3f}ms  spread={spread}"
                + (f"  {n}" if n else "")
            )
    if report.sim_mean_ms_mean is not None:
        stddev = (
            f"{report.sim_mean_ms_stddev:.3f}"
            if report.sim_mean_ms_stddev is not None
            else "n/a"
        )
        print(
            f"across VMs: mean={report.sim_mean_ms_mean:.3f}ms  "
            f"stddev={stddev}ms  "
            f"median={report.sim_mean_ms_median:.3f}ms  "
            f"p95={report.sim_mean_ms_p95:.3f}ms  "
            f"(n={len(report.per_vm)})"
        )
