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


def summarize(
    results: Iterable[Result],
    *,
    submitted: int,
    job_uid: str,
    run_description: str | None = None,
) -> BatchReport:
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
                stddev_ms=sim.get("stddev_ms"),
                count=sim.get("count"),
            )
        )
    per_vm.sort(key=lambda p: p.vm_id)

    sim_ms = [p.mean_ms for p in per_vm]
    mean = statistics.fmean(sim_ms) if sim_ms else None
    stddev = _pooled_stddev(per_vm)
    median = statistics.median(sim_ms) if sim_ms else None
    p95 = _p95(sim_ms)

    return BatchReport(
        job_uid=job_uid,
        run_description=run_description,
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
    stddev = sim.get("stddev_ms")
    if isinstance(stddev, (int, float)):
        out["stddev_ms"] = float(stddev)
    count = sim.get("count")
    if isinstance(count, int):
        out["count"] = count
    return out


def _pooled_stddev(per_vm: list[PerVmSim]) -> float | None:
    """Pooled sample stddev of sim frame times across runs.

    Treats all frames from all runs as one big sample, reconstructed from
    each run's (count, mean_ms, stddev_ms). Returns None if any run is
    missing count or stddev_ms, or if the combined frame count is < 2.
    """
    if not per_vm:
        return None
    total_n = 0
    for p in per_vm:
        if p.count is None or p.stddev_ms is None:
            return None
        total_n += p.count
    if total_n < 2:
        return None
    grand_mean = sum(p.count * p.mean_ms for p in per_vm) / total_n  # type: ignore[operator]
    ss = 0.0
    for p in per_vm:
        n = p.count  # type: ignore[assignment]
        sigma = p.stddev_ms  # type: ignore[assignment]
        ss += (n - 1) * sigma * sigma + n * (p.mean_ms - grand_mean) ** 2
    return math.sqrt(ss / (total_n - 1))


def from_bucket(
    results_bucket: str,
    job_uid: str,
    *,
    submitted: int,
    project: str | None = None,
    client=None,
    run_description: str | None = None,
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
    return summarize(
        results,
        submitted=submitted,
        job_uid=job_uid,
        run_description=run_description,
    )


def print_report(report: BatchReport) -> None:
    print(f"\n=== Batch {report.job_uid} ===")
    if report.run_description:
        print(report.run_description)
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
            sd = f"  stddev={p.stddev_ms:.3f}ms" if p.stddev_ms is not None else ""
            n = f"n={p.count}" if p.count is not None else ""
            print(
                f"  {p.vm_id:<{vm_width}}  mean={p.mean_ms:.3f}ms  spread={spread}{sd}"
                + (f"  {n}" if n else "")
            )
    if report.sim_mean_ms_mean is not None:
        stddev = (
            f"{report.sim_mean_ms_stddev:.3f}"
            if report.sim_mean_ms_stddev is not None
            else "n/a"
        )
        total_frames = sum(p.count for p in report.per_vm if p.count is not None)
        frames_hint = f"  frames= {total_frames}" if total_frames else ""
        print(
            f"across VMs: mean= {report.sim_mean_ms_mean:.3f}ms  "
            f"stddev= {stddev}ms  "
            f"median= {report.sim_mean_ms_median:.3f}ms  "
            f"p95= {report.sim_mean_ms_p95:.3f}ms  "
            f"(vms= {len(report.per_vm)}{frames_hint})"
        )
