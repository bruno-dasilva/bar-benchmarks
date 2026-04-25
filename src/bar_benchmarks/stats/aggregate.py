"""Aggregate a batch's results.json objects into a printable BatchReport."""

from __future__ import annotations

import json
import math
import re
import statistics
import sys
from collections import Counter
from collections.abc import Iterable

from bar_benchmarks.types import BatchReport, PerVmSim, Result

_JOB_UID_RE = re.compile(r"^bar-bench-(\d+)-[0-9a-f]+$")


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

    wall_samples = [r.run.engine_wall_s for r in valid if r.run.engine_wall_s is not None]
    wall_mean = statistics.fmean(wall_samples) if wall_samples else None

    instance_type = results[0].instance_type if results else None
    region = results[0].region if results else None

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
        engine_wall_s_mean=wall_mean,
        instance_type=instance_type,
        region=region,
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
    results = list(list_job_results(client, bucket, job_uid))
    return summarize(
        results,
        submitted=submitted,
        job_uid=job_uid,
        run_description=run_description,
    )


def _list_job_uid_prefixes(client, bucket) -> list[str]:
    """Return top-level `bar-bench-*` directories under the bucket."""
    iterator = client.list_blobs(bucket, prefix="", delimiter="/")
    # Force pagination so iterator.prefixes populates.
    for _ in iterator.pages:
        pass
    out: list[str] = []
    for pref in iterator.prefixes:
        name = pref.rstrip("/")
        if _JOB_UID_RE.match(name):
            out.append(name)
    return out


def list_job_results(client, bucket, job_uid: str) -> Iterable[Result]:
    """Yield parsed Result objects for every results.json under `<job_uid>/`."""
    prefix = f"{job_uid}/"
    for blob in client.list_blobs(bucket, prefix=prefix):
        if not blob.name.endswith("/results.json"):
            continue
        body = blob.download_as_bytes()
        yield Result.model_validate_json(body)


def from_window(
    *,
    results_bucket: str,
    engine: str,
    bar_content: str,
    map_: str,
    scenario: str,
    machine_type: str,
    scan_limit: int = 100,
    project: str | None = None,
    client=None,
    extra_results: Iterable[Result] = (),
    extra_submitted: int = 0,
    exclude_job_uids: Iterable[str] = (),
    run_description: str | None = None,
) -> tuple[BatchReport, list[str]]:
    """Pool results across recent jobs whose run.json shape matches.

    Scans up to `scan_limit` most-recent `bar-bench-*` job_uids; for each
    job whose run.json has the same engine/bar_content/map/scenario/
    machine_type, every results.json under that prefix is added to the
    pool. `count` and `iterations` are NOT part of the match — small
    runs and large runs feed the same window.

    `extra_results` / `extra_submitted` let a caller (e.g. `bar-bench
    run` after a fresh batch) fold in just-produced results without
    re-listing their blobs. `exclude_job_uids` skips matching run.jsons —
    used by `bar-bench run` to avoid double-counting the job whose
    run.json was just uploaded but whose results.json files are also
    being passed via `extra_results`.

    Returns the synthesized BatchReport plus the list of contributing
    historical job_uids (most-recent first). The synthesized
    `job_uid` field is the most recent contributing job_uid (or the
    provided `extra_results` job's batch_id if the historical pool is
    empty), so downstream consumers like the Action's
    `results-gcs-uri` output still point at a real bucket prefix.
    """
    if client is None:
        from google.cloud import storage

        client = storage.Client(project=project)
    bucket = client.bucket(results_bucket.removeprefix("gs://"))

    job_uids = _list_job_uid_prefixes(client, bucket)

    def ts(u: str) -> int:
        m = _JOB_UID_RE.match(u)
        return int(m.group(1)) if m else 0

    recent = sorted(job_uids, key=ts, reverse=True)[:scan_limit]
    excluded = set(exclude_job_uids)

    pool: list[Result] = []
    submitted_total = 0
    contributing: list[str] = []

    for job_uid in recent:
        if job_uid in excluded:
            continue
        try:
            body = bucket.blob(f"{job_uid}/run.json").download_as_bytes()
        except Exception:
            continue
        try:
            meta = json.loads(body)
        except json.JSONDecodeError:
            continue
        if not (
            meta.get("engine") == engine
            and meta.get("bar_content") == bar_content
            and meta.get("map") == map_
            and meta.get("scenario") == scenario
            and meta.get("machine_type") == machine_type
        ):
            continue
        try:
            job_results = list(list_job_results(client, bucket, job_uid))
        except Exception as exc:  # noqa: BLE001 — log and skip a flaky job
            print(
                f"[window] skipped {job_uid}: results listing failed "
                f"({type(exc).__name__}: {exc})",
                file=sys.stderr,
            )
            continue
        n_valid = sum(1 for r in job_results if r.valid)
        job_submitted = int(meta.get("count", 0)) * int(meta.get("iterations", 1) or 1)
        # Fall back to actual results count when run.json's count is missing.
        if job_submitted == 0:
            job_submitted = len(job_results)
        pool.extend(job_results)
        submitted_total += job_submitted
        contributing.append(job_uid)
        print(
            f"[window] {job_uid}: matched (valid={n_valid}/{len(job_results)}, "
            f"submitted={job_submitted})",
            file=sys.stderr,
        )

    extras = list(extra_results)
    pool.extend(extras)
    submitted_total += extra_submitted

    if extras and extras[0].batch_id and extras[0].batch_id not in excluded:
        synth_job_uid = extras[0].batch_id
    elif contributing:
        synth_job_uid = contributing[0]
    elif extras:
        synth_job_uid = extras[0].batch_id or "rolling-empty"
    else:
        synth_job_uid = "rolling-empty"

    if run_description is None:
        run_description = (
            f"rolling aggregate of {len(contributing)} job(s) over last "
            f"{scan_limit} (matched: engine={engine}, bar_content={bar_content}, "
            f"map={map_}, scenario={scenario}, machine_type={machine_type})"
        )

    report = summarize(
        pool,
        submitted=submitted_total,
        job_uid=synth_job_uid,
        run_description=run_description,
    )
    return report, contributing


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
    _print_cost(report)


def _print_cost(report: BatchReport) -> None:
    if report.cached:
        print("compute: $0.000 (cached run)")
        return
    if report.total_billable_s is None:
        return
    shape = f"{report.instance_type}/{report.region}"
    if report.compute_usd is not None and report.price_per_vm_hour_usd is not None:
        print(
            f"compute: ${report.compute_usd:.3f}  "
            f"(billable= {report.total_billable_s:.0f}s "
            f"@ ${report.price_per_vm_hour_usd:.6f}/hr, {shape} spot)"
        )
    else:
        print(
            f"compute: no spot price for {shape}; "
            f"billable= {report.total_billable_s:.0f}s"
        )
