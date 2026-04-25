"""Decide whether to skip a Batch run by aggregating recent matching results.

Every `bar-bench run` writes `<results-bucket>/<job_uid>/run.json` with the
shape-defining parameters at submit time, and `results.json` per VM as the
batch executes. The cache is now a *rolling sample window*: scan the last
N most-recent `bar-bench-*` job_uids, pool every results.json from runs
whose `(engine, bar_content, map, scenario, machine_type)` matches the
request, and skip the new submission iff the pool already has enough valid
results.

Match is intentionally loose — `count` and `iterations` are *not* matched —
so a 10-VM run last week and a 20-VM run today both feed the same pool.
The `report.json` completion sentinel is no longer required either: a
half-finished prior run still contributes whatever valid results.json
files it managed to upload.

Job UIDs embed a Unix timestamp (`bar-bench-<epoch>-<rand>`), so lexical
sort by timestamp gives "most recent first" without extra metadata.
"""

from __future__ import annotations

import sys

from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import BatchReport


def find_rolling_window(
    *,
    results_bucket: str,
    engine: str,
    bar_content: str,
    map_: str,
    scenario: str,
    machine_type: str,
    min_samples: int = 50,
    scan_limit: int = 100,
    project: str | None = None,
    client=None,
) -> tuple[BatchReport, list[str], bool]:
    """Aggregate the rolling window and decide hit vs miss.

    Returns `(report, contributing_job_uids, hit)`. `hit` is True iff the
    pool has at least `min_samples` valid results — caller should skip
    submitting a fresh Batch job in that case and emit `report` directly.
    """
    report, contributing = aggregate.from_window(
        results_bucket=results_bucket,
        engine=engine,
        bar_content=bar_content,
        map_=map_,
        scenario=scenario,
        machine_type=machine_type,
        scan_limit=scan_limit,
        project=project,
        client=client,
    )
    hit = report.valid >= min_samples
    if hit:
        print(
            f"[lookup] CACHE HIT — rolling window has valid={report.valid} "
            f"(>= min_samples={min_samples}) across {len(contributing)} job(s); "
            f"skipping fresh Batch submission",
            file=sys.stderr,
        )
    else:
        print(
            f"[lookup] cache miss — rolling window has valid={report.valid} "
            f"(< min_samples={min_samples}) across {len(contributing)} job(s); "
            f"a fresh Batch job will be submitted",
            file=sys.stderr,
        )
    return report, contributing, hit
