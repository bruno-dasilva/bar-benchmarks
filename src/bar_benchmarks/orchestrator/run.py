"""Top-level orchestration for `bar-bench run`: build, upload, submit, poll, reconcile, report."""

from __future__ import annotations

import secrets
import sys
import time
from pathlib import Path

from bar_benchmarks.orchestrator import artifacts, batch_submitter, poller, reconcile
from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import BatchConfig, BatchReport


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("Could not locate project root (no pyproject.toml on ancestor path)")


def _mint_job_id() -> str:
    return f"bar-bench-{int(time.time())}-{secrets.token_hex(3)}"


def run(cfg: BatchConfig) -> BatchReport:
    job_uid = _mint_job_id()
    print(f"[run] job_uid={job_uid}", file=sys.stderr)

    wheel = cfg.wheel if cfg.wheel else artifacts.build_wheel(_project_root())
    print(f"[run] wheel={wheel.name}", file=sys.stderr)

    plan = artifacts.plan(cfg, job_uid, wheel)
    manifest = artifacts.manifest_bytes(cfg, job_uid, plan)

    artifacts_bucket = cfg.artifacts_bucket.removeprefix("gs://")
    print(f"[run] uploading artifacts → gs://{artifacts_bucket}/{job_uid}/", file=sys.stderr)
    artifacts.upload(artifacts_bucket, plan, manifest)

    print(f"[run] submitting Batch Job ({cfg.count} tasks)", file=sys.stderr)
    job = batch_submitter.submit(cfg, job_id=job_uid)
    print(f"[run] submitted: {job.name}", file=sys.stderr)

    def _log(j):
        print(f"[run] state={j.status.state.name}", file=sys.stderr)

    final = poller.wait(job.name, on_update=_log)
    print(f"[run] terminal state: {final.status.state.name}", file=sys.stderr)

    rec = reconcile.reconcile(cfg.results_bucket, job_uid, cfg.count)
    if rec.missing_indices:
        print(
            f"[run] missing results for task indices: {rec.missing_indices}",
            file=sys.stderr,
        )

    report = aggregate.from_bucket(cfg.results_bucket, job_uid, submitted=cfg.count)
    aggregate.print_report(report)
    return report
