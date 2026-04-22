"""Top-level orchestration for `bar-bench run`: resolve, ensure, submit, poll, reconcile, report."""

from __future__ import annotations

import secrets
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from bar_benchmarks.orchestrator import (
    artifacts,
    batch_submitter,
    poller,
    reconcile,
    run_info,
)
from bar_benchmarks.orchestrator.catalog import Catalog
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


def _pack_overlay(scenario_dir: Path) -> Path:
    """Tar the scenario's bar-data/ tree into a temp overlay.tar.gz. An
    absent bar-data/ yields an empty tarball so the runner's extract step
    is a no-op rather than a missing-file error."""
    tmp = Path(tempfile.mkstemp(prefix="overlay-", suffix=".tar.gz")[1])
    bar_data = scenario_dir / "bar-data"
    if bar_data.is_dir():
        subprocess.run(
            ["tar", "-C", str(bar_data), "-czf", str(tmp), "."],
            check=True, stdout=sys.stdout, stderr=sys.stderr,
        )
    else:
        subprocess.run(
            ["tar", "-czf", str(tmp), "-T", "/dev/null"],
            check=True, stdout=sys.stdout, stderr=sys.stderr,
        )
    return tmp


def run(cfg: BatchConfig) -> BatchReport:
    job_uid = _mint_job_id()
    print(f"[run] job_uid={job_uid}", file=sys.stderr)

    cat = Catalog.load(cfg.catalog_path)

    wheel = cfg.wheel if cfg.wheel else artifacts.build_wheel(_project_root())
    print(f"[run] wheel={wheel.name}", file=sys.stderr)

    overlay = _pack_overlay(cfg.scenario_dir)
    print(f"[run] overlay packed from {cfg.scenario_dir}", file=sys.stderr)

    plan = artifacts.plan(cfg, job_uid, cat=cat, overlay=overlay, wheel=wheel)
    manifest = artifacts.manifest_bytes(cfg, job_uid, plan)

    artifacts_bucket = cfg.artifacts_bucket.removeprefix("gs://")
    artifacts.ensure_and_upload(
        artifacts_bucket, cfg, plan, manifest, cat=cat, project=cfg.project
    )

    submitted_at = datetime.now(UTC)
    run_info.upload(
        cfg.results_bucket,
        job_uid,
        run_info.run_info_bytes(cfg, job_uid, submitted_at),
        project=cfg.project,
    )
    results_bucket_name = cfg.results_bucket.removeprefix("gs://")
    print(
        f"[run] wrote run.json → gs://{results_bucket_name}/{job_uid}/run.json",
        file=sys.stderr,
    )

    print(f"[run] submitting Batch Job ({cfg.count} tasks)", file=sys.stderr)
    job = batch_submitter.submit(cfg, job_id=job_uid)
    print(f"[run] submitted: {job.name}", file=sys.stderr)

    def _log(j):
        print(f"[run] state={j.status.state.name}", file=sys.stderr)

    final = poller.wait(job.name, on_update=_log)
    print(f"[run] terminal state: {final.status.state.name}", file=sys.stderr)

    rec = reconcile.reconcile(cfg.results_bucket, job_uid, cfg.count, project=cfg.project)
    if rec.missing_indices:
        print(
            f"[run] missing results for task indices: {rec.missing_indices}",
            file=sys.stderr,
        )

    report = aggregate.from_bucket(
        cfg.results_bucket,
        job_uid,
        submitted=cfg.count,
        project=cfg.project,
        run_description=cfg.run_description,
    )
    aggregate.print_report(report)
    return report
