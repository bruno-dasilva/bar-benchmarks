"""Top-level orchestration for `bar-bench run`: resolve, ensure, submit, poll, reconcile, report."""

from __future__ import annotations

import json
import secrets
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from google.cloud import batch_v1

from bar_benchmarks.orchestrator import artifacts, batch_submitter
from bar_benchmarks.orchestrator.catalog import Catalog
from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import BatchConfig, BatchReport

_TERMINAL_JOB_STATES = {
    batch_v1.JobStatus.State.SUCCEEDED,
    batch_v1.JobStatus.State.FAILED,
    batch_v1.JobStatus.State.CANCELLED,
    batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
}


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


def _wait_for_terminal(job_name: str, *, interval_s: float = 15.0) -> batch_v1.Job:
    client = batch_v1.BatchServiceClient()
    while True:
        job = client.get_job(name=job_name)
        print(f"[run] state={job.status.state.name}", file=sys.stderr)
        if job.status.state in _TERMINAL_JOB_STATES:
            return job
        time.sleep(interval_s)


def _upload_run_info(cfg: BatchConfig, job_uid: str, submitted_at: datetime) -> None:
    """Write the per-run parameters blob to `<results-bucket>/<job_uid>/run.json`.

    Post-hoc breadcrumb for "what was this run": catalog names, scenario
    folder, machine shape, operator description, submit timestamp.
    Separate from the artifacts-bucket manifest.json (consumed by task VMs).
    """
    from google.cloud import storage

    body = json.dumps(
        {
            "job_uid": job_uid,
            "submitted_at": submitted_at.isoformat(),
            "run_description": cfg.run_description,
            "engine": cfg.engine_name,
            "bar_content": cfg.bar_content_name,
            "map": cfg.map_name,
            "scenario": cfg.scenario_dir.name,
            "count": cfg.count,
            "region": cfg.region,
            "machine_type": cfg.machine_type,
            "min_cpu_platform": cfg.min_cpu_platform,
            "max_run_duration_s": cfg.max_run_duration_s,
        },
        indent=2,
        sort_keys=True,
    ).encode()
    bucket_name = cfg.results_bucket.removeprefix("gs://")
    bucket = storage.Client(project=cfg.project).bucket(bucket_name)
    bucket.blob(f"{job_uid}/run.json").upload_from_string(body, content_type="application/json")
    print(f"[run] wrote run.json → gs://{bucket_name}/{job_uid}/run.json", file=sys.stderr)


def _missing_task_indices(
    results_bucket: str, job_uid: str, submitted: int, *, project: str | None = None
) -> list[int]:
    """Return the sorted list of task indices that never uploaded results.json."""
    from google.cloud import storage

    client = storage.Client(project=project)
    bucket_name = results_bucket.removeprefix("gs://")
    bucket = client.bucket(bucket_name)
    prefix = f"{job_uid}/"
    present: set[int] = set()
    for blob in client.list_blobs(bucket, prefix=prefix):
        # Expect key: <job_uid>/<task_index>/results.json
        parts = blob.name[len(prefix):].split("/")
        if len(parts) == 2 and parts[1] == "results.json":
            try:
                present.add(int(parts[0]))
            except ValueError:
                continue
    return sorted(set(range(submitted)) - present)


def run(cfg: BatchConfig, *, report_json_path: Path | None = None) -> BatchReport:
    job_uid = _mint_job_id()
    print(f"[run] job_uid={job_uid}", file=sys.stderr)

    cat = Catalog.load(cfg.catalog_path)

    wheel = cfg.wheel if cfg.wheel else artifacts.build_wheel()
    print(f"[run] wheel={wheel.name}", file=sys.stderr)

    overlay = _pack_overlay(cfg.scenario_dir)
    print(f"[run] overlay packed from {cfg.scenario_dir}", file=sys.stderr)

    artifacts.build_and_upload(cfg, job_uid, cat=cat, overlay=overlay, wheel=wheel)

    _upload_run_info(cfg, job_uid, datetime.now(UTC))

    print(f"[run] submitting Batch Job ({cfg.count} tasks)", file=sys.stderr)
    job = batch_submitter.submit(cfg, job_id=job_uid)
    print(f"[run] submitted: {job.name}", file=sys.stderr)

    final = _wait_for_terminal(job.name)
    print(f"[run] terminal state: {final.status.state.name}", file=sys.stderr)

    missing = _missing_task_indices(cfg.results_bucket, job_uid, cfg.count, project=cfg.project)
    if missing:
        print(f"[run] missing results for task indices: {missing}", file=sys.stderr)

    report = aggregate.from_bucket(
        cfg.results_bucket,
        job_uid,
        submitted=cfg.count,
        project=cfg.project,
        run_description=cfg.run_description,
    )
    aggregate.print_report(report)
    if report_json_path is not None:
        report_json_path.write_text(report.model_dump_json(indent=2))
        print(f"[run] wrote report JSON → {report_json_path}", file=sys.stderr)
    return report
