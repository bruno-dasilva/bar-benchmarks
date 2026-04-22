"""Archive a per-run parameters blob into the results bucket.

Written at `<results-bucket>/<job_uid>/run.json` as a sibling to the
per-task `<job_uid>/<task_index>/results.json` files. This is the
post-hoc breadcrumb for "what was this run": catalog names, scenario
folder, machine shape, operator description, submit timestamp.

Separate from the artifacts-bucket `manifest.json`, which is consumed by
the task VMs' runner. This file is for humans and future tooling
reviewing past runs.
"""

from __future__ import annotations

import json
from datetime import datetime

from bar_benchmarks.types import BatchConfig


def run_info_bytes(cfg: BatchConfig, job_uid: str, submitted_at: datetime) -> bytes:
    body = {
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
    }
    return json.dumps(body, indent=2, sort_keys=True).encode()


def upload(
    results_bucket: str,
    job_uid: str,
    body: bytes,
    *,
    project: str | None = None,
    client=None,
) -> None:
    if client is None:
        from google.cloud import storage

        client = storage.Client(project=project)
    bucket_name = results_bucket.removeprefix("gs://")
    key = f"{job_uid}/run.json"
    bucket = client.bucket(bucket_name)
    bucket.blob(key).upload_from_string(body, content_type="application/json")
