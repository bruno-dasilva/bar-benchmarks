from __future__ import annotations

import os
from pathlib import Path


def _env_path(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default))


def artifacts_dir() -> Path:
    """GCS FUSE mount holding the 5 input artifacts + wheel. Read-only on the VM."""
    return _env_path("BAR_ARTIFACTS_DIR", "/mnt/artifacts")


def results_dir() -> Path:
    """GCS FUSE mount the collector writes results.json into, scoped per job."""
    return _env_path("BAR_RESULTS_DIR", "/mnt/results")


def data_dir() -> Path:
    """Local --write-dir for spring-headless; holds games/, maps/, benchmark-results.json."""
    return _env_path("BAR_DATA_DIR", "/var/bar-data")


def run_dir() -> Path:
    """Local task scratch for preflight.json, verdict.json, poison.json."""
    return _env_path("BAR_RUN_DIR", "/var/bar-run")


def engine_dir() -> Path:
    """Local extraction target for engine.tar.gz."""
    return _env_path("BAR_ENGINE_DIR", "/opt/recoil")


def benchmark_output_path() -> Path:
    """Absolute path of the overlay's benchmark JSON, relative to data_dir()."""
    rel = os.environ.get("BAR_BENCHMARK_OUTPUT_PATH", "benchmark-results.json")
    return data_dir() / rel


def batch_job_uid() -> str | None:
    """Injected by Batch on the VM; None when running on a dev machine."""
    return os.environ.get("BATCH_JOB_UID")


def batch_task_index() -> str:
    """Injected by Batch on the VM; defaults to '0' for dev smoke runs."""
    return os.environ.get("BATCH_TASK_INDEX", "0")
