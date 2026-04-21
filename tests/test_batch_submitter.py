from __future__ import annotations

from pathlib import Path

from google.cloud import batch_v1

from bar_benchmarks.orchestrator import batch_submitter
from bar_benchmarks.types import BatchConfig


def _cfg() -> BatchConfig:
    return BatchConfig(
        engine=Path("/tmp/engine.tar.gz"),
        bar_content=Path("/tmp/bar-content.tar.gz"),
        overlay=Path("/tmp/overlay.tar.gz"),
        map=Path("/tmp/map.sd7"),
        startscript=Path("/tmp/startscript.txt"),
        count=4,
        project="bar-experiments",
        region="us-west4",
        artifacts_bucket="gs://bar-experiments-bench-artifacts",
        results_bucket="gs://bar-experiments-bench-results",
        machine_type="n1-standard-8",
        max_run_duration_s=1800,
    )


def test_job_shape_snapshot():
    job = batch_submitter.build_job(_cfg(), job_uid="job-xyz")

    assert len(job.task_groups) == 1
    group = job.task_groups[0]
    assert group.task_count == 4
    assert group.parallelism == 4

    spec = group.task_spec
    assert spec.max_retry_count == 0
    assert spec.max_run_duration.seconds == 1800

    # Volumes: artifacts + results, scoped under job_uid.
    mounts = {v.mount_path: v.gcs.remote_path for v in spec.volumes}
    assert mounts["/mnt/artifacts"] == "bar-experiments-bench-artifacts/job-xyz"
    assert mounts["/mnt/results"] == "bar-experiments-bench-results/job-xyz"

    # Four runnables in order: bootstrap, poison (bg+alwaysRun), main, collector (alwaysRun).
    assert len(spec.runnables) == 4
    boot, poison, main, coll = spec.runnables
    assert "pip install" in boot.script.text
    assert poison.background is True
    assert poison.always_run is True
    assert "poison.monitor" in poison.script.text
    assert "task.main" in main.script.text
    assert coll.always_run is True
    assert "task.collector" in coll.script.text

    # Env vars injected.
    env = dict(spec.environment.variables)
    assert env["BAR_ARTIFACTS_DIR"] == "/mnt/artifacts"
    assert env["BAR_RESULTS_DIR"] == "/mnt/results"
    assert env["BAR_BENCHMARK_OUTPUT_PATH"] == "benchmark-results.json"

    # Allocation policy.
    inst = job.allocation_policy.instances[0].policy
    assert inst.machine_type == "n1-standard-8"
    assert inst.min_cpu_platform == "Intel Skylake"
    assert inst.provisioning_model == batch_v1.AllocationPolicy.ProvisioningModel.SPOT
    assert inst.boot_disk.size_gb == 50
    assert inst.boot_disk.type_ == "pd-balanced"
    assert (
        job.allocation_policy.service_account.email
        == "benchmark-runner@bar-experiments.iam.gserviceaccount.com"
    )
    assert job.logs_policy.destination == batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
