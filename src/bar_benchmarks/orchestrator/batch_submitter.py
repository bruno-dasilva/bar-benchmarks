"""Build and submit the google.cloud.batch_v1.Job for one benchmark batch.

Keeps the Job proto shape in one place so it can be snapshot-tested and
swapped wholesale if we move off GCP Batch later.
"""

from __future__ import annotations

from google.cloud import batch_v1

from bar_benchmarks.types import BatchConfig

SERVICE_ACCOUNT = "benchmark-runner@bar-experiments.iam.gserviceaccount.com"
MIN_CPU_PLATFORM = "Intel Skylake"
BOOT_DISK_GB = 50
BOOT_DISK_TYPE = "pd-balanced"

ENV_VARS = {
    "BAR_ARTIFACTS_DIR": "/mnt/artifacts",
    "BAR_RESULTS_DIR": "/mnt/results",
    "BAR_DATA_DIR": "/var/bar-data",
    "BAR_RUN_DIR": "/var/bar-run",
    "BAR_ENGINE_DIR": "/opt/recoil",
    "BAR_BENCHMARK_OUTPUT_PATH": "benchmark-results.json",
}

BOOTSTRAP_SCRIPT = r"""#!/bin/sh
set -eu
apt-get update -y >/dev/null
apt-get install -y --no-install-recommends python3 python3-pip python3-venv >/dev/null
WHEEL="$(ls /mnt/artifacts/bar_benchmarks-*.whl | head -n1)"
# Install pydantic with its deps (pulls pydantic_core), then the wheel
# without deps (skips control-host-only deps like typer, google-cloud-*).
python3 -m pip install --break-system-packages pydantic
python3 -m pip install --break-system-packages --no-deps "$WHEEL"
"""


def _runnable(text: str, *, background: bool = False, always_run: bool = False) -> batch_v1.Runnable:
    return batch_v1.Runnable(
        script=batch_v1.Runnable.Script(text=text),
        background=background,
        always_run=always_run,
    )


def build_job(
    cfg: BatchConfig,
    job_uid: str,
) -> batch_v1.Job:
    artifacts_bucket = cfg.artifacts_bucket.removeprefix("gs://")
    results_bucket = cfg.results_bucket.removeprefix("gs://")

    volumes = [
        batch_v1.Volume(
            gcs=batch_v1.GCS(remote_path=f"{artifacts_bucket}/{job_uid}"),
            mount_path="/mnt/artifacts",
        ),
        batch_v1.Volume(
            gcs=batch_v1.GCS(remote_path=f"{results_bucket}/{job_uid}"),
            mount_path="/mnt/results",
        ),
    ]

    runnables = [
        _runnable(BOOTSTRAP_SCRIPT),
        _runnable(
            "python3 -m bar_benchmarks.poison.monitor",
            background=True,
            always_run=True,
        ),
        _runnable("python3 -m bar_benchmarks.task.main"),
        _runnable("python3 -m bar_benchmarks.task.collector", always_run=True),
    ]

    task_spec = batch_v1.TaskSpec(
        runnables=runnables,
        volumes=volumes,
        environment=batch_v1.Environment(variables=ENV_VARS),
        max_run_duration={"seconds": cfg.max_run_duration_s},
        max_retry_count=0,
    )

    group = batch_v1.TaskGroup(
        task_count=cfg.count,
        parallelism=cfg.count,
        task_spec=task_spec,
    )

    policy = batch_v1.AllocationPolicy.InstancePolicy(
        machine_type=cfg.machine_type,
        min_cpu_platform=MIN_CPU_PLATFORM,
        provisioning_model=batch_v1.AllocationPolicy.ProvisioningModel.SPOT,
        boot_disk=batch_v1.AllocationPolicy.Disk(
            size_gb=BOOT_DISK_GB,
            type_=BOOT_DISK_TYPE,
        ),
    )

    allocation = batch_v1.AllocationPolicy(
        instances=[batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=policy)],
        service_account=batch_v1.ServiceAccount(email=SERVICE_ACCOUNT),
    )

    return batch_v1.Job(
        task_groups=[group],
        allocation_policy=allocation,
        logs_policy=batch_v1.LogsPolicy(
            destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        ),
    )


def submit(cfg: BatchConfig, job_id: str, *, client: batch_v1.BatchServiceClient | None = None) -> batch_v1.Job:
    """Submit the Job. Returns the created Job with server-assigned fields (uid, state)."""
    if client is None:
        client = batch_v1.BatchServiceClient()
    job = build_job(cfg, job_uid=job_id)
    parent = f"projects/{cfg.project}/locations/{cfg.region}"
    return client.create_job(parent=parent, job=job, job_id=job_id)
