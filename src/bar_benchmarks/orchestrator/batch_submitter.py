"""Build and submit the google.cloud.batch_v1.Job for one benchmark batch.

Keeps the Job proto shape in one place so it can be snapshot-tested and
swapped wholesale if we move off GCP Batch later.
"""

from __future__ import annotations

from google.cloud import batch_v1

from bar_benchmarks.types import BatchConfig

BOOT_DISK_GB = 50
BOOT_DISK_TYPE = "pd-balanced"
# Dedicated scratch disk for per-VM working sets (engine extract, BAR
# data, runtime pypkgs). Batch's default root-fs scratch under /mnt/disks
# is tiny (a few hundred MB) so pip --target hits ENOSPC; 10 GB is
# ample headroom for the wheel, pydantic, and the engine extraction.
SCRATCH_DEVICE_NAME = "bar-scratch"
SCRATCH_DISK_GB = 10
SCRATCH_DISK_TYPE = "pd-balanced"
SCRATCH_MOUNT = "/mnt/disks/scratch"

# Each runnable runs inside this container. Using an official Python
# image decouples the task runtime from whatever base image Batch picks
# for the host VM (some Batch image variants still ship Debian 11 /
# Python 3.9 with a broken system pip). python:3.11-slim is Debian-12
# based and ships `python3`, `pip`, and `venv` ready to use.
CONTAINER_IMAGE = "python:3.11-slim"

# Batch VM rootfs is read-only outside /mnt/disks/*. Every host path
# we write to — GCS FUSE mounts AND per-VM scratch — must live under
# /mnt/disks. Containers see the canonical /mnt/artifacts, /var/bar-run
# etc. through bind remapping, so task code / env vars don't change.
_HOST_ARTIFACTS = "/mnt/disks/artifacts"
_HOST_ARTIFACTS_BUCKET = "/mnt/disks/artifacts-bucket"
_HOST_RESULTS = "/mnt/disks/results"
# Scratch paths live on the attached SCRATCH_MOUNT data disk.
_HOST_DATA = f"{SCRATCH_MOUNT}/bar-data"
_HOST_RUN = f"{SCRATCH_MOUNT}/bar-run"
_HOST_ENGINE = f"{SCRATCH_MOUNT}/engine"

CONTAINER_VOLUMES = [
    f"{_HOST_ARTIFACTS}:/mnt/artifacts",
    f"{_HOST_ARTIFACTS_BUCKET}:/mnt/artifacts-bucket",
    f"{_HOST_RESULTS}:/mnt/results",
    f"{_HOST_DATA}:/var/bar-data",
    f"{_HOST_RUN}:/var/bar-run",
    f"{_HOST_ENGINE}:/opt/recoil",
]

# pip --target layout instead of a venv: Batch mounts /mnt/disks/*
# with noexec in some images, which breaks `python -m venv --seed`
# (the seeded python3 copy lands inside the shared mount and can't be
# re-executed). --target just drops packages into a directory with no
# executables of its own, and PYTHONPATH makes subsequent runnables
# pick them up.
PACKAGES_DIR = "/var/bar-run/pypkgs"

ENV_VARS = {
    "BAR_ARTIFACTS_DIR": "/mnt/artifacts",
    "BAR_ARTIFACTS_BUCKET_DIR": "/mnt/artifacts-bucket",
    "BAR_RESULTS_DIR": "/mnt/results",
    "BAR_DATA_DIR": "/var/bar-data",
    "BAR_RUN_DIR": "/var/bar-run",
    "BAR_ENGINE_DIR": "/opt/recoil",
    "BAR_BENCHMARK_OUTPUT_PATH": "benchmark-results.json",
    "PYTHONPATH": PACKAGES_DIR,
}

BOOTSTRAP_SCRIPT = r"""set -eu
# Idempotent: if the Batch agent re-executes this runnable on the same
# VM, a half-populated pypkgs tree trips pip --target's cross-device
# copytree path and fails with FileExistsError. Start clean.
rm -rf /var/bar-run/pypkgs
mkdir -p /var/bar-run/pypkgs
# pydantic with deps (pulls pydantic_core); wheel without deps (skips
# control-host-only deps like typer, google-cloud-*).
pip install --no-cache-dir --target /var/bar-run/pypkgs pydantic
WHEEL="$(ls /mnt/artifacts/bar_benchmarks-*.whl | head -n1)"
pip install --no-cache-dir --target /var/bar-run/pypkgs --no-deps "$WHEEL"
"""


def _container_runnable(
    commands: list[str],
    *,
    background: bool = False,
    always_run: bool = False,
) -> batch_v1.Runnable:
    return batch_v1.Runnable(
        container=batch_v1.Runnable.Container(
            image_uri=CONTAINER_IMAGE,
            entrypoint="",
            commands=commands,
            volumes=CONTAINER_VOLUMES,
        ),
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
            mount_path=_HOST_ARTIFACTS,
        ),
        batch_v1.Volume(
            gcs=batch_v1.GCS(remote_path=artifacts_bucket),
            mount_path=_HOST_ARTIFACTS_BUCKET,
        ),
        batch_v1.Volume(
            gcs=batch_v1.GCS(remote_path=f"{results_bucket}/{job_uid}"),
            mount_path=_HOST_RESULTS,
        ),
        batch_v1.Volume(
            device_name=SCRATCH_DEVICE_NAME,
            mount_path=SCRATCH_MOUNT,
        ),
    ]

    runnables = [
        _container_runnable(["/bin/sh", "-c", BOOTSTRAP_SCRIPT]),
        _container_runnable(
            ["python3", "-m", "bar_benchmarks.poison.monitor"],
            background=True,
            always_run=True,
        ),
        _container_runnable(["python3", "-m", "bar_benchmarks.task.main"]),
        _container_runnable(
            ["python3", "-m", "bar_benchmarks.task.collector"],
            always_run=True,
        ),
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

    policy_kwargs: dict = {
        "machine_type": cfg.machine_type,
        "provisioning_model": batch_v1.AllocationPolicy.ProvisioningModel.SPOT,
    }
    if cfg.min_cpu_platform:
        policy_kwargs["min_cpu_platform"] = cfg.min_cpu_platform

    policy = batch_v1.AllocationPolicy.InstancePolicy(
        **policy_kwargs,
        boot_disk=batch_v1.AllocationPolicy.Disk(
            size_gb=BOOT_DISK_GB,
            type_=BOOT_DISK_TYPE,
        ),
        disks=[
            batch_v1.AllocationPolicy.AttachedDisk(
                new_disk=batch_v1.AllocationPolicy.Disk(
                    size_gb=SCRATCH_DISK_GB,
                    type_=SCRATCH_DISK_TYPE,
                ),
                device_name=SCRATCH_DEVICE_NAME,
            ),
        ],
    )

    service_account_email = (
        cfg.service_account
        or f"benchmark-runner@{cfg.project}.iam.gserviceaccount.com"
    )
    allocation = batch_v1.AllocationPolicy(
        instances=[batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=policy)],
        service_account=batch_v1.ServiceAccount(email=service_account_email),
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
