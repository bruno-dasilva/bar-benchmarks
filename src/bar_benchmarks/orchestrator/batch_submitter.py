"""Build and submit the google.cloud.batch_v1.Job for one benchmark batch.

Keeps the Job proto shape in one place so it can be snapshot-tested and
swapped wholesale if we move off GCP Batch later.
"""

from __future__ import annotations

from google.cloud import batch_v1

from bar_benchmarks.types import BatchConfig

# Default min_cpu_platform per multi-generation machine family. Pinning
# tightens reproducibility on families that span several CPU generations
# (n1 = Sandy Bridge … Skylake, n2 = Cascade/Ice Lake, etc.). Single-
# generation families (c3, c3d, c4, c4d, h3, n4, t2a, e2) are omitted —
# Batch either ignores or rejects a pin there. Override via BatchConfig
# .min_cpu_platform ("" to force-unset, non-empty string to pin).
_DEFAULT_MIN_CPU_PLATFORM: dict[str, str] = {
    "n1": "Intel Skylake",
    "n2": "Intel Ice Lake",
    "n2d": "AMD Milan",
    "c2": "Intel Cascade Lake",
    "c2d": "AMD Milan",
    "m1": "Intel Skylake",
    "m2": "Intel Cascade Lake",
    "m3": "Intel Ice Lake",
    "t2d": "AMD Milan",
}


def default_min_cpu_platform(machine_type: str) -> str | None:
    """Return the canonical min_cpu_platform for a machine family, or
    None for single-generation families where pinning is a no-op."""
    family = machine_type.split("-", 1)[0]
    return _DEFAULT_MIN_CPU_PLATFORM.get(family)


def _resolve_min_cpu_platform(machine_type: str, override: str | None) -> str | None:
    # Explicit "" means "force unset" (user opting out of the default).
    if override == "":
        return None
    if override is not None:
        return override
    return default_min_cpu_platform(machine_type)


# Per-task resource claim. Batch divides vCPUs and memory per VM by
# these to auto-derive tasks-per-VM, so sizing these ⇒ choosing how
# many tasks pack onto a VM. At 16000 on c2d-standard-16 (the default
# machine type) this resolves to one task per VM — a whole VM owned by
# a single benchmark run, which is the point. Halve TASK_CPU_MILLI to
# pack two tasks per VM, or bump --machine-type to c2d-standard-32 and
# keep the same one-task-per-VM shape. Memory sizing is analogous.
TASK_CPU_MILLI = 16000
TASK_MEMORY_MIB = 28 * 1024

BOOT_DISK_GB = 30
BOOT_DISK_TYPE = "pd-balanced"
# Dedicated scratch disk for per-VM working sets (engine extract, BAR
# data, runtime pypkgs). Batch's default root-fs scratch under /mnt/disks
# is tiny (a few hundred MB) so pip --target hits ENOSPC. When K tasks
# share a VM this disk holds K full copies of the per-task trees —
# scale SCRATCH_DISK_GB with K if tasks start hitting ENOSPC.
SCRATCH_DEVICE_NAME = "bar-scratch"
SCRATCH_DISK_GB = 10
SCRATCH_DISK_TYPE = "pd-balanced"
SCRATCH_MOUNT = "/mnt/disks/scratch"
SCRATCH_CONTAINER_PATH = "/var/bar-scratch"

# Each runnable runs inside this container. The image is hosted in
# Artifact Registry (batch-runtime/Dockerfile builds it) so VMs
# on private IPs can pull without external network access — Private
# Google Access covers *.pkg.dev. Pre-installed pydantic means the
# bootstrap doesn't hit PyPI either.
CONTAINER_IMAGE = (
    "us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:2026-04-22"
)

# Batch VM rootfs is read-only outside /mnt/disks/*. GCS FUSE mounts and
# the scratch disk live under /mnt/disks; containers see
# /mnt/artifacts*, /mnt/results, and /var/bar-scratch via bind-mount.
_HOST_ARTIFACTS = "/mnt/disks/artifacts"
_HOST_ARTIFACTS_BUCKET = "/mnt/disks/artifacts-bucket"
_HOST_RESULTS = "/mnt/disks/results"

CONTAINER_VOLUMES = [
    f"{_HOST_ARTIFACTS}:/mnt/artifacts",
    f"{_HOST_ARTIFACTS_BUCKET}:/mnt/artifacts-bucket",
    f"{_HOST_RESULTS}:/mnt/results",
    f"{SCRATCH_MOUNT}:{SCRATCH_CONTAINER_PATH}",
]

# Static TaskSpec env. BAR_DATA_DIR / BAR_RUN_DIR / BAR_ENGINE_DIR /
# PYTHONPATH are NOT here — they vary per task and are injected by
# PER_TASK_ENV_WRAPPER below, which resolves $BATCH_TASK_INDEX at task
# start. TaskSpec.environment is static at submit time so it can't
# encode per-task values.
ENV_VARS = {
    "BAR_ARTIFACTS_DIR": "/mnt/artifacts",
    "BAR_ARTIFACTS_BUCKET_DIR": "/mnt/artifacts-bucket",
    "BAR_RESULTS_DIR": "/mnt/results",
    "BAR_BENCHMARK_OUTPUT_PATH": "benchmark-results.json",
}

# Wrapper prefixed to every runnable's command list. Reads the
# Batch-injected BATCH_TASK_INDEX, exports the four per-task env vars
# task code reads via bar_benchmarks.paths, then execs the real
# command. Keeps paths.py + runner/collector unaware of co-scheduling —
# each task sees its own /var/bar-scratch/tasks/$idx subtree as if it
# owned the VM.
PER_TASK_ENV_WRAPPER = r"""set -eu
idx="${BATCH_TASK_INDEX:-0}"
root=/var/bar-scratch/tasks/$idx
export BAR_DATA_DIR=$root/bar-data
export BAR_RUN_DIR=$root/bar-run
export BAR_ENGINE_DIR=$root/engine
export PYTHONPATH=$root/pypkgs
exec "$@"
"""

BOOTSTRAP_SCRIPT = r"""set -eu
idx="${BATCH_TASK_INDEX:-0}"
root=/var/bar-scratch/tasks/$idx
# Idempotent: a half-populated pypkgs from a same-VM retry trips pip
# --target's cross-device copytree path. Start clean.
rm -rf "$root/pypkgs"
mkdir -p "$root/bar-data" "$root/bar-run" "$root/engine" "$root/pypkgs"
# Wheel without deps (pydantic is baked into the container image;
# control-host-only deps like typer, google-cloud-* aren't needed).
WHEEL="$(ls /mnt/artifacts/bar_benchmarks-*.whl | head -n1)"
pip install --no-cache-dir --target "$root/pypkgs" --no-deps "$WHEEL"
"""


def _container_runnable(
    commands: list[str],
    *,
    image: str,
    background: bool = False,
    always_run: bool = False,
) -> batch_v1.Runnable:
    # sh -c '<wrapper>' -- arg1 arg2 ...  →  the wrapper script runs
    # with "$@" = commands, then execs them. `--` becomes $0 and is
    # ignored by the wrapper.
    wrapped = ["/bin/sh", "-c", PER_TASK_ENV_WRAPPER, "--", *commands]
    return batch_v1.Runnable(
        container=batch_v1.Runnable.Container(
            image_uri=image,
            entrypoint="",
            commands=wrapped,
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
        _container_runnable(["/bin/sh", "-c", BOOTSTRAP_SCRIPT], image=CONTAINER_IMAGE),
        _container_runnable(
            ["python3", "-m", "bar_benchmarks.task.runner"], image=CONTAINER_IMAGE
        ),
        _container_runnable(
            ["python3", "-m", "bar_benchmarks.task.collector"],
            image=CONTAINER_IMAGE,
            always_run=True,
        ),
    ]

    task_spec = batch_v1.TaskSpec(
        runnables=runnables,
        volumes=volumes,
        environment=batch_v1.Environment(variables=ENV_VARS),
        compute_resource=batch_v1.ComputeResource(
            cpu_milli=TASK_CPU_MILLI,
            memory_mib=TASK_MEMORY_MIB,
        ),
        max_run_duration={"seconds": cfg.max_run_duration_s},
        max_retry_count=0,
    )

    # task_count_per_node is omitted: Batch auto-derives K from
    # vCPUs/memory per VM ÷ per-task compute_resource.
    group = batch_v1.TaskGroup(
        task_count=cfg.count,
        parallelism=cfg.count,
        task_spec=task_spec,
    )

    policy_kwargs: dict = {
        "machine_type": cfg.machine_type,
        "provisioning_model": batch_v1.AllocationPolicy.ProvisioningModel.SPOT,
    }
    min_cpu = _resolve_min_cpu_platform(cfg.machine_type, cfg.min_cpu_platform)
    if min_cpu:
        policy_kwargs["min_cpu_platform"] = min_cpu

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
    # Private-IP-only networking: every Batch VM lands in the project's
    # `default` VPC with no external IP. The `default` subnet in
    # us-central1 has Private Google Access enabled, so GCS / Logging /
    # Artifact Registry stay reachable via the private.googleapis.com VIP.
    allocation = batch_v1.AllocationPolicy(
        instances=[
            batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=policy)
        ],
        service_account=batch_v1.ServiceAccount(email=service_account_email),
        network=batch_v1.AllocationPolicy.NetworkPolicy(
            network_interfaces=[
                batch_v1.AllocationPolicy.NetworkInterface(
                    network=f"projects/{cfg.project}/global/networks/default",
                    subnetwork=(
                        f"projects/{cfg.project}/regions/{cfg.region}"
                        "/subnetworks/default"
                    ),
                    no_external_ip_address=True,
                )
            ]
        ),
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
