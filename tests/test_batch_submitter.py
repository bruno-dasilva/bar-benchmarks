from __future__ import annotations

from pathlib import Path

from google.cloud import batch_v1

from bar_benchmarks.orchestrator import batch_submitter
from bar_benchmarks.types import BatchConfig


def _cfg() -> BatchConfig:
    return BatchConfig(
        engine_name="recoil-test",
        bar_content_name="bar-test",
        map_name="tiny",
        scenario_dir=Path("/tmp/lategame1"),
        catalog_path=Path("/tmp/artifacts.toml"),
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
    # Unset — Batch derives K from vCPUs/memory per VM ÷ per-task
    # compute_resource. 0 is the protobuf "field not set" value.
    assert group.task_count_per_node == 0

    spec = group.task_spec
    assert spec.max_retry_count == 0
    assert spec.max_run_duration.seconds == 1800
    # One task per VM, claiming the full 16-vCPU shape with headroom
    # left on the 32-GB memory for Batch overhead.
    assert spec.compute_resource.cpu_milli == batch_submitter.TASK_CPU_MILLI
    assert spec.compute_resource.memory_mib == batch_submitter.TASK_MEMORY_MIB

    # Volumes: per-job artifacts + whole bucket + results — on the host
    # at /mnt/disks/* (the Batch convention), re-bind-mounted into the
    # containers' canonical /mnt/* paths via CONTAINER_VOLUMES.
    mounts = {v.mount_path: v.gcs.remote_path for v in spec.volumes}
    assert mounts["/mnt/disks/artifacts"] == "bar-experiments-bench-artifacts/job-xyz"
    assert mounts["/mnt/disks/artifacts-bucket"] == "bar-experiments-bench-artifacts"
    assert mounts["/mnt/disks/results"] == "bar-experiments-bench-results/job-xyz"

    # Four container runnables in order: bootstrap, poison (bg+alwaysRun),
    # main, collector (alwaysRun). Each task carves out its own
    # /var/bar-scratch/tasks/$BATCH_TASK_INDEX subtree at runtime via the
    # PER_TASK_ENV_WRAPPER, so multiple tasks can share one VM.
    assert len(spec.runnables) == 4
    boot, poison, main, coll = spec.runnables
    expected_image = batch_submitter.CONTAINER_IMAGE
    assert expected_image.startswith(
        "us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:"
    )
    expected_python = "python3"
    wrapper_prefix = ["/bin/sh", "-c", batch_submitter.PER_TASK_ENV_WRAPPER, "--"]
    for r in spec.runnables:
        assert r.container.image_uri == expected_image
        vols = list(r.container.volumes)
        # FUSE mounts remapped to canonical in-container paths.
        assert "/mnt/disks/artifacts:/mnt/artifacts" in vols
        assert "/mnt/disks/artifacts-bucket:/mnt/artifacts-bucket" in vols
        assert "/mnt/disks/results:/mnt/results" in vols
        # Single scratch mount — the data disk surfaces inside the
        # container at /var/bar-scratch, and PER_TASK_ENV_WRAPPER points
        # each task's BAR_*_DIR / PYTHONPATH at tasks/<idx>/ underneath.
        assert "/mnt/disks/scratch:/var/bar-scratch" in vols
        # Every runnable is wrapped so $BATCH_TASK_INDEX resolves before
        # the real command executes.
        assert list(r.container.commands[: len(wrapper_prefix)]) == wrapper_prefix

    # Bootstrap's real command runs under `sh -c <BOOTSTRAP_SCRIPT>`;
    # verify the wheel install is in that script and pydantic is NOT
    # (it's baked into the container image now).
    assert list(boot.container.commands[len(wrapper_prefix):]) == ["/bin/sh", "-c", batch_submitter.BOOTSTRAP_SCRIPT]
    assert "pip install" in batch_submitter.BOOTSTRAP_SCRIPT
    assert '"$root/pypkgs"' in batch_submitter.BOOTSTRAP_SCRIPT
    # pydantic is baked into the container image, so the bootstrap
    # doesn't pip-install it anymore.
    assert "pip install --no-cache-dir --target" in batch_submitter.BOOTSTRAP_SCRIPT
    assert "--target \"$root/pypkgs\" pydantic" not in batch_submitter.BOOTSTRAP_SCRIPT
    assert poison.background is True
    assert poison.always_run is True
    assert list(poison.container.commands[len(wrapper_prefix):]) == [expected_python, "-m", "bar_benchmarks.poison.monitor"]
    assert list(main.container.commands[len(wrapper_prefix):]) == [expected_python, "-m", "bar_benchmarks.task.main"]
    assert coll.always_run is True
    assert list(coll.container.commands[len(wrapper_prefix):]) == [expected_python, "-m", "bar_benchmarks.task.collector"]

    # Wrapper exports every per-task env var the task code reads.
    for var in ("BAR_DATA_DIR", "BAR_RUN_DIR", "BAR_ENGINE_DIR", "PYTHONPATH"):
        assert f"export {var}=" in batch_submitter.PER_TASK_ENV_WRAPPER
    assert "BATCH_TASK_INDEX" in batch_submitter.PER_TASK_ENV_WRAPPER

    # Static env: only the invariant-across-tasks paths + benchmark
    # output filename. Per-task dirs are set by the wrapper.
    env = dict(spec.environment.variables)
    assert env == {
        "BAR_ARTIFACTS_DIR": "/mnt/artifacts",
        "BAR_ARTIFACTS_BUCKET_DIR": "/mnt/artifacts-bucket",
        "BAR_RESULTS_DIR": "/mnt/results",
        "BAR_BENCHMARK_OUTPUT_PATH": "benchmark-results.json",
    }

    # Allocation policy.
    assert job.allocation_policy.instances[0].install_ops_agent is True
    inst = job.allocation_policy.instances[0].policy
    assert inst.machine_type == "n1-standard-8"
    # n1 is a multi-gen family; table pins it to Skylake for reproducibility.
    assert inst.min_cpu_platform == "Intel Skylake"
    assert inst.provisioning_model == batch_v1.AllocationPolicy.ProvisioningModel.SPOT
    assert inst.boot_disk.size_gb == 30
    assert inst.boot_disk.type_ == "pd-balanced"
    # Dedicated 10 GB scratch disk mounted at /mnt/disks/scratch; all
    # per-VM scratch paths (bar-data, bar-run, engine extract) live here.
    assert len(inst.disks) == 1
    scratch = inst.disks[0]
    assert scratch.device_name == batch_submitter.SCRATCH_DEVICE_NAME
    assert scratch.new_disk.size_gb == batch_submitter.SCRATCH_DISK_GB
    assert scratch.new_disk.type_ == batch_submitter.SCRATCH_DISK_TYPE
    assert mounts["/mnt/disks/scratch"] == ""  # device-backed volume, no GCS path
    assert (
        job.allocation_policy.service_account.email
        == "benchmark-runner@bar-experiments.iam.gserviceaccount.com"
    )
    # Private-IP-only by default: every VM pinned to the project's
    # `default` VPC + subnet with no external IP. PGA on the subnet
    # keeps GCS / Logging / AR reachable.
    ifaces = list(job.allocation_policy.network.network_interfaces)
    assert len(ifaces) == 1
    assert ifaces[0].network == "projects/bar-experiments/global/networks/default"
    assert (
        ifaces[0].subnetwork
        == "projects/bar-experiments/regions/us-west4/subnetworks/default"
    )
    assert ifaces[0].no_external_ip_address is True
    assert job.logs_policy.destination == batch_v1.LogsPolicy.Destination.CLOUD_LOGGING


def test_service_account_override():
    cfg = _cfg().model_copy(update={"service_account": "custom@other.iam.gserviceaccount.com"})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    assert job.allocation_policy.service_account.email == "custom@other.iam.gserviceaccount.com"


def test_service_account_derives_from_project():
    cfg = _cfg().model_copy(update={"project": "some-other-project"})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    assert (
        job.allocation_policy.service_account.email
        == "benchmark-runner@some-other-project.iam.gserviceaccount.com"
    )


def test_min_cpu_platform_override():
    cfg = _cfg().model_copy(update={"min_cpu_platform": "Intel Ice Lake"})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    inst = job.allocation_policy.instances[0].policy
    assert inst.min_cpu_platform == "Intel Ice Lake"


def test_min_cpu_platform_auto_for_single_gen_family():
    # c3d is AMD Genoa single-gen — the table omits it, so no pin.
    cfg = _cfg().model_copy(update={"machine_type": "c3d-standard-8"})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    inst = job.allocation_policy.instances[0].policy
    assert inst.machine_type == "c3d-standard-8"
    assert inst.min_cpu_platform == ""


def test_min_cpu_platform_empty_override_forces_unset():
    # Opt out of the table default for an n1 run.
    cfg = _cfg().model_copy(update={"min_cpu_platform": ""})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    inst = job.allocation_policy.instances[0].policy
    assert inst.min_cpu_platform == ""


def test_task_count_per_node_explicit_cap():
    # Setting the value overrides Batch's auto-derive. Caller is
    # responsible for ensuring K * compute_resource fits the VM.
    cfg = _cfg().model_copy(update={"task_count_per_node": 2})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    assert job.task_groups[0].task_count_per_node == 2


def test_network_policy_override():
    cfg = _cfg().model_copy(update={"network": "bar-vpc", "subnetwork": "bar-subnet"})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    ifaces = list(job.allocation_policy.network.network_interfaces)
    assert len(ifaces) == 1
    assert ifaces[0].network == "projects/bar-experiments/global/networks/bar-vpc"
    assert (
        ifaces[0].subnetwork
        == "projects/bar-experiments/regions/us-west4/subnetworks/bar-subnet"
    )
    assert ifaces[0].no_external_ip_address is True


def test_network_policy_omitted_when_network_none():
    cfg = _cfg().model_copy(update={"network": None, "subnetwork": None})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    assert len(job.allocation_policy.network.network_interfaces) == 0


def test_network_policy_respects_no_external_ip_false():
    cfg = _cfg().model_copy(
        update={"network": "bar-vpc", "subnetwork": "bar-subnet", "no_external_ip": False}
    )
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    iface = job.allocation_policy.network.network_interfaces[0]
    assert iface.no_external_ip_address is False


def test_container_image_override():
    override = "us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:pinned-abc"
    cfg = _cfg().model_copy(update={"container_image": override})
    job = batch_submitter.build_job(cfg, job_uid="job-xyz")
    for r in job.task_groups[0].task_spec.runnables:
        assert r.container.image_uri == override


def test_default_min_cpu_platform_table():
    assert batch_submitter.default_min_cpu_platform("n1-standard-8") == "Intel Skylake"
    assert batch_submitter.default_min_cpu_platform("n2d-highmem-4") == "AMD Milan"
    assert batch_submitter.default_min_cpu_platform("c3d-standard-8") is None
    assert batch_submitter.default_min_cpu_platform("e2-medium") is None
