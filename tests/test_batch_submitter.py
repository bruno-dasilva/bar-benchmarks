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

    spec = group.task_spec
    assert spec.max_retry_count == 0
    assert spec.max_run_duration.seconds == 1800
    # One task per VM, claiming the 8-vCPU shape with headroom left on
    # the 32-GB memory for Batch overhead.
    assert spec.compute_resource.cpu_milli == 8000
    assert spec.compute_resource.memory_mib == 28 * 1024

    # Volumes: per-job artifacts + whole bucket + results — on the host
    # at /mnt/disks/* (the Batch convention), re-bind-mounted into the
    # containers' canonical /mnt/* paths via CONTAINER_VOLUMES.
    mounts = {v.mount_path: v.gcs.remote_path for v in spec.volumes}
    assert mounts["/mnt/disks/artifacts"] == "bar-experiments-bench-artifacts/job-xyz"
    assert mounts["/mnt/disks/artifacts-bucket"] == "bar-experiments-bench-artifacts"
    assert mounts["/mnt/disks/results"] == "bar-experiments-bench-results/job-xyz"

    # Four container runnables in order: bootstrap, poison (bg+alwaysRun),
    # main, collector (alwaysRun). All share the same image (installed wheel
    # lands in a shared venv under /var/bar-run).
    assert len(spec.runnables) == 4
    boot, poison, main, coll = spec.runnables
    expected_image = "python:3.11-slim"
    expected_python = "python3"
    for r in spec.runnables:
        assert r.container.image_uri == expected_image
        vols = list(r.container.volumes)
        # FUSE mounts remapped to canonical in-container paths.
        assert "/mnt/disks/artifacts:/mnt/artifacts" in vols
        assert "/mnt/disks/artifacts-bucket:/mnt/artifacts-bucket" in vols
        assert "/mnt/disks/results:/mnt/results" in vols
        # Scratch paths live under /mnt/disks/scratch (the attached data disk).
        assert "/mnt/disks/scratch/bar-data:/var/bar-data" in vols
        assert "/mnt/disks/scratch/bar-run:/var/bar-run" in vols
        assert "/mnt/disks/scratch/engine:/opt/recoil" in vols

    assert "pip install" in boot.container.commands[-1]
    assert "/var/bar-run/pypkgs" in boot.container.commands[-1]
    assert poison.background is True
    assert poison.always_run is True
    assert list(poison.container.commands) == [expected_python, "-m", "bar_benchmarks.poison.monitor"]
    assert list(main.container.commands) == [expected_python, "-m", "bar_benchmarks.task.main"]
    assert coll.always_run is True
    assert list(coll.container.commands) == [expected_python, "-m", "bar_benchmarks.task.collector"]

    # Env vars injected.
    env = dict(spec.environment.variables)
    assert env["BAR_ARTIFACTS_DIR"] == "/mnt/artifacts"
    assert env["BAR_ARTIFACTS_BUCKET_DIR"] == "/mnt/artifacts-bucket"
    assert env["BAR_RESULTS_DIR"] == "/mnt/results"
    assert env["BAR_BENCHMARK_OUTPUT_PATH"] == "benchmark-results.json"
    # PYTHONPATH lets the system python3 in each runnable pick up pydantic +
    # the bar_benchmarks wheel installed with `pip install --target` above.
    assert env["PYTHONPATH"] == "/var/bar-run/pypkgs"

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
    assert scratch.device_name == "bar-scratch"
    assert scratch.new_disk.size_gb == 10
    assert scratch.new_disk.type_ == "pd-balanced"
    assert mounts["/mnt/disks/scratch"] == ""  # device-backed volume, no GCS path
    assert (
        job.allocation_policy.service_account.email
        == "benchmark-runner@bar-experiments.iam.gserviceaccount.com"
    )
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


def test_default_min_cpu_platform_table():
    assert batch_submitter.default_min_cpu_platform("n1-standard-8") == "Intel Skylake"
    assert batch_submitter.default_min_cpu_platform("n2d-highmem-4") == "AMD Milan"
    assert batch_submitter.default_min_cpu_platform("c3d-standard-8") is None
    assert batch_submitter.default_min_cpu_platform("e2-medium") is None
