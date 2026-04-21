from __future__ import annotations

from pathlib import Path

from bar_benchmarks import paths


def test_defaults(monkeypatch):
    for var in [
        "BAR_ARTIFACTS_DIR",
        "BAR_RESULTS_DIR",
        "BAR_DATA_DIR",
        "BAR_RUN_DIR",
        "BAR_ENGINE_DIR",
        "BAR_BENCHMARK_OUTPUT_PATH",
        "BATCH_JOB_UID",
        "BATCH_TASK_INDEX",
    ]:
        monkeypatch.delenv(var, raising=False)

    assert paths.artifacts_dir() == Path("/mnt/artifacts")
    assert paths.results_dir() == Path("/mnt/results")
    assert paths.data_dir() == Path("/var/bar-data")
    assert paths.run_dir() == Path("/var/bar-run")
    assert paths.engine_dir() == Path("/opt/recoil")
    assert paths.benchmark_output_path() == Path("/var/bar-data/benchmark-results.json")
    assert paths.batch_job_uid() is None
    assert paths.batch_task_index() == "0"


def test_env_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("BAR_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("BAR_BENCHMARK_OUTPUT_PATH", "sub/out.json")
    monkeypatch.setenv("BATCH_JOB_UID", "job-abc")
    monkeypatch.setenv("BATCH_TASK_INDEX", "7")

    assert paths.data_dir() == tmp_path / "data"
    assert paths.benchmark_output_path() == tmp_path / "data" / "sub/out.json"
    assert paths.batch_job_uid() == "job-abc"
    assert paths.batch_task_index() == "7"
