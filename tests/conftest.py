from __future__ import annotations

import json
import tarfile
from pathlib import Path

import pytest


@pytest.fixture
def task_env(monkeypatch, tmp_path):
    """Point all BAR_* paths at a tmp tree and return the layout."""
    artifacts = tmp_path / "mnt-artifacts"
    results = tmp_path / "mnt-results"
    data = tmp_path / "var-bar-data"
    run = tmp_path / "var-bar-run"
    engine = tmp_path / "opt-recoil"
    for d in (artifacts, results, data, run, engine):
        d.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("BAR_ARTIFACTS_DIR", str(artifacts))
    monkeypatch.setenv("BAR_RESULTS_DIR", str(results))
    monkeypatch.setenv("BAR_DATA_DIR", str(data))
    monkeypatch.setenv("BAR_RUN_DIR", str(run))
    monkeypatch.setenv("BAR_ENGINE_DIR", str(engine))
    monkeypatch.setenv("BAR_BENCHMARK_OUTPUT_PATH", "benchmark-results.json")
    monkeypatch.setenv("BATCH_JOB_UID", "job-test")
    monkeypatch.setenv("BATCH_TASK_INDEX", "0")

    return {
        "artifacts": artifacts,
        "results": results,
        "data": data,
        "run": run,
        "engine": engine,
    }


def _make_tarball(out: Path, files: dict[str, str], *, mode: dict[str, int] | None = None) -> None:
    mode = mode or {}
    with tarfile.open(out, "w:gz") as tf:
        for name, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = mode.get(name, 0o644)
            from io import BytesIO

            tf.addfile(info, BytesIO(data))


@pytest.fixture
def tiny_artifacts(task_env):
    """Populate artifacts dir with tiny fixture tarballs + manifest. Engine stub writes benchmark JSON."""
    artifacts = task_env["artifacts"]
    data = task_env["data"]
    map_filename = "tiny.smf"

    stub_script = (
        "#!/bin/sh\n"
        f'echo "{{\\"frames\\": 10, \\"fps\\": 60}}" > "{data}/benchmark-results.json"\n'
        "exit 0\n"
    )
    _make_tarball(
        artifacts / "engine.tar.gz",
        {"spring-headless": stub_script},
        mode={"spring-headless": 0o755},
    )
    _make_tarball(
        artifacts / "bar-content.tar.gz",
        {"VERSION": "1.2.3\n", "shared.lua": "-- base"},
    )
    # Overlay mirrors /var/bar-data/: files under games/BAR.sdd/ override
    # bar-content, and top-level files are bar-data extras.
    _make_tarball(
        artifacts / "overlay.tar.gz",
        {
            "games/BAR.sdd/shared.lua": "-- overlay wins\n",
            "games/BAR.sdd/extra.lua": "-- added",
            "benchmark_snapshot.lua": "-- extra drop at bar-data root",
        },
    )
    (artifacts / map_filename).write_bytes(b"map-bytes")
    (artifacts / "startscript.txt").write_text("[GAME] { ... }\n")

    manifest = {
        "job_uid": "job-test",
        "region": "us-west4",
        "instance_type": "n1-standard-8",
        "map_filename": map_filename,
        "artifact_hashes": {
            "engine": "0" * 64,
            "bar_content": "1" * 64,
            "overlay": "2" * 64,
            "map": "3" * 64,
            "startscript": "4" * 64,
        },
        "wheel_filename": "bar_benchmarks-0.1.0-py3-none-any.whl",
    }
    (artifacts / "manifest.json").write_text(json.dumps(manifest))
    return manifest
