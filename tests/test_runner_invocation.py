from __future__ import annotations

import json

from bar_benchmarks.task import runner


def test_runner_happy_path(task_env, tiny_artifacts):
    verdicts = runner.run()
    assert len(verdicts) == 1
    verdict = verdicts[0]
    assert verdict.engine_exit == 0
    assert verdict.error is None
    assert verdict.engine_wall_s is not None and verdict.engine_wall_s >= 0

    # Benchmark output was written by the stub, then moved into iter-0/.
    bench = json.loads((task_env["run"] / "iter-0" / "benchmark.json").read_text())
    assert bench == {"frames": 10, "fps": 60}
    assert not (task_env["data"] / "benchmark-results.json").exists()

    on_disk = json.loads((task_env["run"] / "iter-0" / "verdict.json").read_text())
    assert on_disk["engine_exit"] == 0
    assert on_disk["error"] is None


def _replace_engine(task_env, tiny_artifacts, stub: bytes) -> None:
    import tarfile
    from io import BytesIO

    engine_tar = task_env["bucket"] / tiny_artifacts["paths"]["engine"]
    engine_tar.unlink()
    with tarfile.open(engine_tar, "w:gz") as tf:
        info = tarfile.TarInfo(name="spring-headless")
        info.size = len(stub)
        info.mode = 0o755
        tf.addfile(info, BytesIO(stub))


def test_runner_overlay_output_missing(task_env, tiny_artifacts):
    # Replace engine with a stub that exits 0 but writes no benchmark file.
    _replace_engine(task_env, tiny_artifacts, b"#!/bin/sh\nexit 0\n")

    verdicts = runner.run()
    assert len(verdicts) == 1
    assert verdicts[0].engine_exit == 0
    assert verdicts[0].error == "overlay_output_missing"


def test_runner_engine_crash(task_env, tiny_artifacts):
    _replace_engine(task_env, tiny_artifacts, b"#!/bin/sh\nexit 42\n")

    verdicts = runner.run()
    assert len(verdicts) == 1
    assert verdicts[0].engine_exit == 42
    assert verdicts[0].error == "engine_crash"


def test_runner_loops_iterations(task_env, tiny_artifacts):
    manifest_path = task_env["artifacts"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["iterations"] = 3
    manifest_path.write_text(json.dumps(manifest))

    verdicts = runner.run()
    assert len(verdicts) == 3
    for v in verdicts:
        assert v.engine_exit == 0
        assert v.error is None

    for i in range(3):
        iter_dir = task_env["run"] / f"iter-{i}"
        assert (iter_dir / "verdict.json").is_file()
        bench = json.loads((iter_dir / "benchmark.json").read_text())
        assert bench == {"frames": 10, "fps": 60}

    # Each iter should have moved the engine's output — nothing leaks back
    # into the write-dir.
    assert not (task_env["data"] / "benchmark-results.json").exists()
