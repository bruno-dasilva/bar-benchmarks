from __future__ import annotations

import json
from datetime import UTC, datetime

from bar_benchmarks.task import collector
from bar_benchmarks.types import RunnerVerdict


def _write_inputs(task_env, *, error=None, has_bench=True, iter_index: int = 0):
    """Write what the runner would have left behind for one iter."""
    iter_dir = task_env["run"] / f"iter-{iter_index}"
    iter_dir.mkdir(parents=True, exist_ok=True)

    verdict = RunnerVerdict(
        started_at=datetime(2026, 4, 20, tzinfo=UTC),
        ended_at=datetime(2026, 4, 20, 0, 0, 30, tzinfo=UTC),
        engine_exit=0 if error is None else 1,
        engine_wall_s=30.0,
        error=error,
    )
    (iter_dir / "verdict.json").write_text(json.dumps(verdict.model_dump(mode="json")))
    if has_bench:
        (iter_dir / "benchmark.json").write_text(json.dumps({"frames": 10, "fps": 60}))
    return iter_dir


def test_collector_happy_path(task_env, tiny_artifacts):
    _write_inputs(task_env)
    results = collector.run()
    assert len(results) == 1
    result = results[0]

    out = task_env["results"] / "0" / "0" / "results.json"
    assert out.is_file()
    on_disk = json.loads(out.read_text())
    assert result.valid is True
    assert result.invalid_reason is None
    assert result.vm_id == "0-0"
    assert on_disk["benchmark"] == {"frames": 10, "fps": 60}
    assert on_disk["batch_id"] == "job-test"
    assert on_disk["instance_type"] == "n1-standard-8"
    assert on_disk["artifact_names"]["engine"] == "recoil-test"


def test_collector_engine_crash_marks_invalid(task_env, tiny_artifacts):
    _write_inputs(task_env, error="engine_crash")
    results = collector.run()
    assert len(results) == 1
    assert results[0].valid is False
    assert results[0].invalid_reason == "engine_crash"


def test_collector_uploads_infolog_when_present(task_env, tiny_artifacts):
    iter_dir = _write_inputs(task_env)
    (iter_dir / "infolog.txt").write_text("engine log contents\n")

    collector.run()

    uploaded = task_env["results"] / "0" / "0" / "infolog.txt"
    assert uploaded.is_file()
    assert uploaded.read_text() == "engine log contents\n"


def test_collector_skips_infolog_when_absent(task_env, tiny_artifacts):
    _write_inputs(task_env)

    collector.run()

    assert not (task_env["results"] / "0" / "0" / "infolog.txt").exists()


def test_collector_missing_verdict(task_env, tiny_artifacts):
    # No iter dirs at all, but manifest.iterations defaults to 1 → one
    # placeholder result for iter 0.
    results = collector.run()
    assert len(results) == 1
    assert results[0].valid is False
    assert results[0].invalid_reason == "runner_did_not_run"
    assert (task_env["results"] / "0" / "0" / "results.json").is_file()


def test_collector_multi_iter(task_env, tiny_artifacts):
    # Manifest claims 3 iterations; runner left 2 complete + 1 missing.
    manifest_path = task_env["artifacts"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["iterations"] = 3
    manifest_path.write_text(json.dumps(manifest))

    _write_inputs(task_env, iter_index=0)
    _write_inputs(task_env, iter_index=1, error="engine_crash")

    results = collector.run()
    assert len(results) == 3

    assert results[0].valid is True
    assert results[0].vm_id == "0-0"
    assert results[1].valid is False
    assert results[1].invalid_reason == "engine_crash"
    assert results[2].valid is False
    assert results[2].invalid_reason == "runner_did_not_run"

    for i in range(3):
        assert (task_env["results"] / "0" / str(i) / "results.json").is_file()
