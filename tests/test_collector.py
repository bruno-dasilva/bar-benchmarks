from __future__ import annotations

import json
from datetime import UTC, datetime

from bar_benchmarks.task import collector
from bar_benchmarks.types import PoisonSummary, PreflightResult, RunnerVerdict


def _write_inputs(task_env, *, preflight_ok=True, error=None, tripped=False, has_bench=True):
    run = task_env["run"]
    data = task_env["data"]

    (run / "preflight.json").write_text(
        json.dumps(PreflightResult(passed=preflight_ok).model_dump())
    )
    verdict = RunnerVerdict(
        started_at=datetime(2026, 4, 20, tzinfo=UTC),
        ended_at=datetime(2026, 4, 20, 0, 0, 30, tzinfo=UTC),
        engine_exit=0 if error is None else 1,
        timings={"engine_wall_s": 30.0},
        benchmark_output_path=str(data / "benchmark-results.json"),
        error=error,
    )
    (run / "verdict.json").write_text(json.dumps(verdict.model_dump(mode="json")))
    (run / "poison.json").write_text(json.dumps(PoisonSummary(tripped=tripped).model_dump()))
    if has_bench:
        (data / "benchmark-results.json").write_text(json.dumps({"frames": 10, "fps": 60}))


def test_collector_happy_path(task_env, tiny_artifacts):
    _write_inputs(task_env)
    result = collector.run()

    out = task_env["results"] / "0" / "results.json"
    assert out.is_file()
    on_disk = json.loads(out.read_text())
    assert result.valid is True
    assert result.invalid_reason is None
    assert on_disk["benchmark"] == {"frames": 10, "fps": 60}
    assert on_disk["batch_id"] == "job-test"
    assert on_disk["instance_type"] == "n1-standard-8"
    assert on_disk["artifact_names"]["engine"] == "recoil-test"


def test_collector_preflight_failed(task_env, tiny_artifacts):
    _write_inputs(task_env, preflight_ok=False)
    result = collector.run()
    assert result.valid is False
    assert result.invalid_reason == "preflight_failed"


def test_collector_poisoned_overrides_engine_crash(task_env, tiny_artifacts):
    _write_inputs(task_env, error="engine_crash", tripped=True)
    result = collector.run()
    assert result.valid is False
    assert result.invalid_reason == "poisoned"


def test_collector_uploads_infolog_when_present(task_env, tiny_artifacts):
    _write_inputs(task_env)
    (task_env["data"] / "infolog.txt").write_text("engine log contents\n")

    collector.run()

    uploaded = task_env["results"] / "0" / "infolog.txt"
    assert uploaded.is_file()
    assert uploaded.read_text() == "engine log contents\n"


def test_collector_skips_infolog_when_absent(task_env, tiny_artifacts):
    _write_inputs(task_env)
    # No infolog written (e.g., engine never started).

    collector.run()

    assert not (task_env["results"] / "0" / "infolog.txt").exists()


def test_collector_missing_verdict(task_env, tiny_artifacts):
    # Simulate: runner never wrote verdict.json (e.g., preflight skipped it).
    (task_env["run"] / "preflight.json").write_text(
        json.dumps(PreflightResult(passed=True).model_dump())
    )
    (task_env["run"] / "poison.json").write_text(
        json.dumps(PoisonSummary(tripped=False).model_dump())
    )
    result = collector.run()
    assert result.valid is False
    assert result.invalid_reason == "runner_did_not_run"
