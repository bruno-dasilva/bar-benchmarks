from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from bar_benchmarks.orchestrator import run_info
from bar_benchmarks.types import BatchConfig


def _cfg(tmp_path: Path, **overrides) -> BatchConfig:
    scenario = tmp_path / "lategame1"
    scenario.mkdir()
    defaults = dict(
        engine_name="recoil-5c157c8-perf-wins",
        bar_content_name="bar-test-29871-90f4bc1",
        map_name="hellas-basin-v1.4",
        scenario_dir=scenario,
        run_description=None,
        catalog_path=tmp_path / "artifacts.toml",
        count=20,
        project="bar-experiments",
        region="us-central1",
        artifacts_bucket="gs://bar-experiments-bench-artifacts",
        results_bucket="gs://bar-experiments-bench-results",
        machine_type="n1-standard-8",
        min_cpu_platform="Intel Skylake",
        max_run_duration_s=1800,
        service_account="benchmark-runner@bar-experiments.iam.gserviceaccount.com",
    )
    defaults.update(overrides)
    return BatchConfig(**defaults)


def test_run_info_bytes_includes_description_and_params(tmp_path):
    cfg = _cfg(tmp_path, run_description="Testing n1 vs n2 on lategame1.")
    submitted_at = datetime(2026, 4, 21, 15, 30, 0, tzinfo=UTC)

    body = json.loads(run_info.run_info_bytes(cfg, "job-xyz", submitted_at))

    assert body == {
        "job_uid": "job-xyz",
        "submitted_at": "2026-04-21T15:30:00+00:00",
        "run_description": "Testing n1 vs n2 on lategame1.",
        "engine": "recoil-5c157c8-perf-wins",
        "bar_content": "bar-test-29871-90f4bc1",
        "map": "hellas-basin-v1.4",
        "scenario": "lategame1",
        "count": 20,
        "region": "us-central1",
        "machine_type": "n1-standard-8",
        "min_cpu_platform": "Intel Skylake",
        "max_run_duration_s": 1800,
    }


def test_run_info_bytes_omits_infrastructure_and_local_paths(tmp_path):
    cfg = _cfg(tmp_path)
    body = json.loads(run_info.run_info_bytes(cfg, "job-xyz", datetime.now(UTC)))

    for excluded in (
        "project",
        "artifacts_bucket",
        "results_bucket",
        "service_account",
        "catalog_path",
        "wheel",
        "scenario_dir",
    ):
        assert excluded not in body


def test_run_info_bytes_null_description(tmp_path):
    cfg = _cfg(tmp_path, run_description=None)
    body = json.loads(run_info.run_info_bytes(cfg, "job-xyz", datetime.now(UTC)))
    assert body["run_description"] is None


class _FakeBlob:
    def __init__(self, name, store):
        self.name = name
        self._store = store

    def upload_from_string(self, data, content_type=None):
        self._store[self.name] = {
            "data": data if isinstance(data, bytes) else data.encode(),
            "content_type": content_type,
        }


class _FakeBucket:
    def __init__(self, name, store):
        self.name = name
        self._store = store

    def blob(self, name):
        return _FakeBlob(name, self._store)


class _FakeClient:
    def __init__(self):
        self.store: dict[str, dict] = {}
        self.last_bucket: str | None = None

    def bucket(self, name):
        self.last_bucket = name
        return _FakeBucket(name, self.store)


def test_upload_writes_run_json_with_json_content_type():
    client = _FakeClient()
    body = b'{"job_uid":"job-xyz"}'

    run_info.upload(
        "gs://bar-experiments-bench-results",
        "job-xyz",
        body,
        client=client,
    )

    assert client.last_bucket == "bar-experiments-bench-results"
    assert "job-xyz/run.json" in client.store
    written = client.store["job-xyz/run.json"]
    assert written["data"] == body
    assert written["content_type"] == "application/json"


def test_upload_strips_gs_prefix():
    client = _FakeClient()
    run_info.upload("bar-experiments-bench-results", "job-xyz", b"{}", client=client)
    assert client.last_bucket == "bar-experiments-bench-results"
