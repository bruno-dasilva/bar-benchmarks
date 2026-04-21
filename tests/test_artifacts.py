from __future__ import annotations

import hashlib
import json
from pathlib import Path

from bar_benchmarks.orchestrator import artifacts
from bar_benchmarks.types import BatchConfig


def _make_cfg(tmp_path: Path) -> tuple[BatchConfig, Path]:
    engine = tmp_path / "engine.tar.gz"
    bar = tmp_path / "bar-content.tar.gz"
    overlay = tmp_path / "overlay.tar.gz"
    m = tmp_path / "Red Comet 2.sd7"
    startscript = tmp_path / "startscript.txt"
    for p, body in [
        (engine, b"E"),
        (bar, b"B"),
        (overlay, b"O"),
        (m, b"M"),
        (startscript, b"S"),
    ]:
        p.write_bytes(body)
    wheel = tmp_path / "bar_benchmarks-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"W")

    cfg = BatchConfig(
        engine=engine,
        bar_content=bar,
        overlay=overlay,
        map=m,
        startscript=startscript,
        count=3,
        project="bar-experiments",
        region="us-west4",
        artifacts_bucket="gs://bar-experiments-bench-artifacts",
        results_bucket="gs://bar-experiments-bench-results",
        machine_type="n1-standard-8",
        max_run_duration_s=1800,
        wheel=wheel,
    )
    return cfg, wheel


def test_sha256_file(tmp_path):
    p = tmp_path / "x"
    p.write_bytes(b"hello")
    assert artifacts.sha256_file(p) == hashlib.sha256(b"hello").hexdigest()


def test_plan_uses_map_filename_verbatim(tmp_path):
    cfg, wheel = _make_cfg(tmp_path)
    plan = artifacts.plan(cfg, "job-xyz", wheel)
    assert plan.map_filename == "Red Comet 2.sd7"
    assert plan.key_prefix == "job-xyz/"
    assert plan.local_paths["Red Comet 2.sd7"] == cfg.map
    assert plan.local_paths["engine.tar.gz"] == cfg.engine
    assert plan.local_paths[wheel.name] == wheel
    assert plan.hashes.engine == hashlib.sha256(b"E").hexdigest()


def test_manifest_bytes_shape(tmp_path):
    cfg, wheel = _make_cfg(tmp_path)
    plan = artifacts.plan(cfg, "job-xyz", wheel)
    manifest = json.loads(artifacts.manifest_bytes(cfg, "job-xyz", plan))
    assert manifest["job_uid"] == "job-xyz"
    assert manifest["region"] == "us-west4"
    assert manifest["instance_type"] == "n1-standard-8"
    assert manifest["map_filename"] == "Red Comet 2.sd7"
    assert manifest["wheel_filename"] == wheel.name
    assert manifest["artifact_hashes"]["engine"] == hashlib.sha256(b"E").hexdigest()


class _FakeBlob:
    def __init__(self, name, store):
        self.name = name
        self._store = store

    def upload_from_filename(self, path):
        self._store[self.name] = Path(path).read_bytes()

    def upload_from_string(self, data, content_type=None):
        self._store[self.name] = data if isinstance(data, bytes) else data.encode()


class _FakeBucket:
    def __init__(self, store):
        self._store = store

    def blob(self, name):
        return _FakeBlob(name, self._store)


class _FakeClient:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def bucket(self, _name):
        return _FakeBucket(self.store)


def test_upload_writes_all_expected_keys(tmp_path):
    cfg, wheel = _make_cfg(tmp_path)
    plan = artifacts.plan(cfg, "job-xyz", wheel)
    manifest = artifacts.manifest_bytes(cfg, "job-xyz", plan)
    client = _FakeClient()

    artifacts.upload("bar-experiments-bench-artifacts", plan, manifest, client=client)

    keys = sorted(client.store.keys())
    assert keys == sorted(
        [
            "job-xyz/engine.tar.gz",
            "job-xyz/bar-content.tar.gz",
            "job-xyz/overlay.tar.gz",
            "job-xyz/Red Comet 2.sd7",
            "job-xyz/startscript.txt",
            f"job-xyz/{wheel.name}",
            "job-xyz/manifest.json",
        ]
    )
    assert client.store["job-xyz/engine.tar.gz"] == b"E"
    assert json.loads(client.store["job-xyz/manifest.json"])["job_uid"] == "job-xyz"
