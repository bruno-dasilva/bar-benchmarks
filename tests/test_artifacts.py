from __future__ import annotations

import json
from pathlib import Path

from bar_benchmarks.orchestrator import artifacts
from bar_benchmarks.orchestrator.catalog import Catalog
from bar_benchmarks.types import BatchConfig

CATALOG_TOML = """
[engine.recoil-test]
dest = "gs://bar-experiments-bench-artifacts/engine/recoil-test.tar.gz"
commit = "abc1234"

[bar_content.bar-test]
dest = "gs://bar-experiments-bench-artifacts/bar-content/bar-test.tar.gz"
version = "Beyond All Reason test-1-abc1234"

[map."tiny-v1"]
dest = "gs://bar-experiments-bench-artifacts/maps/tiny.sd7"
"""


def _make_cfg(tmp_path: Path) -> tuple[BatchConfig, Path, Path, Path]:
    catalog = tmp_path / "artifacts.toml"
    catalog.write_text(CATALOG_TOML)

    scenario = tmp_path / "scenario"
    scenario.mkdir()
    (scenario / "startscript.txt").write_text("[GAME] {}\n")

    overlay = tmp_path / "overlay.tar.gz"
    overlay.write_bytes(b"O")
    wheel = tmp_path / "bar_benchmarks-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"W")

    cfg = BatchConfig(
        engine_name="recoil-test",
        bar_content_name="bar-test",
        map_name="tiny-v1",
        scenario_dir=scenario,
        catalog_path=catalog,
        count=3,
        project="bar-experiments",
        region="us-west4",
        artifacts_bucket="gs://bar-experiments-bench-artifacts",
        results_bucket="gs://bar-experiments-bench-results",
        machine_type="n1-standard-8",
        max_run_duration_s=1800,
        wheel=wheel,
    )
    return cfg, overlay, wheel, catalog


class _FakeBlob:
    def __init__(self, name, store, upload_counts):
        self.name = name
        self._store = store
        self._upload_counts = upload_counts

    def exists(self):
        return self.name in self._store

    def upload_from_filename(self, path):
        self._upload_counts[self.name] = self._upload_counts.get(self.name, 0) + 1
        self._store[self.name] = Path(path).read_bytes()

    def upload_from_string(self, data, content_type=None):
        self._upload_counts[self.name] = self._upload_counts.get(self.name, 0) + 1
        self._store[self.name] = data if isinstance(data, bytes) else data.encode()


class _FakeBucket:
    def __init__(self, store, upload_counts):
        self._store = store
        self._upload_counts = upload_counts

    def blob(self, name):
        return _FakeBlob(name, self._store, self._upload_counts)


class _FakeClient:
    def __init__(self, store: dict[str, bytes] | None = None):
        self.store: dict[str, bytes] = store if store is not None else {}
        self.upload_counts: dict[str, int] = {}

    def bucket(self, _name):
        return _FakeBucket(self.store, self.upload_counts)


def _stub_builders(monkeypatch, tmp_path):
    """Replace build helpers with stubs that emit tiny local tarballs."""
    def fake_engine(spec, out_dir):
        p = out_dir / f"{spec.name}.tar.gz"
        p.write_bytes(b"engine-bytes")
        return p

    def fake_bar(spec, out_dir):
        p = out_dir / f"{spec.name}.tar.gz"
        p.write_bytes(b"bar-bytes")
        return p

    def fake_map(spec, out_dir):
        p = out_dir / Path(spec.dest_uri).name
        p.write_bytes(b"map-bytes")
        return p

    monkeypatch.setattr(artifacts, "build_engine", fake_engine)
    monkeypatch.setattr(artifacts, "build_bar_content", fake_bar)
    monkeypatch.setattr(artifacts, "fetch_map", fake_map)
    scratch = tmp_path / "build-scratch"
    scratch.mkdir(exist_ok=True)
    monkeypatch.setattr(artifacts, "_workdir", lambda: scratch)


def test_build_and_upload_cache_miss_runs_builder(tmp_path, monkeypatch):
    _stub_builders(monkeypatch, tmp_path)
    cfg, overlay, wheel, catalog = _make_cfg(tmp_path)
    cat = Catalog.load(catalog)
    client = _FakeClient()

    artifacts.build_and_upload(cfg, "job-xyz", cat=cat, overlay=overlay, wheel=wheel, client=client)

    assert sorted(client.store.keys()) == sorted([
        "engine/recoil-test.tar.gz",
        "bar-content/bar-test.tar.gz",
        "maps/tiny.sd7",
        "job-xyz/overlay.tar.gz",
        "job-xyz/startscript.txt",
        f"job-xyz/{wheel.name}",
        "job-xyz/manifest.json",
    ])
    assert client.store["engine/recoil-test.tar.gz"] == b"engine-bytes"
    assert client.store["bar-content/bar-test.tar.gz"] == b"bar-bytes"
    assert client.store["maps/tiny.sd7"] == b"map-bytes"

    manifest = json.loads(client.store["job-xyz/manifest.json"])
    assert manifest["job_uid"] == "job-xyz"
    assert manifest["region"] == "us-west4"
    assert manifest["instance_type"] == "n1-standard-8"
    assert manifest["iterations"] == 1
    assert manifest["map_filename"] == "tiny.sd7"
    assert manifest["artifact_names"] == {
        "engine": "recoil-test",
        "bar_content": "bar-test",
        "map": "tiny-v1",
    }
    assert manifest["paths"] == {
        "engine": "engine/recoil-test.tar.gz",
        "bar_content": "bar-content/bar-test.tar.gz",
        "map": "maps/tiny.sd7",
    }


def test_build_and_upload_cache_hit_skips_builder(tmp_path, monkeypatch):
    def fail(*_args, **_kw):
        raise AssertionError("builder should not run on cache hit")

    monkeypatch.setattr(artifacts, "build_engine", fail)
    monkeypatch.setattr(artifacts, "build_bar_content", fail)
    monkeypatch.setattr(artifacts, "fetch_map", fail)
    scratch = tmp_path / "build-scratch"
    scratch.mkdir(exist_ok=True)
    monkeypatch.setattr(artifacts, "_workdir", lambda: scratch)

    cfg, overlay, wheel, catalog = _make_cfg(tmp_path)
    cat = Catalog.load(catalog)
    pre = {
        "engine/recoil-test.tar.gz": b"cached-engine",
        "bar-content/bar-test.tar.gz": b"cached-bar",
        "maps/tiny.sd7": b"cached-map",
    }
    client = _FakeClient(store=dict(pre))

    events: list[tuple[str, bool]] = []
    artifacts.build_and_upload(
        cfg,
        "job-xyz",
        cat=cat,
        overlay=overlay,
        wheel=wheel,
        client=client,
        on_upload=lambda uri, cached: events.append((uri, cached)),
    )

    for key, body in pre.items():
        assert client.store[key] == body
        assert client.upload_counts.get(key, 0) == 0
    for key in ("job-xyz/overlay.tar.gz", "job-xyz/startscript.txt", "job-xyz/manifest.json"):
        assert client.upload_counts.get(key, 0) == 1
    shared_events = [
        (uri, cached)
        for uri, cached in events
        if "/engine/" in uri or "/bar-content/" in uri or "/maps/" in uri
    ]
    assert all(cached for _, cached in shared_events)
    assert len(shared_events) == 3
