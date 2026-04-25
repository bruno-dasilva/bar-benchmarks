from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from bar_benchmarks.orchestrator import lookup
from bar_benchmarks.stats import aggregate
from bar_benchmarks.types import ArtifactNames, Result, RunnerVerdict


def _result(job_uid: str, vm_id: str, *, valid: bool = True) -> Result:
    return Result(
        batch_id=job_uid,
        vm_id=vm_id,
        instance_type="c2d-standard-16",
        region="us-central1",
        artifact_names=ArtifactNames(
            engine="recoil-abc",
            bar_content="bar-test-1",
            map="hellas-basin",
        ),
        run=RunnerVerdict(
            started_at=datetime(2026, 4, 20, tzinfo=UTC),
            ended_at=datetime(2026, 4, 20, tzinfo=UTC),
            engine_exit=0 if valid else 1,
        ),
        benchmark={"streams": {"sim": {"mean_ms": 20.0, "stddev_ms": 1.0, "count": 10}}}
        if valid
        else {},
        invalid_reason=None if valid else "engine_crash",
    )


class _Blob:
    def __init__(self, name: str, body: bytes | None):
        self.name = name
        self._body = body

    def download_as_bytes(self) -> bytes:
        if self._body is None:
            raise FileNotFoundError(self.name)
        return self._body


class _Page:
    def __init__(self, prefixes):
        self.prefixes = prefixes


class _ListBlobsResult:
    def __init__(self, blobs, prefixes):
        self._blobs = blobs
        self.prefixes = list(prefixes)
        self.pages = [_Page(self.prefixes)]

    def __iter__(self):
        return iter(self._blobs)


class _Bucket:
    def __init__(self, name: str, blobs: dict[str, bytes]):
        self.name = name
        self._blobs = blobs

    def blob(self, name: str) -> _Blob:
        return _Blob(name, self._blobs.get(name))


class _FakeClient:
    """In-memory stand-in for google.cloud.storage.Client."""

    def __init__(self, blobs: dict[str, bytes]):
        # blobs keyed by their full path under the bucket.
        self._blobs = blobs

    def bucket(self, name: str) -> _Bucket:
        return _Bucket(name, self._blobs)

    def list_blobs(self, bucket, *, prefix: str = "", delimiter: str | None = None):
        if delimiter == "/":
            # Top-level "directory" listing — emulate prefixes-only behavior.
            prefixes = set()
            for path in self._blobs:
                if not path.startswith(prefix):
                    continue
                rest = path[len(prefix):]
                if delimiter in rest:
                    prefixes.add(prefix + rest.split(delimiter, 1)[0] + delimiter)
            return _ListBlobsResult([], prefixes)
        # Flat listing under a prefix.
        matched = [
            _Blob(path, body)
            for path, body in self._blobs.items()
            if path.startswith(prefix)
        ]
        return _ListBlobsResult(matched, [])


def _seed_job(blobs: dict[str, bytes], job_uid: str, *, run_meta: dict, results: list[Result]):
    blobs[f"{job_uid}/run.json"] = json.dumps(run_meta).encode()
    for i, r in enumerate(results):
        blobs[f"{job_uid}/{i}/results.json"] = r.model_dump_json().encode()


def _meta(*, count: int = 5, machine_type: str = "c2d-standard-16", **overrides) -> dict:
    base = {
        "job_uid": "ignored",
        "engine": "recoil-abc",
        "bar_content": "bar-test-1",
        "map": "hellas-basin",
        "scenario": "lategame1",
        "machine_type": machine_type,
        "count": count,
        "iterations": 1,
    }
    base.update(overrides)
    return base


def _kwargs(**overrides) -> dict:
    base = dict(
        results_bucket="gs://test",
        engine="recoil-abc",
        bar_content="bar-test-1",
        map_="hellas-basin",
        scenario="lategame1",
        machine_type="c2d-standard-16",
    )
    base.update(overrides)
    return base


def test_from_window_pools_matching_jobs_in_recency_order():
    blobs: dict[str, bytes] = {}
    # Three matching jobs of size 5 each → 15 valid results.
    for i, ts in enumerate([1000, 2000, 3000]):
        _seed_job(
            blobs,
            f"bar-bench-{ts}-aaa",
            run_meta=_meta(count=5),
            results=[_result(f"bar-bench-{ts}-aaa", f"vm{i}-{j}") for j in range(5)],
        )
    client = _FakeClient(blobs)
    report, contributing = aggregate.from_window(
        **_kwargs(), client=client, scan_limit=100
    )
    assert report.valid == 15
    assert report.submitted == 15  # 3 jobs × count=5
    # Most recent first.
    assert contributing == ["bar-bench-3000-aaa", "bar-bench-2000-aaa", "bar-bench-1000-aaa"]
    assert report.job_uid == "bar-bench-3000-aaa"


def test_from_window_filters_on_shape_fields():
    blobs: dict[str, bytes] = {}
    # Matching.
    _seed_job(
        blobs,
        "bar-bench-3000-aaa",
        run_meta=_meta(count=4),
        results=[_result("bar-bench-3000-aaa", f"vm-{j}") for j in range(4)],
    )
    # Different machine_type → must be excluded.
    _seed_job(
        blobs,
        "bar-bench-2000-bbb",
        run_meta=_meta(count=4, machine_type="n1-standard-8"),
        results=[_result("bar-bench-2000-bbb", f"vm-{j}") for j in range(4)],
    )
    # Different bar_content → excluded.
    _seed_job(
        blobs,
        "bar-bench-1000-ccc",
        run_meta=_meta(count=4, bar_content="bar-test-OTHER"),
        results=[_result("bar-bench-1000-ccc", f"vm-{j}") for j in range(4)],
    )
    client = _FakeClient(blobs)
    report, contributing = aggregate.from_window(**_kwargs(), client=client)
    assert contributing == ["bar-bench-3000-aaa"]
    assert report.valid == 4


def test_from_window_ignores_count_and_iterations_in_match():
    blobs: dict[str, bytes] = {}
    # Different count + iterations both still feed the pool.
    _seed_job(
        blobs,
        "bar-bench-2000-aaa",
        run_meta=_meta(count=3, iterations=1),
        results=[_result("bar-bench-2000-aaa", f"vm-{j}") for j in range(3)],
    )
    _seed_job(
        blobs,
        "bar-bench-1000-bbb",
        run_meta=_meta(count=2, iterations=2),
        results=[_result("bar-bench-1000-bbb", f"vm-{j}") for j in range(4)],
    )
    client = _FakeClient(blobs)
    report, contributing = aggregate.from_window(**_kwargs(), client=client)
    assert sorted(contributing) == ["bar-bench-1000-bbb", "bar-bench-2000-aaa"]
    assert report.valid == 7
    # submitted = 3*1 + 2*2 = 7
    assert report.submitted == 7


def test_from_window_respects_scan_limit():
    blobs: dict[str, bytes] = {}
    for ts in range(1, 11):  # 10 matching jobs
        _seed_job(
            blobs,
            f"bar-bench-{ts:04d}-aaa",
            run_meta=_meta(count=2),
            results=[_result(f"bar-bench-{ts:04d}-aaa", f"vm-{j}") for j in range(2)],
        )
    client = _FakeClient(blobs)
    report, contributing = aggregate.from_window(
        **_kwargs(), client=client, scan_limit=3
    )
    assert len(contributing) == 3
    # The three most recent.
    assert contributing == [
        "bar-bench-0010-aaa",
        "bar-bench-0009-aaa",
        "bar-bench-0008-aaa",
    ]
    assert report.valid == 6


def test_from_window_skips_excluded_uid():
    blobs: dict[str, bytes] = {}
    for ts in [1000, 2000]:
        _seed_job(
            blobs,
            f"bar-bench-{ts}-aaa",
            run_meta=_meta(count=3),
            results=[_result(f"bar-bench-{ts}-aaa", f"vm-{j}") for j in range(3)],
        )
    client = _FakeClient(blobs)
    extras = [_result("bar-bench-3000-zzz", f"new-{j}") for j in range(2)]
    report, contributing = aggregate.from_window(
        **_kwargs(),
        client=client,
        extra_results=extras,
        extra_submitted=2,
        exclude_job_uids={"bar-bench-2000-aaa"},
    )
    assert "bar-bench-2000-aaa" not in contributing
    assert "bar-bench-1000-aaa" in contributing
    assert report.valid == 3 + 2  # one historical job + extras
    # synth job_uid = extras' batch_id (the just-completed job)
    assert report.job_uid == "bar-bench-3000-zzz"


def test_from_window_handles_invalid_results_in_pool():
    blobs: dict[str, bytes] = {}
    _seed_job(
        blobs,
        "bar-bench-1000-aaa",
        run_meta=_meta(count=4),
        results=[
            _result("bar-bench-1000-aaa", "vm-0"),
            _result("bar-bench-1000-aaa", "vm-1"),
            _result("bar-bench-1000-aaa", "vm-2", valid=False),
            _result("bar-bench-1000-aaa", "vm-3", valid=False),
        ],
    )
    client = _FakeClient(blobs)
    report, _ = aggregate.from_window(**_kwargs(), client=client)
    assert report.valid == 2
    assert report.invalid == 2
    assert report.invalid_reasons.get("engine_crash") == 2


def test_find_rolling_window_hit_below_threshold():
    blobs: dict[str, bytes] = {}
    _seed_job(
        blobs,
        "bar-bench-1000-aaa",
        run_meta=_meta(count=10),
        results=[_result("bar-bench-1000-aaa", f"vm-{j}") for j in range(10)],
    )
    client = _FakeClient(blobs)
    report, contributing, hit = lookup.find_rolling_window(
        **_kwargs(), client=client, min_samples=50
    )
    assert hit is False
    assert report.valid == 10


def test_find_rolling_window_hit_at_threshold():
    blobs: dict[str, bytes] = {}
    _seed_job(
        blobs,
        "bar-bench-1000-aaa",
        run_meta=_meta(count=50),
        results=[_result("bar-bench-1000-aaa", f"vm-{j}") for j in range(50)],
    )
    client = _FakeClient(blobs)
    report, contributing, hit = lookup.find_rolling_window(
        **_kwargs(), client=client, min_samples=50
    )
    assert hit is True
    assert report.valid == 50


def test_from_window_skips_jobs_with_malformed_run_json():
    blobs: dict[str, bytes] = {
        "bar-bench-2000-aaa/run.json": b"not json {{{",
    }
    _seed_job(
        blobs,
        "bar-bench-1000-bbb",
        run_meta=_meta(count=3),
        results=[_result("bar-bench-1000-bbb", f"vm-{j}") for j in range(3)],
    )
    client = _FakeClient(blobs)
    report, contributing = aggregate.from_window(**_kwargs(), client=client)
    assert contributing == ["bar-bench-1000-bbb"]
    assert report.valid == 3
