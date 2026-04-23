"""Ensure the shared + per-job artifacts are in the bucket, then upload the manifest.

Bucket layout:

    gs://<artifacts-bucket>/
        engine/<name>.tar.gz                     (shared across jobs)
        bar-content/<name>.tar.gz                (shared across jobs)
        maps/<map_filename>                      (shared across jobs)
        <job_uid>/
            overlay.tar.gz
            startscript.txt
            bar_benchmarks-<ver>-py3-none-any.whl
            manifest.json

Shared keys are derived from the catalog name (not content hash) so we
can decide whether to skip a build before we have the tarball on disk.
On cache miss, we shell out to scripts/build-*.sh to materialize the
tarball locally, then upload.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

from bar_benchmarks.orchestrator.catalog import (
    BarContentSpec,
    Catalog,
    EngineSpec,
    MapSpec,
    key_from_uri,
)
from bar_benchmarks.types import ArtifactNames, BatchConfig


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("could not locate project root (no pyproject.toml on ancestor path)")


def build_wheel() -> Path:
    """Build the current project's wheel into dist/ and return its path."""
    project_root = _repo_root()
    dist = project_root / "dist"
    for existing in dist.glob("*.whl"):
        existing.unlink()
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(dist)],
        cwd=project_root,
        check=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    wheels = sorted(dist.glob("*.whl"))
    if not wheels:
        raise RuntimeError("uv build produced no wheel")
    return wheels[-1]


def _run(cmd: list[str]) -> None:
    print(f"[build] {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True, stdout=sys.stdout, stderr=sys.stderr)


def build_engine(spec: EngineSpec, out_dir: Path) -> Path:
    out = out_dir / f"{spec.name}.tar.gz"
    _run([
        str(_repo_root() / "scripts" / "build-engine.sh"),
        "--commit", spec.commit,
        "--output", str(out),
    ])
    return out


def build_bar_content(spec: BarContentSpec, out_dir: Path) -> Path:
    out = out_dir / f"{spec.name}.tar.gz"
    _run([
        str(_repo_root() / "scripts" / "build-bar-content.sh"),
        "--version", spec.version,
        "--output", str(out),
    ])
    return out


def fetch_map(spec: MapSpec, out_dir: Path) -> Path:
    if spec.source_url is None:
        raise RuntimeError(
            f"map {spec.name!r} is not in the bucket and has no source URL to mirror from; "
            f"upload the map file directly to {spec.dest_uri}, "
            f"or add a `source = \"https://...\"` line to the map's entry in artifacts.toml "
            f"so the orchestrator can mirror it on cache miss"
        )
    # dest URI basename is the canonical on-disk filename (runner copies it
    # to /var/bar-data/maps/<basename>).
    out = out_dir / Path(spec.dest_uri).name
    _run(["curl", "--fail", "--location", "--progress-bar", "--output", str(out), spec.source_url])
    return out


def _workdir() -> Path:
    """Per-run scratch dir for built tarballs. Auto-cleaned on process exit."""
    return Path(tempfile.mkdtemp(prefix="bar-bench-build-"))


def build_and_upload(
    cfg: BatchConfig,
    job_uid: str,
    *,
    cat: Catalog,
    overlay: Path,
    wheel: Path,
    client=None,
    on_upload: Callable[[str, bool], None] | None = None,
) -> None:
    """Ensure shared blobs exist in the artifacts bucket (build+upload on
    cache miss), upload per-job blobs, and write the manifest last.

    `on_upload(uri, cached)` fires for every bucket key touched.
    """
    engine = cat.engine(cfg.engine_name)
    bar = cat.bar_content(cfg.bar_content_name)
    mp = cat.map(cfg.map_name)

    _, engine_key = key_from_uri(engine.dest_uri)
    _, bar_key = key_from_uri(bar.dest_uri)
    _, map_key = key_from_uri(mp.dest_uri)
    map_filename = Path(map_key).name

    if client is None:
        from google.cloud import storage  # lazy import so tests don't need creds

        client = storage.Client(project=cfg.project)
    if on_upload is None:
        def on_upload(uri: str, cached: bool) -> None:
            verb = "cached" if cached else "uploading"
            print(f"[run] {verb} → {uri}", file=sys.stderr)

    bucket_name = cfg.artifacts_bucket.removeprefix("gs://")
    bucket = client.bucket(bucket_name)
    scratch = _workdir()

    def _ensure_shared(key: str, ensure_local: Callable[[], Path]) -> None:
        uri = f"gs://{bucket_name}/{key}"
        blob = bucket.blob(key)
        if blob.exists():
            on_upload(uri, True)
            return
        local = ensure_local()
        on_upload(uri, False)
        blob.upload_from_filename(str(local))

    _ensure_shared(engine_key, lambda: build_engine(engine, scratch))
    _ensure_shared(bar_key, lambda: build_bar_content(bar, scratch))
    _ensure_shared(map_key, lambda: fetch_map(mp, scratch))

    key_prefix = f"{job_uid}/"
    job_uploads = {
        "overlay.tar.gz": overlay,
        "startscript.txt": cfg.scenario_dir / "startscript.txt",
        wheel.name: wheel,
    }
    for name, src in job_uploads.items():
        key = key_prefix + name
        on_upload(f"gs://{bucket_name}/{key}", False)
        bucket.blob(key).upload_from_filename(str(src))

    manifest = json.dumps(
        {
            "job_uid": job_uid,
            "region": cfg.region,
            "instance_type": cfg.machine_type,
            "map_filename": map_filename,
            "artifact_names": ArtifactNames(
                engine=cfg.engine_name,
                bar_content=cfg.bar_content_name,
                map=cfg.map_name,
            ).model_dump(),
            "paths": {"engine": engine_key, "bar_content": bar_key, "map": map_key},
        },
        indent=2,
        sort_keys=True,
    ).encode()
    manifest_key = key_prefix + "manifest.json"
    on_upload(f"gs://{bucket_name}/{manifest_key}", False)
    bucket.blob(manifest_key).upload_from_string(manifest, content_type="application/json")
