"""Hash and upload the 5 input artifacts + the task-side wheel + a manifest.

Layout under the artifacts bucket for one job:

    gs://<artifacts-bucket>/<job_uid>/
        engine.tar.gz
        bar-content.tar.gz
        overlay.tar.gz
        <map_filename>              (original filename, preserved)
        startscript.txt
        bar_benchmarks-<ver>-py3-none-any.whl
        manifest.json
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from bar_benchmarks.types import ArtifactHashes, BatchConfig

CHUNK = 1 << 20


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True)
class UploadPlan:
    """What the orchestrator intends to upload, with hashes and destination keys."""

    hashes: ArtifactHashes
    wheel_hash: str
    wheel_filename: str
    map_filename: str
    key_prefix: str  # "<job_uid>/"
    local_paths: dict[str, Path]  # canonical_name -> local path


def build_wheel(project_root: Path) -> Path:
    """Build the current project's wheel into dist/ and return its path."""
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


def plan(cfg: BatchConfig, job_uid: str, wheel: Path) -> UploadPlan:
    hashes = ArtifactHashes(
        engine=sha256_file(cfg.engine),
        bar_content=sha256_file(cfg.bar_content),
        overlay=sha256_file(cfg.overlay),
        map=sha256_file(cfg.map),
        startscript=sha256_file(cfg.startscript),
    )
    map_filename = cfg.map.name
    wheel_filename = wheel.name
    local = {
        "engine.tar.gz": cfg.engine,
        "bar-content.tar.gz": cfg.bar_content,
        "overlay.tar.gz": cfg.overlay,
        map_filename: cfg.map,
        "startscript.txt": cfg.startscript,
        wheel_filename: wheel,
    }
    return UploadPlan(
        hashes=hashes,
        wheel_hash=sha256_file(wheel),
        wheel_filename=wheel_filename,
        map_filename=map_filename,
        key_prefix=f"{job_uid}/",
        local_paths=local,
    )


def manifest_bytes(cfg: BatchConfig, job_uid: str, plan_: UploadPlan) -> bytes:
    body = {
        "job_uid": job_uid,
        "region": cfg.region,
        "instance_type": cfg.machine_type,
        "map_filename": plan_.map_filename,
        "artifact_hashes": plan_.hashes.model_dump(),
        "wheel_filename": plan_.wheel_filename,
        "wheel_sha256": plan_.wheel_hash,
    }
    return json.dumps(body, indent=2, sort_keys=True).encode()


def upload(bucket_name: str, plan_: UploadPlan, manifest: bytes, *, client=None) -> None:
    """Upload the six files + manifest.json under `<job_uid>/`.

    `client` defaults to a real `google.cloud.storage.Client`; inject in tests.
    """
    if client is None:
        from google.cloud import storage  # imported lazily so tests don't need creds

        client = storage.Client()
    bucket = client.bucket(bucket_name)
    for name, src in plan_.local_paths.items():
        blob = bucket.blob(plan_.key_prefix + name)
        blob.upload_from_filename(str(src))
    manifest_blob = bucket.blob(plan_.key_prefix + "manifest.json")
    manifest_blob.upload_from_string(manifest, content_type="application/json")
