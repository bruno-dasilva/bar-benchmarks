"""Shell out to scripts/build-{engine,bar-content}.sh on cache miss.

The production orchestrator checks the bucket first; only when a named
artifact is missing does it invoke the build helper to materialize it
locally, then upload to the catalog's `dest`. Map misses fall through
to mirroring the `source` URL with curl.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from bar_benchmarks.orchestrator.catalog import BarContentSpec, EngineSpec, MapSpec


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("could not locate project root (no pyproject.toml on ancestor path)")


def _scripts_dir() -> Path:
    return _repo_root() / "scripts"


def _run(cmd: list[str]) -> None:
    print(f"[build] {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True, stdout=sys.stdout, stderr=sys.stderr)


def build_engine(spec: EngineSpec, out_dir: Path) -> Path:
    out = out_dir / f"{spec.name}.tar.gz"
    _run([
        str(_scripts_dir() / "build-engine.sh"),
        "--commit", spec.commit,
        "--output", str(out),
    ])
    return out


def build_bar_content(spec: BarContentSpec, out_dir: Path) -> Path:
    out = out_dir / f"{spec.name}.tar.gz"
    _run([
        str(_scripts_dir() / "build-bar-content.sh"),
        "--version", spec.version,
        "--output", str(out),
    ])
    return out


def fetch_map(spec: MapSpec, out_dir: Path) -> Path:
    if spec.source_url is None:
        raise RuntimeError(
            f"map {spec.name!r} is not in the bucket and has no source URL to mirror from; "
            f"upload the map file directly to {spec.dest_uri}, "
            f"or add a `source = \"https://...\"` line to the map's entry in scripts/artifacts.toml "
            f"so the orchestrator can mirror it on cache miss"
        )
    # dest URI basename is the canonical on-disk filename (runner copies it
    # to /var/bar-data/maps/<basename>).
    out = out_dir / Path(spec.dest_uri).name
    _run(["curl", "--fail", "--location", "--progress-bar", "--output", str(out), spec.source_url])
    return out


def workdir() -> Path:
    """Per-run scratch dir for built tarballs. Auto-cleaned on process exit."""
    return Path(tempfile.mkdtemp(prefix="bar-bench-build-"))
