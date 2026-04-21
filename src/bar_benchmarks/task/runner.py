"""Stage artifacts onto local disk and invoke spring-headless.

Implements steps 1–9 of ARCHITECTURE.md § runner. Runs on the Batch VM;
reads from /mnt/artifacts (Cloud Storage FUSE) and writes to
/var/bar-data and /var/bar-run.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path

from bar_benchmarks import paths
from bar_benchmarks.types import RunnerVerdict

BAR_SDD_NAME = "BAR.sdd"
ENGINE_TARBALL = "engine.tar.gz"
BAR_CONTENT_TARBALL = "bar-content.tar.gz"
OVERLAY_TARBALL = "overlay.tar.gz"
STARTSCRIPT_NAME = "startscript.txt"
MANIFEST_NAME = "manifest.json"


def _extract_tarball(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(src, "r:*") as tf:
        # filter kwarg arrived in Python 3.11.4 (PEP 706 backport);
        # Debian 12 / batch-debian still ship 3.11.2.
        try:
            tf.extractall(dest, filter="data")
        except TypeError:
            tf.extractall(dest)


def _stage(artifacts: Path, map_filename: str) -> Path:
    """Run steps 1–6. Returns the path of the startscript on disk."""
    engine_root = paths.engine_dir()
    data = paths.data_dir()
    games = data / "games"
    bar_sdd = games / BAR_SDD_NAME
    maps = data / "maps"

    for d in (engine_root, games, maps, paths.run_dir()):
        d.mkdir(parents=True, exist_ok=True)

    _extract_tarball(artifacts / ENGINE_TARBALL, engine_root)
    _extract_tarball(artifacts / BAR_CONTENT_TARBALL, bar_sdd)
    # The overlay tarball mirrors /var/bar-data/ — files under games/BAR.sdd/
    # overwrite/extend the game content (standard overlay use), while files
    # at other paths drop extras (e.g. a benchmark_snapshot.lua) into
    # /var/bar-data/ so the engine's write-dir picks them up.
    _extract_tarball(artifacts / OVERLAY_TARBALL, data)

    shutil.copy2(artifacts / map_filename, maps / map_filename)

    if not (bar_sdd / "VERSION").is_file():
        raise RuntimeError(f"BAR content missing VERSION file at {bar_sdd / 'VERSION'}")
    if not (maps / map_filename).is_file():
        raise RuntimeError(f"Map missing at {maps / map_filename}")

    return artifacts / STARTSCRIPT_NAME


def _invoke_engine(startscript: Path) -> tuple[int, float]:
    binary = paths.engine_dir() / "spring-headless"
    if not binary.is_file():
        raise RuntimeError(f"spring-headless not found at {binary}")
    if not os.access(binary, os.X_OK):
        binary.chmod(0o755)

    cmd = [str(binary), "--isolation", "--write-dir", str(paths.data_dir()), str(startscript)]
    t0 = time.monotonic()
    proc = subprocess.run(cmd, check=False)
    wall = time.monotonic() - t0
    return proc.returncode, wall


def run() -> RunnerVerdict:
    started_at = datetime.now(UTC)
    artifacts = paths.artifacts_dir()
    manifest = json.loads((artifacts / MANIFEST_NAME).read_text())
    map_filename = manifest["map_filename"]

    error: str | None = None
    engine_exit = -1
    wall = 0.0
    bench_out: str | None = None

    try:
        startscript = _stage(artifacts, map_filename)
        engine_exit, wall = _invoke_engine(startscript)
        if engine_exit != 0:
            error = "engine_crash"
        else:
            bp = paths.benchmark_output_path()
            if bp.is_file():
                bench_out = str(bp)
            else:
                error = "overlay_output_missing"
    except Exception as e:
        error = f"runner_exception: {e}"

    ended_at = datetime.now(UTC)
    verdict = RunnerVerdict(
        started_at=started_at,
        ended_at=ended_at,
        engine_exit=engine_exit,
        timings={"engine_wall_s": wall},
        benchmark_output_path=bench_out,
        error=error,
    )

    out = paths.run_dir() / "verdict.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(verdict.model_dump(mode="json"), indent=2))
    return verdict


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
