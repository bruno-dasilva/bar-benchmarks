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
OVERLAY_TARBALL = "overlay.tar.gz"
STARTSCRIPT_NAME = "startscript.txt"
MANIFEST_NAME = "manifest.json"
INFOLOG_FILENAME = "infolog.txt"


def _extract_tarball(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(src, "r:*") as tf:
        # filter kwarg arrived in Python 3.11.4 (PEP 706 backport);
        # Debian 12 / batch-debian still ship 3.11.2.
        try:
            tf.extractall(dest, filter="data")
        except TypeError:
            tf.extractall(dest)


def _stage(
    artifacts: Path,
    bucket_root: Path,
    shared_keys: dict[str, str],
    map_filename: str,
) -> Path:
    """Run steps 1–6. Returns the path of the startscript on disk.

    `artifacts` is the per-job FUSE mount (overlay, startscript, wheel,
    manifest). `bucket_root` is the whole-bucket FUSE mount; engine /
    bar-content / map live under it at the keys given by `shared_keys`.
    """
    engine_root = paths.engine_dir()
    data = paths.data_dir()
    games = data / "games"
    bar_sdd = games / BAR_SDD_NAME
    maps = data / "maps"

    for d in (engine_root, games, maps, paths.run_dir()):
        d.mkdir(parents=True, exist_ok=True)

    _extract_tarball(bucket_root / shared_keys["engine"], engine_root)
    _extract_tarball(bucket_root / shared_keys["bar_content"], bar_sdd)
    # The overlay tarball mirrors /var/bar-data/ — files under games/BAR.sdd/
    # overwrite/extend the game content (standard overlay use), while files
    # at other paths drop extras (e.g. a benchmark_snapshot.lua) into
    # /var/bar-data/ so the engine's write-dir picks them up.
    _extract_tarball(artifacts / OVERLAY_TARBALL, data)

    shutil.copy2(bucket_root / shared_keys["map"], maps / map_filename)

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


def iter_dir(i: int) -> Path:
    d = paths.run_dir() / f"iter-{i}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_verdict(i: int, verdict: RunnerVerdict) -> None:
    (iter_dir(i) / "verdict.json").write_text(
        json.dumps(verdict.model_dump(mode="json"), indent=2)
    )


def _run_one(startscript: Path, i: int) -> RunnerVerdict:
    """Execute one engine invocation, capturing its benchmark output + infolog
    into the per-iteration directory so the next iter starts clean."""
    out_path = paths.benchmark_output_path()
    infolog = paths.data_dir() / INFOLOG_FILENAME
    # Wipe leftovers from a prior iter so missing-output detection fires.
    if out_path.exists():
        out_path.unlink()
    if infolog.exists():
        infolog.unlink()

    started_at = datetime.now(UTC)
    engine_exit, wall = _invoke_engine(startscript)
    ended_at = datetime.now(UTC)

    error: str | None = None
    if engine_exit != 0:
        error = "engine_crash"
    elif not out_path.is_file():
        error = "overlay_output_missing"

    verdict = RunnerVerdict(
        started_at=started_at,
        ended_at=ended_at,
        engine_exit=engine_exit,
        engine_wall_s=wall,
        error=error,
    )
    _write_verdict(i, verdict)

    target = iter_dir(i)
    if out_path.is_file():
        shutil.move(str(out_path), str(target / "benchmark.json"))
    if infolog.is_file():
        shutil.move(str(infolog), str(target / INFOLOG_FILENAME))
    return verdict


def run() -> list[RunnerVerdict]:
    artifacts = paths.artifacts_dir()
    bucket_root = paths.artifacts_bucket_dir()
    manifest = json.loads((artifacts / MANIFEST_NAME).read_text())
    map_filename = manifest["map_filename"]
    shared_keys = manifest["paths"]
    iterations = int(manifest.get("iterations", 1))

    try:
        startscript = _stage(artifacts, bucket_root, shared_keys, map_filename)
    except Exception as e:
        # Staging failure applies to all iterations — record a single
        # iter-0 failure verdict so the collector has something to upload.
        now = datetime.now(UTC)
        verdict = RunnerVerdict(
            started_at=now,
            ended_at=now,
            engine_exit=-1,
            error=f"runner_exception: {e}",
        )
        _write_verdict(0, verdict)
        return [verdict]

    verdicts: list[RunnerVerdict] = []
    for i in range(iterations):
        try:
            verdicts.append(_run_one(startscript, i))
        except Exception as e:
            now = datetime.now(UTC)
            v = RunnerVerdict(
                started_at=now,
                ended_at=now,
                engine_exit=-1,
                error=f"runner_exception: {e}",
            )
            _write_verdict(i, v)
            verdicts.append(v)
    return verdicts


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
