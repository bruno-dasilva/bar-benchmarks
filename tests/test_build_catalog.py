from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from bar_benchmarks.orchestrator.catalog import Catalog

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "action" / "build_catalog.py"

BASE_CATALOG = """\
[engine.recoil-abc1234]
dest = "gs://bucket/engine/recoil-abc1234.tar.gz"
commit = "abc1234deadbeef"

[bar_content.bar-test-1-abc1234]
dest = "gs://bucket/bar-content/bar-test-1-abc1234.tar.gz"
version = "Beyond All Reason test-1-abc1234"

[map."m1"]
dest = "gs://bucket/maps/m1.sd7"
"""


def _run(tmp_path: Path, *args: str) -> subprocess.CompletedProcess:
    base = tmp_path / "artifacts.toml"
    base.write_text(BASE_CATALOG)
    out = tmp_path / "merged.toml"
    return subprocess.run(
        [
            sys.executable, str(SCRIPT),
            "--base-catalog", str(base),
            "--out-catalog", str(out),
            "--artifacts-bucket", "gs://bucket",
            *args,
        ],
        capture_output=True, text=True,
    )


def test_passthrough_registered_names(tmp_path):
    r = _run(tmp_path,
             "--engine", "recoil-abc1234",
             "--bar-content", "bar-test-1-abc1234")
    assert r.returncode == 0, r.stderr
    assert "engine-name=recoil-abc1234" in r.stdout
    assert "bar-content-name=bar-test-1-abc1234" in r.stdout

    merged = tmp_path / "merged.toml"
    cat = Catalog.load(merged)
    # Nothing new appended for pass-through; original entries still resolve.
    assert cat.engine("recoil-abc1234").commit == "abc1234deadbeef"


def test_adhoc_engine_commit_appends_entry(tmp_path):
    r = _run(tmp_path,
             "--engine-commit", "5c157c84bf11cfeadadade183f373b03cdb9fb7a",
             "--bar-content", "bar-test-1-abc1234")
    assert r.returncode == 0, r.stderr
    assert "engine-name=recoil-5c157c8" in r.stdout

    cat = Catalog.load(tmp_path / "merged.toml")
    eng = cat.engine("recoil-5c157c8")
    assert eng.commit == "5c157c84bf11cfeadadade183f373b03cdb9fb7a"
    assert eng.dest_uri == "gs://bucket/engine/recoil-5c157c8.tar.gz"


def test_adhoc_bar_content_version_appends_entry(tmp_path):
    r = _run(tmp_path,
             "--engine", "recoil-abc1234",
             "--bar-content-version", "Beyond All Reason test-29871-90f4bc1")
    assert r.returncode == 0, r.stderr
    assert "bar-content-name=bar-test-29871-90f4bc1" in r.stdout

    cat = Catalog.load(tmp_path / "merged.toml")
    bc = cat.bar_content("bar-test-29871-90f4bc1")
    assert bc.version == "Beyond All Reason test-29871-90f4bc1"
    assert bc.dest_uri == "gs://bucket/bar-content/bar-test-29871-90f4bc1.tar.gz"


def test_collision_with_registered_name_reuses_entry(tmp_path):
    # The truncated SHA already matches `recoil-abc1234` in the base catalog —
    # we should reuse it, not append a duplicate.
    r = _run(tmp_path,
             "--engine-commit", "abc1234deadbeef",
             "--bar-content", "bar-test-1-abc1234")
    assert r.returncode == 0, r.stderr
    # abc1234d[eadbeef] → first 7 chars = abc1234 → name = recoil-abc1234
    assert "engine-name=recoil-abc1234" in r.stdout
    # Merged file should parse and contain the entry exactly once.
    merged = (tmp_path / "merged.toml").read_text()
    assert merged.count("[engine.recoil-abc1234]") + merged.count('[engine."recoil-abc1234"]') == 1


def test_rejects_mutually_exclusive_flags(tmp_path):
    r = _run(tmp_path,
             "--engine", "recoil-abc1234",
             "--engine-commit", "deadbeefcafebab",
             "--bar-content", "bar-test-1-abc1234")
    assert r.returncode != 0
    assert "mutually exclusive" in r.stderr


def test_rejects_unknown_registered_name(tmp_path):
    r = _run(tmp_path,
             "--engine", "recoil-does-not-exist",
             "--bar-content", "bar-test-1-abc1234")
    assert r.returncode != 0
    assert "not in base catalog" in r.stderr


def test_rejects_malformed_version(tmp_path):
    r = _run(tmp_path,
             "--engine", "recoil-abc1234",
             "--bar-content-version", "some other string")
    assert r.returncode != 0
    assert "Beyond All Reason" in r.stderr
