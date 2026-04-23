"""Merge ad-hoc engine / bar-content entries onto the base artifacts.toml.

Used by the GitHub Action wrapper. The composite action can accept either a
catalog name (already registered in artifacts.toml) or an ad-hoc identifier
(engine commit SHA, bar-content version string) that the caller has not
pre-registered. On the ad-hoc path this script synthesizes a catalog entry
on the fly so the orchestrator sees it like any other entry.

Inputs:
  --base-catalog PATH      Path to the repo's artifacts.toml.
  --out-catalog PATH       Where to write the merged catalog.
  --artifacts-bucket URI   gs://<bucket> — used to build dest URIs for ad-hoc entries.
  --engine NAME            (optional) Pre-registered engine name. Pass through.
  --engine-commit SHA      (optional) Ad-hoc engine commit. Mutually exclusive with --engine.
  --bar-content NAME       (optional) Pre-registered bar-content name. Pass through.
  --bar-content-version S  (optional) Ad-hoc bar-content version string. Mutually exclusive.

Outputs:
  Writes merged catalog to --out-catalog. Prints GITHUB_OUTPUT lines to stdout:
    engine-name=<resolved name>
    bar-content-name=<resolved name>
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path


def _toml_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _resolve_engine(
    data: dict,
    *,
    engine: str | None,
    engine_commit: str | None,
    artifacts_bucket: str,
) -> tuple[str, str | None]:
    """Return (effective_name, toml_block_to_append_or_None)."""
    if engine and engine_commit:
        sys.exit("--engine and --engine-commit are mutually exclusive")
    if not engine and not engine_commit:
        sys.exit("one of --engine or --engine-commit is required")
    if engine:
        if engine not in data.get("engine", {}):
            sys.exit(f"engine {engine!r} not in base catalog")
        return engine, None

    # Ad-hoc path: synthesize an entry.
    sha = engine_commit.strip().lower()
    if len(sha) < 7 or not all(c in "0123456789abcdef" for c in sha):
        sys.exit(f"--engine-commit must be a hex SHA (>=7 chars), got {engine_commit!r}")
    name = f"recoil-{sha[:7]}"
    if name in data.get("engine", {}):
        return name, None  # already registered, reuse
    dest = f"{artifacts_bucket.rstrip('/')}/engine/{name}.tar.gz"
    block = (
        f'\n[engine."{_toml_escape(name)}"]\n'
        f'dest = "{_toml_escape(dest)}"\n'
        f'commit = "{_toml_escape(sha)}"\n'
    )
    return name, block


def _resolve_bar_content(
    data: dict,
    *,
    bar_content: str | None,
    bar_content_version: str | None,
    artifacts_bucket: str,
) -> tuple[str, str | None]:
    if bar_content and bar_content_version:
        sys.exit("--bar-content and --bar-content-version are mutually exclusive")
    if not bar_content and not bar_content_version:
        sys.exit("one of --bar-content or --bar-content-version is required")
    if bar_content:
        if bar_content not in data.get("bar_content", {}):
            sys.exit(f"bar-content {bar_content!r} not in base catalog")
        return bar_content, None

    version = bar_content_version.strip()
    prefix = "Beyond All Reason "
    if not version.startswith(prefix):
        sys.exit(f"--bar-content-version must start with {prefix!r}, got {version!r}")
    # "Beyond All Reason test-29871-90f4bc1" → "bar-test-29871-90f4bc1"
    name = "bar-" + version[len(prefix):].replace(" ", "-")
    if name in data.get("bar_content", {}):
        return name, None
    dest = f"{artifacts_bucket.rstrip('/')}/bar-content/{name}.tar.gz"
    block = (
        f'\n[bar_content."{_toml_escape(name)}"]\n'
        f'dest = "{_toml_escape(dest)}"\n'
        f'version = "{_toml_escape(version)}"\n'
    )
    return name, block


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-catalog", required=True, type=Path)
    p.add_argument("--out-catalog", required=True, type=Path)
    p.add_argument("--artifacts-bucket", required=True)
    p.add_argument("--engine", default=None)
    p.add_argument("--engine-commit", default=None)
    p.add_argument("--bar-content", default=None)
    p.add_argument("--bar-content-version", default=None)
    args = p.parse_args(argv)

    base_text = args.base_catalog.read_text()
    data = tomllib.loads(base_text)

    engine_name, engine_block = _resolve_engine(
        data,
        engine=args.engine,
        engine_commit=args.engine_commit,
        artifacts_bucket=args.artifacts_bucket,
    )
    bc_name, bc_block = _resolve_bar_content(
        data,
        bar_content=args.bar_content,
        bar_content_version=args.bar_content_version,
        artifacts_bucket=args.artifacts_bucket,
    )

    merged = base_text
    if not merged.endswith("\n"):
        merged += "\n"
    if engine_block:
        merged += engine_block
    if bc_block:
        merged += bc_block

    # Re-parse as a final self-check — if we emitted invalid TOML the
    # orchestrator would fail with a confusing error later.
    tomllib.loads(merged)
    args.out_catalog.write_text(merged)

    print(f"engine-name={engine_name}")
    print(f"bar-content-name={bc_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
