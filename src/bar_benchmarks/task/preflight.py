"""Preflight microbenchmark. MVP stub: always passes, emits empty microbench."""

from __future__ import annotations

import json

from bar_benchmarks import paths
from bar_benchmarks.types import PreflightResult


def run() -> PreflightResult:
    result = PreflightResult(passed=True, microbench={})
    out = paths.run_dir() / "preflight.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result.model_dump(), indent=2))
    return result


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
