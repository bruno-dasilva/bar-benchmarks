"""Task-side entrypoint: preflight then (if passed) runner."""

from __future__ import annotations

from bar_benchmarks.task import preflight, runner


def main() -> int:
    pf = preflight.run()
    if not pf.passed:
        return 0
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
