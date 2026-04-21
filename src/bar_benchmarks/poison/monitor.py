"""Poison-monitor. MVP stub: writes one clean summary and idles until SIGTERM.

Real implementation will sample /proc/stat (CPU steal %) and other signals
on a rolling window; see ARCHITECTURE.md § poison-monitor.
"""

from __future__ import annotations

import json
import signal
import sys
import time

from bar_benchmarks import paths
from bar_benchmarks.types import PoisonSummary


def _write_summary(summary: PoisonSummary) -> None:
    out = paths.run_dir() / "poison.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary.model_dump(), indent=2))


def main() -> int:
    summary = PoisonSummary(tripped=False, signals={})
    _write_summary(summary)

    # `background: true` keeps this runnable resident until the Task exits;
    # sleep in short intervals so SIGTERM is handled promptly.
    stop = {"flag": False}

    def _handle(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    while not stop["flag"]:
        time.sleep(1.0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
