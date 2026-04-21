from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

from bar_benchmarks import paths


def test_poison_monitor_writes_and_idles(task_env):
    env = dict(os.environ)
    proc = subprocess.Popen(
        [sys.executable, "-m", "bar_benchmarks.poison.monitor"],
        env=env,
    )
    try:
        poison_file = paths.run_dir() / "poison.json"
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not poison_file.is_file():
            time.sleep(0.05)
        assert poison_file.is_file(), "poison.json was never written"
        content = json.loads(poison_file.read_text())
        assert content == {"tripped": False, "signals": {}}

        time.sleep(0.2)
        assert proc.poll() is None, "monitor exited early; should idle in background"
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    assert proc.returncode == 0
