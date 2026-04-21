from __future__ import annotations

import json

from bar_benchmarks.task import preflight


def test_preflight_stub_passes(task_env):
    result = preflight.run()
    assert result.passed is True

    on_disk = json.loads((task_env["run"] / "preflight.json").read_text())
    assert on_disk == {"passed": True, "microbench": {}}
