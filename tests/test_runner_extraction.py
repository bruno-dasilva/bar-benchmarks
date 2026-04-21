from __future__ import annotations

from bar_benchmarks.task import runner


def test_stage_lays_out_filesystem(task_env, tiny_artifacts):
    map_filename = tiny_artifacts["map_filename"]
    startscript = runner._stage(task_env["artifacts"], map_filename)

    bar_sdd = task_env["data"] / "games" / "BAR.sdd"
    assert (task_env["engine"] / "spring-headless").is_file()
    assert (bar_sdd / "VERSION").read_text().strip() == "1.2.3"
    # overlay overwrote shared.lua, added extra.lua
    assert (bar_sdd / "shared.lua").read_text() == "-- overlay wins\n"
    assert (bar_sdd / "extra.lua").is_file()
    assert (task_env["data"] / "maps" / map_filename).read_bytes() == b"map-bytes"
    assert startscript == task_env["artifacts"] / "startscript.txt"
