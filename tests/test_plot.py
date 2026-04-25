from __future__ import annotations

import pytest

alt = pytest.importorskip("altair")
pytest.importorskip("pandas")

from bar_benchmarks.stats.plot import boxplot_compare  # noqa: E402


def test_boxplot_compare_returns_layered_chart() -> None:
    a = [22.1, 22.3, 22.0, 22.5, 22.2]
    b = [22.0, 21.9, 22.1, 22.2, 22.0]
    chart = boxplot_compare(
        a, b, label_a="cand-abc", label_b="base-def", x_title="sim mean (ms)"
    )

    assert isinstance(chart, alt.LayerChart)
    spec = chart.to_dict()
    assert spec["width"] == 720
    # Two layers: boxplot and circles.
    assert len(spec["layer"]) == 2
    # Row order is locked to the insertion order, not alphabetical.
    for layer in spec["layer"]:
        assert layer["encoding"]["y"]["sort"] == ["cand-abc", "base-def"]
        assert layer["encoding"]["x"]["scale"] == {"zero": False}
    # Tooltip on the dot layer formats values to .3f.
    dot_layer = spec["layer"][1]
    fmts = [t.get("format") for t in dot_layer["encoding"]["tooltip"]]
    assert ".3f" in fmts


def test_boxplot_compare_handles_unequal_sizes() -> None:
    a = [10.0, 11.0, 12.0]
    b = [9.0, 9.5, 10.0, 10.5, 11.0, 11.5, 12.0]
    chart = boxplot_compare(a, b, label_a="A", label_b="B")
    spec = chart.to_dict()
    # 10 rows total in the long-form data backing the chart.
    assert len(spec["datasets"][next(iter(spec["datasets"]))]) == len(a) + len(b)
