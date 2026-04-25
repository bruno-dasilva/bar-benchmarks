"""Altair box plot for comparing two batches of per-VM measurements.

Gated behind the `plot` extra. Importing this module without altair +
pandas + vl-convert-python installed raises ModuleNotFoundError; the
CLI catches it and rewords to `pip install bar-benchmarks[plot]`.

Example:
    chart = boxplot_compare(a, b, label_a="recoil-abc1234", label_b="recoil-def5678",
                            x_title="sim mean (ms)")
    chart.save("comparison.png")  # via vl-convert-python
"""

from __future__ import annotations

from collections.abc import Sequence

import altair as alt
import pandas as pd


def boxplot_compare(
    samples_a: Sequence[float],
    samples_b: Sequence[float],
    label_a: str = "A",
    label_b: str = "B",
    x_title: str = "value",
    title: str = "Box plot — IQR, whiskers = full range, dots = raw samples",
) -> alt.LayerChart:
    """Two-row horizontal box plot with raw samples overlaid as dots.

    Returns a layered Altair chart. Caller decides how to render
    (Jupyter inline, `chart.save("x.html")`, `chart.save("x.png")`).
    """
    rows: list[dict[str, float | str]] = []
    for v in samples_a:
        rows.append({"group": label_a, "value": float(v)})
    for v in samples_b:
        rows.append({"group": label_b, "value": float(v)})
    df = pd.DataFrame(rows, columns=["group", "value"])

    sort = [label_a, label_b]
    y_enc = alt.Y("group:N", title=None, sort=sort)
    color_enc = alt.Color("group:N", sort=sort, legend=None)

    box = (
        alt.Chart(df)
        .mark_boxplot(size=40, extent="min-max")
        .encode(
            x=alt.X("value:Q", title=x_title, scale=alt.Scale(zero=False)),
            y=y_enc,
            color=color_enc,
        )
    )
    dots = (
        alt.Chart(df)
        .mark_circle(size=35, opacity=0.35)
        .encode(
            x=alt.X("value:Q", title=x_title, scale=alt.Scale(zero=False)),
            y=y_enc,
            color=color_enc,
            tooltip=[
                alt.Tooltip("group:N"),
                alt.Tooltip("value:Q", format=".3f"),
            ],
        )
    )
    return (box + dots).properties(width=720, height=alt.Step(70), title=title)
