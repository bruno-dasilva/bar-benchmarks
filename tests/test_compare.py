from __future__ import annotations

import numpy as np
import pytest

from bar_benchmarks.stats import compare as compare_mod
from bar_benchmarks.types import BatchReport, PerVmSim


def _report(job_uid: str, means: list[float]) -> BatchReport:
    return BatchReport(
        job_uid=job_uid,
        submitted=len(means),
        valid=len(means),
        invalid=0,
        per_vm=[PerVmSim(vm_id=f"vm-{i}", mean_ms=m) for i, m in enumerate(means)],
    )


def test_bca_ci_matches_scipy_reference() -> None:
    # n=10 per side -> min(n)=10 -> trim=0.20.
    # Reference computed via scipy.stats.bootstrap(..., method='BCa',
    # n_resamples=10_000, random_state=np.random.default_rng(0)) on the
    # trimmed-mean delta with the same input vectors.
    cand = [22.1, 22.3, 22.0, 22.5, 22.2, 22.4, 22.1, 22.3, 22.2, 22.4]
    base = [22.0, 21.9, 22.1, 22.2, 22.0, 21.8, 22.1, 22.0, 22.1, 22.0]
    cmp = compare_mod.compare(_report("cand", cand), _report("base", base))

    assert cmp.trim_fraction == pytest.approx(0.20)
    assert cmp.cand_trimmed_mean_ms == pytest.approx(22.25, abs=1e-9)
    assert cmp.base_trimmed_mean_ms == pytest.approx(22.033333, abs=1e-5)
    assert cmp.delta_ms == pytest.approx(0.216667, abs=1e-5)
    assert cmp.delta_ms_low == pytest.approx(0.083333, abs=1e-5)
    assert cmp.delta_ms_high == pytest.approx(0.350000, abs=1e-5)
    assert cmp.significant is True
    assert cmp.n_resamples == 10_000
    assert cmp.ci_method == "BCa"
    # Untrimmed sample means stay populated for reference.
    assert cmp.cand_mean_ms == pytest.approx(22.25, abs=1e-9)
    assert cmp.base_mean_ms == pytest.approx(22.02, abs=1e-9)
    # Percent CI rescales by trimmed baseline mean (22.033333).
    assert cmp.delta_pct_low == pytest.approx(0.083333 / 22.033333 * 100, abs=1e-4)
    assert cmp.delta_pct_high == pytest.approx(0.350000 / 22.033333 * 100, abs=1e-4)


def test_identical_samples_ci_spans_zero() -> None:
    samples = [10.0, 11.0, 12.0, 13.0, 14.0]
    cmp = compare_mod.compare(_report("c", samples), _report("b", samples))

    assert cmp.delta_ms == 0.0
    # Reference from seeded BCa bootstrap at trim=0.20.
    assert cmp.delta_ms_low == pytest.approx(-2.333333, abs=1e-5)
    assert cmp.delta_ms_high == pytest.approx(2.0, abs=1e-5)
    assert cmp.significant is False


def test_zero_variance_both_sides() -> None:
    cmp = compare_mod.compare(_report("c", [5.0, 5.0]), _report("b", [4.0, 4.0]))
    assert cmp.delta_ms == pytest.approx(1.0)
    assert cmp.delta_ms_low == pytest.approx(1.0)
    assert cmp.delta_ms_high == pytest.approx(1.0)
    # Both sides constant -> degenerate branch, no bootstrap.
    assert cmp.n_resamples == 0
    assert cmp.significant is True


def test_insufficient_samples_returns_null_ci() -> None:
    cmp = compare_mod.compare(_report("c", [5.0]), _report("b", [4.0, 4.2, 4.1]))
    assert cmp.n_cand == 1
    assert cmp.n_base == 3
    assert cmp.delta_ms is None
    assert cmp.cand_trimmed_mean_ms is None
    assert cmp.base_trimmed_mean_ms is None
    assert cmp.trim_fraction is None
    assert cmp.n_resamples is None
    assert cmp.significant is False
    # Untrimmed means are still populated when available.
    assert cmp.cand_mean_ms == pytest.approx(5.0)
    assert cmp.base_mean_ms == pytest.approx(4.1)


def test_empty_report() -> None:
    cmp = compare_mod.compare(_report("c", []), _report("b", [1.0, 2.0]))
    assert cmp.n_cand == 0
    assert cmp.cand_mean_ms is None
    assert cmp.delta_ms is None


def test_alpha_passthrough_widens_ci() -> None:
    samples_c = [10.0, 10.5, 11.0, 10.8, 10.2]
    samples_b = [9.8, 10.0, 10.1, 9.9, 10.0]
    cmp95 = compare_mod.compare(_report("c", samples_c), _report("b", samples_b), alpha=0.05)
    cmp99 = compare_mod.compare(_report("c", samples_c), _report("b", samples_b), alpha=0.01)

    width_95 = cmp95.delta_ms_high - cmp95.delta_ms_low
    width_99 = cmp99.delta_ms_high - cmp99.delta_ms_low
    assert width_99 > width_95
    assert cmp99.alpha == 0.01
    assert cmp95.alpha == 0.05


def test_adaptive_trim_picks_20_percent_at_small_n() -> None:
    # min(n_cand, n_base) = 10 -> trim_fraction = 0.20.
    cand = [22.1, 22.3, 22.0, 22.5, 22.2, 22.4, 22.1, 22.3, 22.2, 22.4]
    base = [22.0, 21.9, 22.1, 22.2, 22.0, 21.8, 22.1, 22.0, 22.1, 22.0]
    cmp = compare_mod.compare(_report("c", cand), _report("b", base))
    assert cmp.trim_fraction == pytest.approx(0.20)


def test_adaptive_trim_picks_10_percent_at_large_n() -> None:
    # min(n_cand, n_base) = 25 -> trim_fraction = 0.10.
    rng = np.random.default_rng(42)
    base = list(rng.normal(20.0, 0.5, 25))
    cand = [x + 0.5 for x in base]
    cmp = compare_mod.compare(_report("c", cand), _report("b", base))
    assert cmp.trim_fraction == pytest.approx(0.10)
    # Sanity: bootstrap CI should bracket the true +0.5 shift.
    assert cmp.delta_ms_low < 0.5 < cmp.delta_ms_high


def test_trim_robust_to_single_outlier() -> None:
    # Candidate is baseline + 0.05 ms shift with one large positive outlier.
    # Trimmed mean should ignore the outlier; raw mean would not.
    base = [20.0, 20.1, 19.9, 20.2, 19.8, 20.0, 20.1, 19.9, 20.0, 20.1, 19.9, 20.0]
    cand = [20.05, 20.15, 19.95, 20.25, 19.85, 20.05, 20.15, 19.95, 20.05, 20.15, 19.95, 50.0]
    cmp = compare_mod.compare(_report("c", cand), _report("b", base))

    assert cmp.trim_fraction == pytest.approx(0.20)
    # Trimmed delta is small (~0.06 ms); raw mean delta would be ~2.5 ms.
    raw_delta = cmp.cand_mean_ms - cmp.base_mean_ms
    assert raw_delta > 2.0
    assert abs(cmp.delta_ms) < 0.2
