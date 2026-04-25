"""BCa bootstrap comparison of two BatchReports.

The sample unit is per-VM sim frame-time mean (``PerVmSim.mean_ms``):
each VM contributes one independent observation. We emit a BCa
bootstrap two-sided CI on the difference of per-side **trimmed** means
(candidate − baseline), rescaled to percent of the trimmed baseline
mean. Trim fraction is 20% per side when ``min(n_cand, n_base) ≤ 20``,
else 10%. Uses scipy for ``trim_mean`` and ``bootstrap``.
"""

from __future__ import annotations

import statistics

import numpy as np
from scipy import stats as scipy_stats

from bar_benchmarks.types import BatchReport, ComparisonReport

N_RESAMPLES = 10_000
RNG_SEED = 0


def compare(
    cand_report: BatchReport,
    base_report: BatchReport,
    *,
    alpha: float = 0.05,
) -> ComparisonReport:
    """Compare candidate vs baseline BatchReports, returning a BCa CI."""
    cand = [p.mean_ms for p in cand_report.per_vm]
    base = [p.mean_ms for p in base_report.per_vm]
    return _build(
        cand_job_uid=cand_report.job_uid,
        base_job_uid=base_report.job_uid,
        cand=cand,
        base=base,
        alpha=alpha,
    )


def _trim_fraction(n_cand: int, n_base: int) -> float:
    return 0.20 if min(n_cand, n_base) <= 20 else 0.10


def _build(
    *,
    cand_job_uid: str,
    base_job_uid: str,
    cand: list[float],
    base: list[float],
    alpha: float,
) -> ComparisonReport:
    n_c, n_b = len(cand), len(base)
    mean_c = statistics.fmean(cand) if cand else None
    mean_b = statistics.fmean(base) if base else None

    # Need n >= 2 on both sides for any usable bootstrap.
    if n_c < 2 or n_b < 2 or mean_c is None or mean_b is None:
        return ComparisonReport(
            cand_job_uid=cand_job_uid,
            base_job_uid=base_job_uid,
            n_cand=n_c,
            n_base=n_b,
            cand_mean_ms=mean_c,
            base_mean_ms=mean_b,
            alpha=alpha,
        )

    trim = _trim_fraction(n_c, n_b)
    cand_arr = np.asarray(cand, dtype=float)
    base_arr = np.asarray(base, dtype=float)

    trim_c = float(scipy_stats.trim_mean(cand_arr, trim))
    trim_b = float(scipy_stats.trim_mean(base_arr, trim))
    delta = trim_c - trim_b

    def stat(c: np.ndarray, b: np.ndarray, axis: int = -1) -> np.ndarray:
        return scipy_stats.trim_mean(c, trim, axis=axis) - scipy_stats.trim_mean(
            b, trim, axis=axis
        )

    # BCa needs > 1 distinct value to compute the acceleration constant
    # via jackknife. When both sides are constant the CI collapses to
    # the point estimate (mirrors the previous SE==0 branch).
    if np.ptp(cand_arr) == 0 and np.ptp(base_arr) == 0:
        low = high = delta
        n_resamples = 0
    else:
        rng = np.random.default_rng(RNG_SEED)
        result = scipy_stats.bootstrap(
            (cand_arr, base_arr),
            statistic=stat,
            method="BCa",
            confidence_level=1.0 - alpha,
            n_resamples=N_RESAMPLES,
            paired=False,
            vectorized=True,
            random_state=rng,
        )
        low = float(result.confidence_interval.low)
        high = float(result.confidence_interval.high)
        n_resamples = N_RESAMPLES

    if trim_b != 0:
        delta_pct = delta / trim_b * 100.0
        pct_low = low / trim_b * 100.0
        pct_high = high / trim_b * 100.0
    else:
        delta_pct = pct_low = pct_high = None

    significant = low > 0 or high < 0

    return ComparisonReport(
        cand_job_uid=cand_job_uid,
        base_job_uid=base_job_uid,
        n_cand=n_c,
        n_base=n_b,
        cand_mean_ms=mean_c,
        base_mean_ms=mean_b,
        cand_trimmed_mean_ms=trim_c,
        base_trimmed_mean_ms=trim_b,
        trim_fraction=trim,
        delta_ms=delta,
        delta_ms_low=low,
        delta_ms_high=high,
        delta_pct=delta_pct,
        delta_pct_low=pct_low,
        delta_pct_high=pct_high,
        n_resamples=n_resamples,
        ci_method="BCa",
        alpha=alpha,
        significant=significant,
    )


def print_comparison(cmp: ComparisonReport) -> None:
    """Human-readable comparison output, mirroring aggregate.print_report."""
    print(f"\n=== Compare {cmp.cand_job_uid} vs {cmp.base_job_uid} ===")
    print(f"candidate: mean= {_fmt(cmp.cand_mean_ms)}ms  (n={cmp.n_cand})")
    print(f"baseline:  mean= {_fmt(cmp.base_mean_ms)}ms  (n={cmp.n_base})")
    if cmp.delta_ms is None:
        print("insufficient samples for BCa CI (need n ≥ 2 per side)")
        return
    trim_pct = (cmp.trim_fraction or 0.0) * 100.0
    print(
        f"candidate trimmed ({trim_pct:.0f}%): {_fmt(cmp.cand_trimmed_mean_ms)}ms"
    )
    print(
        f"baseline trimmed  ({trim_pct:.0f}%): {_fmt(cmp.base_trimmed_mean_ms)}ms"
    )
    print(
        f"Δ = {cmp.delta_ms:+.3f}ms  "
        f"{(1.0 - cmp.alpha) * 100:.0f}% CI "
        f"[{cmp.delta_ms_low:+.3f}, {cmp.delta_ms_high:+.3f}]ms"
    )
    if cmp.delta_pct is not None:
        print(
            f"Δ = {cmp.delta_pct:+.2f}%  "
            f"{(1.0 - cmp.alpha) * 100:.0f}% CI "
            f"[{cmp.delta_pct_low:+.2f}%, {cmp.delta_pct_high:+.2f}%]  "
            f"{'SIGNIFICANT' if cmp.significant else 'not significant'}"
        )
    print(
        f"CI: {cmp.ci_method} bootstrap, n_resamples={cmp.n_resamples}, "
        f"alpha={cmp.alpha}"
    )


def _fmt(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.3f}"
