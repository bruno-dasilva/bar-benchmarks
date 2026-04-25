from __future__ import annotations

from pathlib import Path

import typer

from bar_benchmarks.types import BatchConfig, BatchReport

app = typer.Typer(
    name="bar-bench",
    help="Harness for running Beyond All Reason scenarios on GCP Batch.",
    no_args_is_help=True,
)

DEFAULT_PROJECT = "bar-experiments"
DEFAULT_REGION = "us-central1"
DEFAULT_ARTIFACTS_BUCKET = "gs://bar-experiments-bench-artifacts"
DEFAULT_RESULTS_BUCKET = "gs://bar-experiments-bench-results"
DEFAULT_MACHINE = "c2d-standard-16"
DEFAULT_CATALOG = Path("artifacts.toml")
DEFAULT_BENCHMARKS_DIR = Path("benchmarks")


@app.command("run")
def run_cmd(
    engine: str = typer.Option(..., help="Catalog name of the engine artifact."),
    bar_content: str = typer.Option(..., help="Catalog name of the bar-content artifact."),
    map_: str = typer.Option(..., "--map", help="Catalog name of the map artifact."),
    scenario: str = typer.Option(
        ...,
        help="Folder name under --benchmarks-dir that provides startscript.txt + bar-data/.",
    ),
    description: str | None = typer.Option(
        None,
        "--description",
        "-d",
        help="One-line description of this run (shown in the final report).",
    ),
    catalog: Path = typer.Option(
        DEFAULT_CATALOG, exists=True, dir_okay=False, readable=True,
        help="Path to the artifact catalog (artifacts.toml).",
    ),
    benchmarks_dir: Path = typer.Option(
        DEFAULT_BENCHMARKS_DIR, exists=True, file_okay=False, readable=True,
        help="Root of the scenarios tree (default: benchmarks/).",
    ),
    count: int = typer.Option(20, min=1),
    iterations: int = typer.Option(
        1,
        min=1,
        help="Engine invocations per VM after staging (keeps artifacts cached between runs).",
    ),
    project: str = typer.Option(DEFAULT_PROJECT),
    region: str = typer.Option(DEFAULT_REGION),
    artifacts_bucket: str = typer.Option(DEFAULT_ARTIFACTS_BUCKET),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    machine_type: str = typer.Option(DEFAULT_MACHINE),
    min_cpu_platform: str | None = typer.Option(
        None,
        help="Pin min CPU platform. Unset = auto-derive from --machine-type "
        "(e.g. n1 → 'Intel Skylake', c3d → no pin). Pass '' to force-unset.",
    ),
    max_run_duration: int = typer.Option(1800, min=60, help="Batch task timeout in seconds."),
    service_account: str | None = typer.Option(
        None,
        help="Service account email attached to the Batch VM. "
        "Defaults to benchmark-runner@<project>.iam.gserviceaccount.com.",
    ),
    wheel: Path | None = typer.Option(None, exists=True, dir_okay=False, readable=True),
    report_json: Path | None = typer.Option(
        None,
        "--report-json",
        help="If set, write the structured BatchReport as JSON to this path after the run.",
    ),
) -> None:
    """Submit a benchmark batch and block until all tasks terminate."""
    from bar_benchmarks.orchestrator import run as orchestrator_run

    scenario_dir = benchmarks_dir / scenario
    if not scenario_dir.is_dir():
        raise typer.BadParameter(
            f"scenario folder not found: {scenario_dir}", param_hint="--scenario"
        )
    if not (scenario_dir / "startscript.txt").is_file():
        raise typer.BadParameter(
            f"scenario has no startscript.txt: {scenario_dir}", param_hint="--scenario"
        )

    cfg = BatchConfig(
        engine_name=engine,
        bar_content_name=bar_content,
        map_name=map_,
        scenario_dir=scenario_dir,
        run_description=description,
        catalog_path=catalog,
        count=count,
        iterations=iterations,
        project=project,
        region=region,
        artifacts_bucket=artifacts_bucket,
        results_bucket=results_bucket,
        machine_type=machine_type,
        min_cpu_platform=min_cpu_platform,
        max_run_duration_s=max_run_duration,
        service_account=service_account,
        wheel=wheel,
    )
    orchestrator_run.run(cfg, report_json_path=report_json)


@app.command("stats")
def stats_cmd(
    job_uid: str = typer.Option(..., help="Batch Job UID to aggregate."),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    project: str = typer.Option(DEFAULT_PROJECT),
    submitted: int = typer.Option(
        0, help="Originally-submitted task count; 0 means infer from uploaded results."
    ),
) -> None:
    """Aggregate a completed batch's results from GCS."""
    from bar_benchmarks.stats import aggregate, cost

    report = aggregate.from_bucket(results_bucket, job_uid, submitted=submitted, project=project)
    report = cost.apply_from_batch_api(report, project=project)
    aggregate.print_report(report)


@app.command("lookup")
def lookup_cmd(
    engine: str = typer.Option(..., help="Engine catalog name to match."),
    bar_content: str = typer.Option(..., help="Bar-content catalog name to match."),
    map_: str = typer.Option(..., "--map", help="Map catalog name to match."),
    scenario: str = typer.Option(..., help="Scenario folder name to match."),
    machine_type: str = typer.Option(..., help="Machine type to match."),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    project: str = typer.Option(DEFAULT_PROJECT),
    scan_limit: int = typer.Option(100, min=1, help="Max recent job_uids to scan."),
    min_samples: int = typer.Option(
        50,
        min=1,
        help="Skip the Batch run when the rolling window has at least this many valid results.",
    ),
    report_json: Path = typer.Option(
        ...,
        "--report-json",
        help="On hit, write the rolling-aggregate BatchReport here.",
    ),
) -> None:
    """Aggregate the rolling window of recent matching runs; skip if n ≥ min-samples.

    Pools every `results.json` from the last `--scan-limit` jobs whose
    `(engine, bar_content, map, scenario, machine_type)` matches. If the
    pool has at least `--min-samples` valid results, prints `hit=true`
    and writes the synthesized BatchReport to `--report-json`. Otherwise
    prints `hit=false` and the caller should run a fresh Batch job.

    Prints `hit=true|false` and `job-uid=<id>` on stdout (both suitable
    to append to `$GITHUB_OUTPUT`). Always exits 0; inspect the `hit=`
    line.
    """
    from bar_benchmarks.orchestrator import lookup as lookup_mod
    from bar_benchmarks.stats import cost

    import sys

    shape = (
        f"engine={engine} bar_content={bar_content} map={map_} "
        f"scenario={scenario} machine_type={machine_type}"
    )
    print(
        f"[lookup] scanning up to {scan_limit} recent jobs in {results_bucket} for {shape} "
        f"(min_samples={min_samples})",
        file=sys.stderr,
    )
    report, contributing, hit = lookup_mod.find_rolling_window(
        results_bucket=results_bucket,
        engine=engine,
        bar_content=bar_content,
        map_=map_,
        scenario=scenario,
        machine_type=machine_type,
        min_samples=min_samples,
        scan_limit=scan_limit,
        project=project,
    )
    if not hit:
        print("hit=false")
        return

    report = cost.apply_cached(report)
    report_json.write_text(report.model_dump_json(indent=2))
    print(
        f"[lookup] wrote rolling BatchReport → {report_json} "
        f"(valid={report.valid} invalid={report.invalid} jobs={len(contributing)})",
        file=sys.stderr,
    )
    print("hit=true")
    print(f"job-uid={report.job_uid}")


@app.command("compare")
def compare_cmd(
    candidate: Path = typer.Option(
        ..., "--candidate", exists=True, dir_okay=False, readable=True,
        help="Path to the candidate BatchReport JSON.",
    ),
    baseline: Path = typer.Option(
        ..., "--baseline", exists=True, dir_okay=False, readable=True,
        help="Path to the baseline BatchReport JSON.",
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o",
        help="If set, write the ComparisonReport as JSON to this path.",
    ),
    alpha: float = typer.Option(
        0.05, min=1e-6, max=0.5,
        help="Two-sided significance level; CI confidence is (1 - alpha).",
    ),
) -> None:
    """BCa bootstrap comparison of two BatchReports.

    Emits a CI on the difference of per-VM sim trimmed means
    (candidate − baseline), rescaled to percent of the trimmed baseline.
    Trim is 20% per side when min(n) ≤ 20, else 10%.
    """
    from bar_benchmarks.stats import compare as compare_mod

    cand_report = BatchReport.model_validate_json(candidate.read_text())
    base_report = BatchReport.model_validate_json(baseline.read_text())
    cmp = compare_mod.compare(cand_report, base_report, alpha=alpha)
    compare_mod.print_comparison(cmp)
    if output is not None:
        output.write_text(cmp.model_dump_json(indent=2))


@app.command("plot")
def plot_cmd(
    candidate: Path = typer.Option(
        ..., "--candidate", exists=True, dir_okay=False, readable=True,
        help="Path to the candidate BatchReport JSON.",
    ),
    baseline: Path = typer.Option(
        ..., "--baseline", exists=True, dir_okay=False, readable=True,
        help="Path to the baseline BatchReport JSON.",
    ),
    output: Path = typer.Option(
        ..., "--output", "-o",
        help="Output path; extension picks the format (.png, .svg, .html).",
    ),
    label_a: str | None = typer.Option(
        None, "--label-a",
        help="Label for the candidate row (default: candidate job_uid).",
    ),
    label_b: str | None = typer.Option(
        None, "--label-b",
        help="Label for the baseline row (default: baseline job_uid).",
    ),
    x_title: str = typer.Option("sim mean (ms)", "--x-title"),
    title: str | None = typer.Option(
        None, "--title",
        help="Chart title; defaults to a generic description.",
    ),
) -> None:
    """Render a horizontal box plot comparing two BatchReports.

    Each per-VM `mean_ms` becomes one sample; whiskers span the full
    range and raw samples overlay as dots. Requires the `plot` extra:
    `pip install bar-benchmarks[plot]`.
    """
    try:
        from bar_benchmarks.stats.plot import boxplot_compare
    except ModuleNotFoundError as e:
        raise typer.BadParameter(
            f"missing optional dep '{e.name}'. Install with: "
            "pip install 'bar-benchmarks[plot]' (or `uv sync --extra plot`)."
        ) from e

    cand_report = BatchReport.model_validate_json(candidate.read_text())
    base_report = BatchReport.model_validate_json(baseline.read_text())

    cand = [p.mean_ms for p in cand_report.per_vm]
    base = [p.mean_ms for p in base_report.per_vm]

    chart = boxplot_compare(
        cand,
        base,
        label_a=label_a or cand_report.job_uid,
        label_b=label_b or base_report.job_uid,
        x_title=x_title,
        title=title or "candidate vs baseline",
    )
    chart.save(str(output))
    typer.echo(f"wrote {output}")
