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
    from bar_benchmarks.stats import aggregate

    report = aggregate.from_bucket(results_bucket, job_uid, submitted=submitted, project=project)
    aggregate.print_report(report)


@app.command("lookup")
def lookup_cmd(
    engine: str = typer.Option(..., help="Engine catalog name to match."),
    bar_content: str = typer.Option(..., help="Bar-content catalog name to match."),
    map_: str = typer.Option(..., "--map", help="Map catalog name to match."),
    scenario: str = typer.Option(..., help="Scenario folder name to match."),
    count: int = typer.Option(..., min=1, help="VM count to match."),
    iterations: int = typer.Option(
        1,
        min=1,
        help="Per-VM iteration count to match. Defaults to 1 so pre-iterations runs match.",
    ),
    machine_type: str = typer.Option(..., help="Machine type to match."),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    project: str = typer.Option(DEFAULT_PROJECT),
    scan_limit: int = typer.Option(100, min=1, help="Max recent run.json blobs to scan."),
    report_json: Path = typer.Option(
        ...,
        "--report-json",
        help="On hit, write the aggregated BatchReport here.",
    ),
) -> None:
    """Look for a prior run matching these parameters; on hit, re-aggregate it.

    Prints `hit=true|false` and `job-uid=<id>` on stdout (both suitable to
    append to $GITHUB_OUTPUT). Always exits 0; inspect the `hit=` line.
    """
    from bar_benchmarks.orchestrator import lookup as lookup_mod
    from bar_benchmarks.stats import aggregate

    import sys

    shape = (
        f"engine={engine} bar_content={bar_content} map={map_} "
        f"scenario={scenario} count={count} iterations={iterations} "
        f"machine_type={machine_type}"
    )
    print(
        f"[lookup] scanning up to {scan_limit} recent runs in {results_bucket} for {shape}",
        file=sys.stderr,
    )
    match = lookup_mod.find_matching_run(
        results_bucket=results_bucket,
        engine=engine,
        bar_content=bar_content,
        map_=map_,
        scenario=scenario,
        count=count,
        iterations=iterations,
        machine_type=machine_type,
        scan_limit=scan_limit,
        project=project,
    )
    if match is None:
        print(
            f"[lookup] cache miss — no prior run matched; a fresh Batch job will be submitted",
            file=sys.stderr,
        )
        print("hit=false")
        return

    job_uid = match["job_uid"]
    submitted_at = match.get("submitted_at") or "unknown"
    print(
        f"[lookup] CACHE HIT — reusing prior run {job_uid} submitted at {submitted_at}; "
        f"re-aggregating its results instead of running a new Batch job",
        file=sys.stderr,
    )
    report = aggregate.from_bucket(
        results_bucket,
        job_uid,
        submitted=count * iterations,
        project=project,
        run_description=match.get("run_description"),
    )
    report_json.write_text(report.model_dump_json(indent=2))
    print(
        f"[lookup] wrote cached BatchReport → {report_json} "
        f"(valid={report.valid} invalid={report.invalid})",
        file=sys.stderr,
    )
    print("hit=true")
    print(f"job-uid={job_uid}")
    if match.get("submitted_at"):
        print(f"submitted-at={match['submitted_at']}")


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
    """Welch's t-test comparison of two BatchReports.

    Emits a 95% CI on the per-VM sim mean difference (candidate − baseline)
    and its rescaling to percent of baseline.
    """
    from bar_benchmarks.stats import compare as compare_mod

    cand_report = BatchReport.model_validate_json(candidate.read_text())
    base_report = BatchReport.model_validate_json(baseline.read_text())
    cmp = compare_mod.compare(cand_report, base_report, alpha=alpha)
    compare_mod.print_comparison(cmp)
    if output is not None:
        output.write_text(cmp.model_dump_json(indent=2))
