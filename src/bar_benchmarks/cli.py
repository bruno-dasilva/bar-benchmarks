from __future__ import annotations

from pathlib import Path

import typer

from bar_benchmarks.types import BatchConfig

app = typer.Typer(
    name="bar-bench",
    help="Harness for running Beyond All Reason scenarios on GCP Batch.",
    no_args_is_help=True,
)

DEFAULT_PROJECT = "bar-experiments"
DEFAULT_REGION = "us-central1"
DEFAULT_ARTIFACTS_BUCKET = "gs://bar-experiments-bench-artifacts"
DEFAULT_RESULTS_BUCKET = "gs://bar-experiments-bench-results"
DEFAULT_MACHINE = "n1-standard-8"
DEFAULT_CATALOG = Path("scripts/artifacts.toml")
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
    catalog: Path = typer.Option(
        DEFAULT_CATALOG, exists=True, dir_okay=False, readable=True,
        help="Path to the artifact catalog (scripts/artifacts.toml).",
    ),
    benchmarks_dir: Path = typer.Option(
        DEFAULT_BENCHMARKS_DIR, exists=True, file_okay=False, readable=True,
        help="Root of the scenarios tree (default: benchmarks/).",
    ),
    count: int = typer.Option(20, min=1),
    project: str = typer.Option(DEFAULT_PROJECT),
    region: str = typer.Option(DEFAULT_REGION),
    artifacts_bucket: str = typer.Option(DEFAULT_ARTIFACTS_BUCKET),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    machine_type: str = typer.Option(DEFAULT_MACHINE),
    min_cpu_platform: str | None = typer.Option(
        None,
        help="Pin min CPU platform (e.g. 'Intel Skylake'). Leave unset for AMD "
        "families like c3d, which reject Intel platform constraints.",
    ),
    max_run_duration: int = typer.Option(1800, min=60, help="Batch task timeout in seconds."),
    service_account: str | None = typer.Option(
        None,
        help="Service account email attached to the Batch VM. "
        "Defaults to benchmark-runner@<project>.iam.gserviceaccount.com.",
    ),
    wheel: Path | None = typer.Option(None, exists=True, dir_okay=False, readable=True),
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
        catalog_path=catalog,
        count=count,
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
    orchestrator_run.run(cfg)


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
