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
DEFAULT_REGION = "us-west4"
DEFAULT_ARTIFACTS_BUCKET = "gs://bar-experiments-bench-artifacts"
DEFAULT_RESULTS_BUCKET = "gs://bar-experiments-bench-results"
DEFAULT_MACHINE = "n1-standard-8"


@app.command("run")
def run_cmd(
    engine: Path = typer.Option(..., exists=True, dir_okay=False, readable=True),
    bar_content: Path = typer.Option(..., exists=True, dir_okay=False, readable=True),
    overlay: Path = typer.Option(..., exists=True, dir_okay=False, readable=True),
    map_: Path = typer.Option(..., "--map", exists=True, dir_okay=False, readable=True),
    startscript: Path = typer.Option(..., exists=True, dir_okay=False, readable=True),
    count: int = typer.Option(20, min=1),
    project: str = typer.Option(DEFAULT_PROJECT),
    region: str = typer.Option(DEFAULT_REGION),
    artifacts_bucket: str = typer.Option(DEFAULT_ARTIFACTS_BUCKET),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    machine_type: str = typer.Option(DEFAULT_MACHINE),
    max_run_duration: int = typer.Option(1800, min=60, help="Batch task timeout in seconds."),
    wheel: Path | None = typer.Option(None, exists=True, dir_okay=False, readable=True),
) -> None:
    """Submit a benchmark batch and block until all tasks terminate."""
    from bar_benchmarks.orchestrator import run as orchestrator_run

    cfg = BatchConfig(
        engine=engine,
        bar_content=bar_content,
        overlay=overlay,
        map=map_,
        startscript=startscript,
        count=count,
        project=project,
        region=region,
        artifacts_bucket=artifacts_bucket,
        results_bucket=results_bucket,
        machine_type=machine_type,
        max_run_duration_s=max_run_duration,
        wheel=wheel,
    )
    orchestrator_run.run(cfg)


@app.command("stats")
def stats_cmd(
    job_uid: str = typer.Option(..., help="Batch Job UID to aggregate."),
    results_bucket: str = typer.Option(DEFAULT_RESULTS_BUCKET),
    submitted: int = typer.Option(
        0, help="Originally-submitted task count; 0 means infer from uploaded results."
    ),
) -> None:
    """Aggregate a completed batch's results from GCS."""
    from bar_benchmarks.stats import aggregate

    report = aggregate.from_bucket(results_bucket, job_uid, submitted=submitted)
    aggregate.print_report(report)
