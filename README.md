# bar-benchmarks

Run [Beyond All Reason](https://www.beyondallreason.info/) game scenarios across
multiple ephemeral cloud VMs for repeatable performance measurement.

> **Status:** MVP scaffold implemented. The orchestrator, runner, collector,
> stats, and CLI are real; **preflight and poison-monitor are stubs** that
> let the harness run end-to-end without rejecting VMs or invalidating runs.
> Cloud smoke-test against a real GCP project is the next step — see
> "Prerequisites" below. For component-level design see
> [ARCHITECTURE.md](./ARCHITECTURE.md); for per-invocation cost math see
> [COSTS.md](./COSTS.md); for agent guidance and open questions see
> [CLAUDE.md](./CLAUDE.md).

## What it does

Given a fixed set of BAR artifacts and a scenario, `bar-benchmarks` submits a
[GCP Batch](https://cloud.google.com/batch/docs) job that spins up a
configurable number of identically-specced VMs (one per run), runs the scenario
on each, and collects per-VM timing data into `results.json` files in a GCS
bucket. It watches each run for environmental "poisons" (e.g. high CPU steal
from the hypervisor) and invalidates any run whose environment was bad, so the
final statistics reflect only clean runs. After all Tasks finish, the results
are aggregated into basic summary stats.

## Input artifacts

Every benchmark batch takes the same five artifacts, uploaded to a GCS
bucket and mounted read-only into each VM:

1. **`engine.tar.gz`** — tarball of the [RecoilEngine](https://github.com/beyond-all-reason/RecoilEngine)
   build tree. Provides the `spring-headless` binary plus its shared libs.
2. **`bar-content.tar.gz`** — tarball of the
   [Beyond-All-Reason/Beyond-All-Reason](https://github.com/beyond-all-reason/Beyond-All-Reason)
   git checkout at a specific commit. Populates `games/BAR.sdd/` on the
   VM; its `VERSION` file binds the archive to `Beyond-All-Reason-<VERSION>`.
3. **`overlay.tar.gz`** — tarball of extra Lua widgets/gadgets that
   instrument the game for benchmarking. Merged on top of `BAR.sdd/`
   (added or overwritten).
4. **Map archive** — raw Spring map file (e.g. `<name>.sd7`). Placed in
   `maps/`.
5. **`startscript.txt`** — the scenario definition (teams, units, AI,
   seed, duration, etc.) passed to the engine. References the map name and
   the matching `Beyond-All-Reason-<VERSION>`.

The harness treats these as opaque inputs. It does not repack or mutate
them.

## Run lifecycle

For each batch:

1. **Stage artifacts** — validate the five inputs exist and upload them to a
   location the VMs can pull from.
2. **Spawn VMs** — provision N cloud VMs with identical instance type, image,
   and region class.
3. **Pre-flight check** — each VM runs a short microbenchmark against a known
   baseline. VMs that fall outside spec are abandoned (noisy-neighbor filter)
   before the real run starts.
4. **Run scenario** — each surviving VM extracts the engine tarball to
   `/opt/recoil/`, stages `BAR.sdd` + overlay under `/var/bar-data/games/`,
   places the map under `/var/bar-data/maps/`, and invokes:
   `spring-headless --isolation --write-dir /var/bar-data <startscript>`.
   Benchmark data is written by the overlay to a JSON file inside the
   write-dir.
5. **Poison monitoring** — throughout the run, host-level signals (CPU steal,
   etc.) are sampled. If any poison threshold is tripped, the run is marked
   invalid.
6. **Collect results** — each VM writes a `results.json` and uploads it to the
   batch's results location. Invalid runs still upload, but flagged as such.
7. **Teardown** — all VMs are destroyed. No persistent infra.
8. **Aggregate stats** — a post-processing step parses the valid `results.json`
   files and emits summary statistics for the batch.

## Poisons

A "poison" is a signal that the VM's environment — not the code under test —
was the dominant cause of observed performance. A poisoned run is dropped from
the aggregate, not repaired. Canonical example:

- **CPU steal %** — the hypervisor scheduling the instance's vCPUs onto the
  physical host below some fraction of wall time. High steal indicates a noisy
  neighbor on the hypervisor.

The full set of poison signals and thresholds is still being decided — see
[CLAUDE.md](./CLAUDE.md).

## Output

- **Per VM:** a `results.json` with run metadata (artifact hashes, instance
  type, timings, telemetry summary) and a `valid` / `invalid` verdict.
- **Per batch:** an aggregate stats report over the valid runs (mean, median,
  p95, variance, count valid / count invalid).

## Usage

```
uv sync
uv run bar-bench run \
    --engine path/to/engine.tar.gz \
    --bar-content path/to/bar-content.tar.gz \
    --overlay path/to/overlay.tar.gz \
    --map path/to/map.sd7 \
    --startscript path/to/startscript.txt \
    --count 20
uv run bar-bench stats --job-uid <uid>
```

`run` uploads the five artifacts plus the freshly-built `bar_benchmarks`
wheel to `gs://<artifacts-bucket>/<job_uid>/`, submits a Batch Job, blocks
until every Task is terminal, reconciles uploaded results against
submitted task count, and prints an aggregate. Defaults target the
`bar-experiments` GCP project in `us-west4`; override with `--project`,
`--region`, `--artifacts-bucket`, `--results-bucket`, `--machine-type`,
and `--max-run-duration`.

## Prerequisites

One-time setup for the `bar-experiments` project (or whatever `--project`
you point at):

- Create both buckets in `us-west4` (single-region for free same-region
  egress): `gs://bar-experiments-bench-artifacts`,
  `gs://bar-experiments-bench-results`.
- Enable the Batch, Compute Engine, and Cloud Storage APIs.
- Create `benchmark-runner@bar-experiments.iam.gserviceaccount.com` and
  grant it `roles/storage.objectUser` on both buckets plus
  `roles/batch.agentReporter` and `roles/logging.logWriter` at the
  project level.
- `gcloud auth application-default login` locally. The operator identity
  needs `roles/batch.jobsEditor`, `roles/storage.objectUser` on the
  artifacts bucket, and `roles/iam.serviceAccountUser` on the
  `benchmark-runner` service account (required to attach it to the Job).

## Open questions

Not yet decided; tracked in [CLAUDE.md](./CLAUDE.md):

- Runtime-dependency set for `spring-headless` on the VM image
  (discover-then-freeze on first run — expect `missing_runtime_deps`
  until this settles)
- Real pre-flight microbenchmark (currently stubbed to always pass)
- Full poison signal set and thresholds (monitor currently stubbed)
