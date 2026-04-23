# bar-benchmarks

Run [Beyond All Reason](https://www.beyondallreason.info/) game scenarios across
multiple ephemeral cloud VMs for repeatable performance measurement.

> **Status:** MVP scaffold implemented. Orchestrator, runner, collector,
> stats, and CLI are real. Cloud smoke-test against a real GCP project
> is the next step — see "Prerequisites" below. For component-level
> design see [ARCHITECTURE.md](./ARCHITECTURE.md); for per-invocation
> cost math see [COSTS.md](./COSTS.md); for agent guidance and open
> questions see [CLAUDE.md](./CLAUDE.md).

## What it does

Given a fixed set of BAR artifacts and a scenario, `bar-benchmarks` submits a
[GCP Batch](https://cloud.google.com/batch/docs) job that spins up a
configurable number of identically-specced VMs (one per run), runs the scenario
on each, and collects per-VM timing data into `results.json` files in a GCS
bucket. After all Tasks finish, the results are aggregated into basic
summary stats.

## Input artifacts

Every benchmark batch takes the same five artifacts, uploaded to a GCS
bucket and mounted read-only into each VM:

1. **`engine.tar.gz`** — tarball of the [RecoilEngine](https://github.com/beyond-all-reason/RecoilEngine)
   build tree. Provides the `spring-headless` binary plus its shared libs.
2. **`bar-content.tar.gz`** — tarball of the
   [Beyond-All-Reason/Beyond-All-Reason](https://github.com/beyond-all-reason/Beyond-All-Reason)
   git checkout at a specific commit. Populates `games/BAR.sdd/` on the
   VM; its `VERSION` file binds the archive to `Beyond-All-Reason-<VERSION>`.
3. **`overlay.tar.gz`** — tarball of scenario-specific files that extend the
   engine's write-dir. Extracted onto `/var/bar-data/`: anything under
   `games/BAR.sdd/` overrides/adds to the base game content (this is how
   benchmarking widgets replace engine-side Lua); anything at other paths
   drops extras into the write-dir for the engine to load via VFS (e.g. a
   `benchmark_snapshot.lua` used by a replay gadget).
4. **Map archive** — raw Spring map file (e.g. `<name>.sd7`). Placed in
   `maps/`.
5. **`startscript.txt`** — the scenario definition (teams, units, AI,
   seed, duration, etc.) passed to the engine. References the map name and
   the matching `Beyond-All-Reason-<VERSION>`.

The harness treats these as opaque inputs. It does not repack or mutate
them. On local iteration (see below), overlay and startscript are
synthesized from a repo-local `benchmarks/<scenario>/` folder, but the
production contract is still the five artifacts above.

## Run lifecycle

For each batch:

1. **Stage artifacts** — validate the five inputs exist and upload them to a
   location the VMs can pull from.
2. **Spawn VMs** — provision N cloud VMs with identical instance type, image,
   and region class.
3. **Run scenario** — each VM extracts the engine tarball to
   `/opt/recoil/`, stages `BAR.sdd` under `/var/bar-data/games/`, extracts
   the overlay on top of `/var/bar-data/` (which overrides `BAR.sdd/`
   content and drops any extra files alongside), places the map under
   `/var/bar-data/maps/`, and invokes:
   `spring-headless --isolation --write-dir /var/bar-data <startscript>`.
   Benchmark data is written by the overlay to a JSON file inside the
   write-dir.
4. **Collect results** — each VM writes a `results.json` and uploads it to the
   batch's results location. Invalid runs still upload, but flagged as such.
5. **Teardown** — all VMs are destroyed. No persistent infra.
6. **Aggregate stats** — a post-processing step parses the valid `results.json`
   files and emits summary statistics for the batch.

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

## Building input artifacts

Two helpers in `scripts/` build the engine and bar-content tarballs from
upstream sources. The production orchestrator invokes them automatically
on a cache miss (see the `[engine]` / `[bar_content]` entries in
[`artifacts.toml`](./artifacts.toml)); they can also be
run by hand to prime the cache.

- **`scripts/build-engine.sh --commit SHA --output FILE`** — pulls the
  latest successful "Build Engine v2" GitHub Actions run for the given
  RecoilEngine commit, downloads its `engine-artifacts-amd64-linux-*`
  artifact, extracts the inner `.7z`, and repacks the install tree as
  `engine.tar.gz` with `spring-headless` at the root. Requires `gh`
  (authenticated), `7z`/`7zz`, `unzip`, `tar`. Caches per-commit under
  `.smoke/engine-build/<sha>/`.
- **`scripts/build-bar-content.sh --version "Beyond All Reason test-<build>-<sha>" --output FILE`** —
  clones `beyond-all-reason/Beyond-All-Reason` (persistent cache at
  `.smoke/bar-content-build/Beyond-All-Reason/`), checks out `<sha>`,
  writes a matching `VERSION` file at the clone root, and tars the tree
  as `bar-content.tar.gz`.

### Scenario folders

Each benchmark scenario is a subdirectory of `benchmarks/`:

```
benchmarks/<name>/
  startscript.txt                  # passed verbatim to spring-headless
  bar-data/                        # the overlay tree
    <extras>.lua                   # drops into /var/bar-data/ (loaded via VFS)
    games/BAR.sdd/
      luarules/gadgets/<bench>.lua # overrides/adds to the base bar-content
```

The orchestrator tars `scenario/bar-data/` into the per-job
`overlay.tar.gz` on the fly and uploads `scenario/startscript.txt` verbatim.

### End-to-end example

Using the `benchmarks/lategame1/` scenario, RecoilEngine commit
`5c157c84bf11cfeadadade183f373b03cdb9fb7a`, BAR commit `90f4bc1`, and map
`hellas-basin-v1.4`, all registered in `artifacts.toml`:

```bash
uv run bar-bench run \
    --engine      recoil-5c157c8-perf-wins \
    --bar-content bar-test-29871-90f4bc1 \
    --map         hellas-basin-v1.4 \
    --scenario    lategame1 \
    --count       10
```

The orchestrator resolves each name against the catalog, builds+uploads
missing artifacts on demand (engine via `scripts/build-engine.sh`,
bar-content via `scripts/build-bar-content.sh`, map via curl against the
entry's `source` URL), submits a Batch Job, and blocks until every Task
is terminal. See [`run_benchmarks.sh`](./run_benchmarks.sh) for a
multi-scenario baseline.

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
