# CLAUDE.md

Guidance for Claude Code sessions working in this repo. Read this before
touching code or proposing design changes. For component-level design
(topology diagram, state machine, interfaces, failure modes) see
[ARCHITECTURE.md](./ARCHITECTURE.md).

## Project summary

`bar-benchmarks` is a harness for running Beyond All Reason (BAR) game
scenarios across multiple ephemeral cloud VMs and collecting repeatable
performance measurements. Given five artifacts (content distribution, Lua
overlay, custom engine, map, startscript) it spawns N identically-specced VMs,
runs a pre-flight sanity microbenchmark on each, runs the scenario, monitors
for environmental "poisons" that would contaminate results, uploads a
`results.json` per VM, and aggregates basic stats over the valid runs. See
[README.md](./README.md) for the user-facing description.

The MVP harness (orchestrator, batch-submitter, task pipeline, collector,
stats, CLI) is scaffolded under `src/bar_benchmarks/`. **Preflight and
poison-monitor are stubs**; cloud smoke against a real GCP project is the
next milestone.

## Engine execution

The thing under test is [RecoilEngine](https://github.com/beyond-all-reason/RecoilEngine)
(C++), shipped as `engine.tar.gz` containing the `spring-headless` binary.
On each VM:

- BAR game content (a directory, not a zip — Spring's `.sdd` format) is
  extracted from `bar-content.tar.gz` to `/var/bar-data/games/BAR.sdd/`.
  The `VERSION` file there binds it to `Beyond-All-Reason-<VERSION>` for
  startscript matching.
- The overlay (`overlay.tar.gz`) is extracted on top of `/var/bar-data/`.
  Files under `games/BAR.sdd/` inside the tarball land in `BAR.sdd/`,
  overwriting or adding to the game content (this is how benchmarking
  widgets replace engine-side Lua). Files at other paths drop extras
  into `/var/bar-data/` (e.g. a `benchmark_snapshot.lua` that the
  overlay's Lua code loads via VFS).
- The map archive goes into `/var/bar-data/maps/`.
- The engine is invoked as
  `spring-headless --isolation --write-dir /var/bar-data <startscript>`.
- Benchmark output is produced by the overlay's Lua code, written to a
  JSON file under `--write-dir` (default
  `/var/bar-data/benchmark-results.json`, configurable via env).

Full on-disk layout, step-by-step runner flow, and failure modes live in
[ARCHITECTURE.md § Engine runtime](./ARCHITECTURE.md#engine-runtime).

## Language and tooling

- **Python 3.11+** for both control-host (orchestrator, stats) and VM-side
  (task script, poison-monitor) code. Single language across both zones.
- **Dependency management:** [`uv`](https://docs.astral.sh/uv/), with
  `pyproject.toml` + `uv.lock`. Lockfile is committed.
- **GCP SDKs:** `google-cloud-batch` (job submission + polling) and
  `google-cloud-storage` (artifact staging, results read-back). Task-side
  GCS reads go through the Batch Cloud Storage FUSE mount, not the SDK.
- **Stats aggregation:** Python stdlib `statistics` (mean / median / pstdev).
  Reach for `numpy` only if a specific metric needs it.
- **Batch VM images** (`batch-debian`, `batch-hpc-rocky`) ship Python 3.11,
  so no runtime bootstrap is needed on the VM.

## Design principles

- **Ephemeral infrastructure.** Every batch provisions fresh VMs and tears
  them down at the end. No long-lived benchmark hosts.
- **Identical specs per batch.** All VMs in a batch share instance type,
  image, and region class. Cross-batch comparison is only meaningful when
  specs match.
- **Fail-closed on poison.** A run whose environment tripped a poison
  threshold is dropped from the aggregate. The harness does not retry,
  re-weight, or otherwise try to "recover" a poisoned run.
- **Artifacts are opaque inputs.** The harness stages and hashes the five
  input artifacts but never modifies or repacks them.
- **Invalidation is visible.** Invalid runs still produce a `results.json` —
  they are flagged, not discarded silently, so the operator can see how many
  VMs were lost to noise.

## Architecture sketch

Execution platform is **GCP Batch**. The orchestrator submits a Batch Job;
Batch owns VM spawn, logging, and teardown. There is no hand-rolled
provisioner. Full design in [ARCHITECTURE.md](./ARCHITECTURE.md).

- **`orchestrator`** — builds and submits the Batch Job, polls until all
  Tasks terminal, reconciles uploaded results against the submitted task
  count.
- **`batch-submitter`** — thin layer that translates a batch config into a
  Batch Job spec (allocation policy, volumes, runnables).
- **`preflight`** — first step of the Task script; microbenchmark + spec
  check. On failure the Task continues to the collector (which uploads an
  invalid-flagged `results.json`).
- **`runner`** — second step of the Task script; extracts the engine
  tarball to `/opt/recoil/`, stages `BAR.sdd` + merges the overlay, copies
  the map, and invokes `spring-headless --isolation --write-dir`.
- **`poison-monitor`** — Batch background runnable (`background: true`,
  `alwaysRun: true`); samples CPU steal etc. and writes a rolling summary
  the collector merges in.
- **`collector`** — final `alwaysRun` runnable in the Task; writes the
  per-task `results.json` to a mounted results GCS bucket.
- **`stats`** — post-hoc aggregation over valid runs; emits the batch report.

## Invariants

- All VMs in a batch use the same spec.
- One scenario per batch (one `startscript.txt`, one engine, one overlay, one
  map, one content distribution).
- Poisoned runs never contribute to aggregate statistics.
- The harness does not mutate input artifacts.

## Non-goals

- Not a BAR client or matchmaker.
- No GUI.
- No persistent infrastructure, no shared benchmarking fleet.
- Not trying to reproduce real-match network conditions — this is a
  single-VM, scenario-driven harness.

## Open design questions

These are **not decided** yet. When work starts on a component that depends
on one of these, ask the user rather than assuming:

- **Results bucket layout.** The key scheme under the results GCS bucket
  (`<job_uid>/<task_index>/results.json` is the current sketch) and the
  retention policy.
- **Runtime-dependency set for `spring-headless`.** Which system packages
  the engine needs beyond the default `batch-debian` image. Strategy is
  discover-then-freeze: start from stock, capture missing-lib errors on
  the first real run, install those via an apt step (or bake a custom
  image), then pin the set. Early Tasks will fail with
  `missing_runtime_deps` until this settles.
- **Pre-flight microbenchmark.** Off-the-shelf (sysbench, stress-ng), a
  custom CPU/memory probe, or a short BAR-engine warm-up against a canned
  scenario.
- **Full poison signal set.** CPU steal % is the canonical example; the rest
  (context switches, thermal throttling, memory pressure, network jitter,
  disk latency) and their thresholds need calibration from real runs.

## Directory layout

```
pyproject.toml
uv.lock
src/bar_benchmarks/
  orchestrator/   # Batch job build + submit + poll + reconcile
  task/           # preflight, runner, collector (Python entrypoints)
  poison/         # poison-monitor (background runnable)
  stats/          # aggregate batch results
  paths.py        # BAR_* env-var resolution shared by task and orchestrator
  cli.py          # `bar-bench` Typer entrypoint
tests/
scripts/          # dev-side iteration tooling — see § Local iteration tooling
benchmarks/       # scenario folders consumed by fake-runner --scenario NAME
  <name>/
    startscript.txt
    bar-data/     # tarred into overlay.tar.gz; extracts onto /var/bar-data/
```

## Local iteration tooling

Five helpers in `scripts/` exercise the task-side pipeline without
round-tripping through GCP Batch.

**Builders** (produce local artifact tarballs from upstream sources):

- `scripts/build-engine.sh --commit SHA --output FILE` — pulls the latest
  successful "Build Engine v2" GitHub Actions run for a RecoilEngine
  commit, extracts its amd64-linux artifact (zip → inner 7z), and repacks
  the install tree as `engine.tar.gz`. Requires `gh` (authenticated),
  `7z`/`7zz`, `unzip`, `tar`. Caches per-commit under `.smoke/engine-build/`.
- `scripts/build-bar-content.sh --version "Beyond All Reason test-<build>-<sha>" --output FILE` —
  clones `beyond-all-reason/Beyond-All-Reason`, checks out `<sha>`, writes
  a matching `VERSION` file at the clone root, and tars the tree.
  Persistent clone at `.smoke/bar-content-build/Beyond-All-Reason/`.
- `scripts/build-startscript.sh --template FILE --version X [--map Y] --output FILE` —
  patches the `GameType=` and (optionally) `Mapname=` lines of a template
  startscript so they stay in sync with a bar-content build.

**Publisher + runner**:

- `scripts/fake-orchestrator.sh --engine|--bar-content|--map NAME [FILE]`
  publishes to the gs:// URI the catalog at
  `scripts/artifacts.toml` binds that name to. Map entries shaped
  `{source = "https://...", dest = "gs://..."}` mirror on demand (no FILE).
- `scripts/fake-runner.sh --engine NAME --bar-content NAME --map NAME --scenario NAME`
  runs the task-side Python pipeline in a Docker container impersonating
  a Batch VM. Engine/bar-content/map are catalog-resolved; the scenario
  is a repo-local folder under `benchmarks/<name>/` whose `startscript.txt`
  is used verbatim and whose `bar-data/` is tarred into `overlay.tar.gz`
  on the fly.

The catalog is the **shared source of truth** for artifact identity:
both `fake-runner.sh` and the production orchestrator
(`bar-bench run --engine NAME --bar-content NAME --map NAME --scenario
NAME`) look up names in `scripts/artifacts.toml`, resolve them to
`dest` gs:// URIs, and stage the referenced blobs. On a cache miss
(the bucket doesn't have the blob yet), the production orchestrator
shells out to `scripts/build-engine.sh` (using the entry's `commit`),
`scripts/build-bar-content.sh` (using `version`), or curl (using the
map entry's `source` URL) to materialize the tarball locally, then
uploads it to the catalog's `dest`. Names — not content hashes — drive
the cache check so the orchestrator can skip the build before
touching local disk.

Bucket layout the orchestrator writes:

```
gs://<artifacts-bucket>/
    engine/<name>.tar.gz                (shared; e.g. recoil-5c157c8-perf-wins)
    bar-content/<name>.tar.gz           (shared; e.g. bar-test-29871-90f4bc1)
    maps/<map_filename>                 (shared; basename from catalog dest URI)
    <job_uid>/
        overlay.tar.gz
        startscript.txt
        bar_benchmarks-<ver>-py3-none-any.whl
        manifest.json
```

The task VM gets two FUSE mounts:

- `/mnt/artifacts` → `<artifacts_bucket>/<job_uid>/` — per-job tree.
- `/mnt/artifacts-bucket` → `<artifacts_bucket>/` — bucket root.
  The runner resolves engine/bar-content/map under this mount using
  the `paths` block in `manifest.json`.

`fake-runner.sh` impersonates a Batch Task VM: it builds the
`bar_benchmarks` wheel into the per-job subtree, stages the artifacts
into the same bucket-layout tree, then runs
`batch_submitter.BOOTSTRAP_SCRIPT` (extracted verbatim from the Python
module) followed by `python3 -m bar_benchmarks.task.main` inside the
same Artifact Registry image the real Batch Job pulls
(`us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:<tag>`,
built from `scripts/batch-runtime.Dockerfile`). Mounts place
`$workdir/{bucket,bucket/fake-runner-local,results,data,run,engine}`
at the canonical Batch paths (`/mnt/artifacts-bucket`,
`/mnt/artifacts`, `/mnt/results`, `/var/bar-data`, `/var/bar-run`,
`/opt/recoil`). On Apple Silicon this requires Docker Desktop with
Rosetta enabled (Settings → General → "Use Rosetta for x86_64/amd64
emulation") so the amd64 `spring-headless` binary can execute.
Downloaded artifacts are cached at
`.smoke/fake-runner/cache/<bucket>/<key>` so re-runs skip the network.

When the runtime image needs to change (e.g. spring-headless surfaces
a missing lib, or a new Python dep should be pre-baked): edit
`scripts/batch-runtime.Dockerfile`, run `scripts/build-batch-runtime.sh`
to push a new tag to AR, and bump the tag in **both**
`batch_submitter.CONTAINER_IMAGE` and `fake-runner.sh:IMAGE_TAG` in
the same commit — the two consumers must stay in lockstep.

Example invocation (engine/bar-content/map are catalog names; scenario
is `benchmarks/lategame1/`):

```
scripts/fake-runner.sh \
    --engine recoil-5c157c8-perf-wins \
    --bar-content bar-test-29871-90f4bc1 \
    --map hellas-basin-v1.4 \
    --scenario lategame1
```
