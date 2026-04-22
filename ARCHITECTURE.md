# Architecture

Detailed design for `bar-benchmarks`. Read [README.md](./README.md) for the
user-facing overview and [CLAUDE.md](./CLAUDE.md) for agent guidance and open
decisions first.

> **Status:** design document. No code exists yet. Execution platform is
> committed to **GCP Batch** and the implementation language is committed
> to **Python 3.11+** (managed with `uv`). The results-store key layout
> and a few other details are still open — see [CLAUDE.md](./CLAUDE.md).

## Execution platform: GCP Batch

All VM lifecycle is delegated to [GCP
Batch](https://cloud.google.com/batch/docs). The orchestrator does not
spawn, SSH into, monitor, or destroy VMs directly — it submits a `Job` and
polls its state. Batch owns:

- Provisioning N identical VMs (`taskGroups[0].taskCount = N`,
  `parallelism = N`) with the configured machine type, CPU platform, boot
  disk, and spot policy.
- Installing the task runtime and mounting the artifacts + results GCS
  buckets into each Task.
- Streaming stdout/stderr to Cloud Logging, tagged with task index.
- Tearing down VMs on Task exit.
- Optional automatic retry of failed Tasks (we disable retry for
  poison-invalidated runs — see Failure modes).

Each **Task** maps 1:1 to one benchmark run on one VM.

## Topology

```
┌────────────────── control host (operator / CI) ──────────────────┐
│                                                                  │
│   artifacts ──► orchestrator ──► (Batch API: submit Job)         │
│                      │                                           │
│                      ▼                                           │
│                    stats ◄──── GCS results bucket                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                             │    ▲
                   Batch job │    │ results.json
                             ▼    │
┌────────────── GCP Batch Task (×N, one VM each) ──────────────────┐
│                                                                  │
│   poison-monitor (background runnable, alwaysRun)                │
│          │                                                       │
│          ▼                                                       │
│   task script: preflight ──► runner ──► engine(startscript.txt)  │
│                                           │                      │
│                                           ▼                      │
│                                      collector ──► GCS           │
│                                                                  │
│   (artifacts + results GCS buckets mounted via Cloud Storage FUSE) │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Components

### `orchestrator` (control host, Python)

- Parses the batch config (artifacts, N, instance spec, poison thresholds).
- Resolves engine / bar-content / map catalog names against
  `scripts/artifacts.toml`, checks whether each blob already lives at
  its content-stable bucket key, and builds+uploads on a miss (via
  `scripts/build-engine.sh` / `scripts/build-bar-content.sh` / curl).
  Uploads the per-job overlay (packed from the scenario) + startscript
  + wheel + manifest under `<job_uid>/`.
- Builds a Batch `Job` spec (see "Batch job shape" below) and submits it
  via `google-cloud-batch`.
- Polls the job until all Tasks reach a terminal state.
- Hands the set of `results.json` object keys to `stats`.
- Does not manage VMs directly — Batch owns the VM lifecycle.

### `batch-submitter` (control host, Python; subcomponent of orchestrator)

- Translates the batch config into a `google.cloud.batch_v1.Job` message.
- Pins the allocation policy: N1 machine type, `minCpuPlatform: "Intel
  Skylake"`, `provisioningModel: SPOT`, 50 GB boot disk.
- Declares GCS volume mounts for the artifacts and results buckets.
- Declares the Task's runnables: background poison-monitor + the main task
  script.
- Kept as a thin layer so swapping to another Batch-style runner later is
  mechanical; not designed as a portable cloud abstraction.

Task-side components are Python modules under `bar_benchmarks.task` and
`bar_benchmarks.poison`, invoked by the Batch runnables as
`python -m bar_benchmarks.task.<name>`.

### `preflight` (Task-side, first step of the task script)

- Runs a short, fixed microbenchmark on the fresh VM before any real work.
- Compares against a stored baseline for that instance type.
- If the VM is outside spec, sets `preflight_passed = false` in the Task's
  local verdict file and skips the runner. The collector still runs so the
  invalid result is uploaded.

### `runner` (Task-side, second step of the task script)

Reads artifacts from the mounted GCS bucket at `/mnt/artifacts/` (Cloud
Storage FUSE — POSIX reads, no fetch code) and stages them on local
disk, then invokes the engine. Concrete steps:

1. Create `/opt/recoil`, `/var/bar-data/{games,maps}`, `/var/bar-run`.
2. Extract `engine.tar.gz` → `/opt/recoil/`.
3. Extract `bar-content.tar.gz` → `/var/bar-data/games/BAR.sdd/`.
4. Extract `overlay.tar.gz` on top of `/var/bar-data/`. The tarball mirrors
   the write-dir: files under `games/BAR.sdd/` override/extend the base
   game content (how benchmarking widgets replace engine-side Lua); files
   at other paths drop extras into the write-dir so the engine picks them
   up via VFS (e.g. a `benchmark_snapshot.lua` alongside the games dir).
5. Copy the map archive into `/var/bar-data/maps/`.
6. Sanity-check `BAR.sdd/VERSION` and the map file exist.
7. Exec `/opt/recoil/spring-headless --isolation --write-dir /var/bar-data
   /mnt/artifacts/startscript.txt`. Capture exit code and wall time;
   stdout/stderr stream to Cloud Logging via Batch.
8. Verify the overlay wrote its benchmark JSON (default path:
   `/var/bar-data/benchmark-results.json`; configurable via env).
9. Write `/var/bar-run/verdict.json` with engine exit code, timings,
   benchmark output path, and any error.

Scenario termination is the operator's responsibility — a correctly
authored `startscript.txt` ends naturally (game-over triggers or scenario
timeout) and the engine exits on its own. Batch's `maxRunDuration` is the
outer safety net.

### `poison-monitor` (Task-side, background runnable)

- Declared as a separate Batch runnable with `background: true` and
  `alwaysRun: true`, so it starts before the task script and keeps running
  even if an earlier step exits non-zero.
- Samples host-visible signals at a fixed interval. Canonical signal: CPU
  steal % from `/proc/stat`. Additional signals are open (see CLAUDE.md).
- Applies threshold logic: sustained breach over a rolling window trips the
  run. Transient spikes do not.
- Writes a rolling poison summary to a known path on the VM; the collector
  reads it at the end.

### `collector` (Task-side, final step of the task script, `alwaysRun: true`)

Merges four inputs into the task's final `results.json`:

- `/var/bar-run/preflight.json` — preflight verdict + microbench numbers.
- `/var/bar-run/verdict.json` — runner summary, engine exit code, timings.
- `/var/bar-run/poison.json` — poison-monitor's rolling summary.
- `/var/bar-data/benchmark-results.json` — the overlay's benchmark output,
  embedded verbatim under the `benchmark` key.

Writes the merged `results.json` to `/mnt/results/` (GCS FUSE) under a
deterministic key (`<job_uid>/<task_index>/results.json`). Runs on every
terminal path — preflight failure, engine crash, poison trip, or clean
success — so invalidation is visible, not silent.

### `stats` (control host)

- Pulls every `results.json` for the batch from the results store.
- Filters to `valid: true` runs.
- Emits batch summary: count valid, count invalid (with reason breakdown),
  and basic statistics (mean, median, p95, variance) over the valid-run
  timings.

## Engine runtime

The thing under test is the [RecoilEngine](https://github.com/beyond-all-reason/RecoilEngine)
build (C++). The binary is `spring-headless` — a headless variant of the
Spring RTS engine that runs scenarios without a display server. It
consumes a `startscript.txt` and produces logs + whatever data the loaded
Lua gadgets/widgets decide to emit.

### On-VM filesystem layout

```
/mnt/artifacts/          (GCS FUSE, read-only)
  engine.tar.gz
  bar-content.tar.gz
  overlay.tar.gz
  <map-file>
  startscript.txt

/mnt/results/            (GCS FUSE, writable, scoped to <job_uid>/<task_index>/)
  results.json           (written by collector at end of task)

/opt/recoil/             (local — extracted engine build)
  spring-headless
  <engine libs + data>

/var/bar-data/           (local — the engine's --write-dir; overlay extracts here)
  games/
    BAR.sdd/             (bar-content extracted; overlay merges under games/BAR.sdd/)
      VERSION            (binds to Beyond-All-Reason-<VERSION>)
      ...
  maps/
    <map-file>
  <overlay extras>       (any non-BAR.sdd files from overlay.tar.gz land here)
  benchmark-results.json (written by overlay at scenario end; path configurable)
  infolog.txt            (engine log)

/var/bar-run/            (local — task scratch)
  preflight.json
  poison.json
  verdict.json
```

### Invocation

```
/opt/recoil/spring-headless \
  --isolation \
  --write-dir /var/bar-data \
  /mnt/artifacts/startscript.txt
```

- `--isolation` — engine ignores any user-level config (`~/.spring/`
  etc.), so behavior depends only on the write-dir and the artifacts.
  Critical for reproducibility on fresh VMs.
- `--write-dir` — the only directory the engine reads data from and
  writes outputs to.
- The startscript path can sit outside the write-dir (we use the read-only
  artifacts mount).

### Benchmark output contract

Benchmarking data is emitted by the overlay's Lua code, not the engine
core. The overlay writes a JSON file under `--write-dir` at a path
configured via env (`BAR_BENCHMARK_OUTPUT_PATH`, default
`benchmark-results.json`). The runner checks for it after the engine
exits; the collector embeds it verbatim in the final `results.json`.

### Runtime dependencies

`spring-headless` needs some set of system libraries (SDL, mesa, libstdc++,
etc.). The exact list is **not yet pinned**. Strategy:

1. Start from the default `batch-debian` image.
2. On the first real run, capture missing-library errors from
   `infolog.txt`.
3. Install the needed packages via an apt step declared on the Batch Job,
   or bake a custom image. Once the set stabilizes, freeze it.

Until then, expect early Tasks to fail with `missing_runtime_deps`.

## Lifecycle

Batch manages the outer states; the task script manages the inner states.

**Batch Task states (Batch-owned):**

```
QUEUED → SCHEDULED → RUNNING → SUCCEEDED
                        │   └─► FAILED
                        └─────► CANCELLED
```

**Inner states within a RUNNING Task (task-script-owned):**

```
poison-monitor starts (background)
        │
        ▼
 preflight ──► runner ──► collector
     │             │          ▲
     ▼             ▼          │
 preflight_failed poisoned ───┘
                (collector still runs and uploads; alwaysRun)
```

A Task always terminates with a `results.json` in GCS unless the VM was
lost (Batch FAILED / CANCELLED before the collector ran). The orchestrator
reconciles the submitted task count against uploaded results: any missing
task index is recorded as `infrastructure_failure`.

Per-task timeout is set on the Batch Job. Exceeding it transitions the
task to FAILED and the collector may not have run; that index is reconciled
as `timeout`.

## Data shapes

Sketch only; exact schema is TBD.

**`results.json`** (one per VM):

```
{
  "batch_id":       "...",
  "vm_id":          "...",
  "instance_type":  "...",
  "region":         "...",
  "artifact_names": {
    "engine":        "recoil-5c157c8-perf-wins",
    "bar_content":   "bar-test-29871-90f4bc1",
    "map":           "hellas-basin-v1.4"
  },
  "preflight": {
    "passed":       true,
    "microbench":   { ... }
  },
  "run": {
    "started_at":   "...",
    "ended_at":     "...",
    "engine_exit":  0,
    "timings":      { ... }
  },
  "benchmark":      { ... },        // overlay-emitted JSON, embedded verbatim
  "poison": {
    "tripped":      false,
    "signals":      { "cpu_steal_pct_max": 0.4, "cpu_steal_pct_mean": 0.1, ... }
  },
  "valid":          true,
  "invalid_reason": null
}
```

**Batch summary** (one per batch, produced by `stats`): counts, mean/median/
p95/variance over the chosen primary metric from `run.timings`, and a
breakdown of invalid reasons.

## Batch job shape

Sketch of the submitted `Job` (exact field names follow the Batch API):

```
job:
  taskGroups:
    - taskCount: N
      parallelism: N
      taskSpec:
        maxRunDuration: <job timeout, e.g. 1800s>
        volumes:
          - gcs: { remotePath: "<artifacts-bucket>" }
            mountPath: /mnt/artifacts
          - gcs: { remotePath: "<results-bucket>/<job-uid>" }
            mountPath: /mnt/results
        runnables:
          - background: true
            alwaysRun: true
            script: { text: "python3 -m bar_benchmarks.poison.monitor" }
          - script: { text: "python3 -m bar_benchmarks.task.main" }   # preflight → runner
          - alwaysRun: true
            script: { text: "python3 -m bar_benchmarks.task.collector" }
  allocationPolicy:
    instances:
      - policy:
          machineType: n1-standard-<K>
          minCpuPlatform: "Intel Skylake"
          provisioningModel: SPOT
          bootDisk: { sizeGb: 50, type: "pd-balanced" }
  logsPolicy: { destination: CLOUD_LOGGING }
```

Retry policy is **off** by default. A failed Task does not auto-retry,
because most failure causes (poison, engine crash) are not fixed by
rerunning on a different VM within the same batch — they're data points.
The orchestrator can choose to top up a batch with a second Job if too
many invalid runs came back.

## Failure modes

| Failure                          | Detected by         | Handling                                 |
|----------------------------------|---------------------|------------------------------------------|
| Batch Task scheduling failure    | Batch               | Task ends FAILED with no upload. Orchestrator records slot as `infrastructure_failure` during reconciliation. |
| Spot preemption mid-run          | Batch               | Task terminates; collector may not run. Slot recorded as `preempted` if no `results.json` lands. |
| Preflight outside spec           | preflight step      | Task script skips runner, collector uploads with `preflight_failed`. |
| Missing runtime lib (`spring-headless` fails at startup) | runner step | Engine exits immediately; collector uploads with `missing_runtime_deps`. Common on early runs before the dep set is frozen. |
| BAR version / startscript mismatch | runner step       | Engine exits with content-not-found error; collector uploads with `bar_version_mismatch`. |
| Engine crashes / non-zero exit   | runner step         | Task script captures exit code, collector uploads with `engine_crash`. |
| Overlay didn't write benchmark JSON | runner step      | Engine exited cleanly but the expected output file is missing; collector uploads with `overlay_output_missing`. |
| Scenario never terminates        | Batch (maxRunDuration) | Task FAILED by timeout. If collector ran, slot flagged `scenario_never_terminated`; otherwise reconciled as `timeout`. |
| Poison threshold tripped         | poison-monitor      | Monitor writes tripped state; collector uploads with `poisoned`. |
| Results bucket write fails       | collector           | Retried in-task. If still failing, Task exits non-zero with no upload; orchestrator logs `collector_failure`. |

## Interfaces to pin down

1. **Results store layout** — object key scheme under the results bucket
   (`<job_uid>/<task_index>/results.json`) and the retention policy.
2. **Poison signal interface** — each signal is `{ name, sample() → float,
   threshold, window }`. Adding a signal is one file.
3. **Task script contract** — the environment variables and mounted paths
   Batch exposes to the three runnables (`BATCH_JOB_UID`, `BATCH_TASK_INDEX`,
   `/mnt/artifacts`, `/mnt/results`). These become the only coupling between
   control-host code and VM-side code.
