#!/usr/bin/env bash
# Run the task-side pipeline (preflight + runner from src/bar_benchmarks/task/)
# locally in a linux/amd64 Docker container that *impersonates* a GCP Batch
# Task VM. The container uses the same canonical paths (/mnt/artifacts,
# /mnt/results, /var/bar-data, /var/bar-run, /opt/recoil), the same BAR_*
# env vars, and the same bootstrap (apt + pip install wheel) that
# orchestrator/batch_submitter.py runs on real Batch Tasks — so a passing
# fake-runner implies the Batch path will work. Amd64 emulation is a side
# benefit: the Linux amd64 spring-headless binary can actually execute on
# an arm64 Mac host (requires Docker Desktop with Rosetta enabled).
#
# Pick artifacts by name from scripts/artifacts.toml (the artifact catalog).
# Each name resolves to a gs:// URI and is independent of any job submission.
# Use scripts/fake-orchestrator.sh to publish a new artifact under a name.

set -euo pipefail

DEFAULT_CATALOG="scripts/artifacts.toml"
DEFAULT_WORKDIR=".smoke/fake-runner"
IMAGE_TAG="bar-benchmarks/fake-runner:dev"

usage() {
  cat >&2 <<'EOF'
Usage:
  fake-runner.sh --engine NAME --bar-content NAME --map NAME \
                 --scenario NAME
                 [--catalog scripts/artifacts.toml]
                 [--benchmarks-dir benchmarks]
                 [--workdir .smoke/fake-runner]
                 [--clean-cache]
                 [--rebuild-image]

--engine/--bar-content/--map resolve to catalog entries (gs:// URIs that
the runner pulls). --scenario is a repo-local folder name under
<benchmarks-dir>/ containing startscript.txt and bar-data/ (any files
under games/BAR.sdd/ override the base content; anything else lands in
/var/bar-data/ as extras).

Downloaded artifacts are cached under <workdir>/cache/... so subsequent
runs skip the network. The runner's working tree
(<workdir>/{bucket,data,run,engine,results}) is wiped at the start of
each run to give the runner a fresh-VM look. <workdir>/bucket mirrors
the real artifacts-bucket layout: content-addressed engine/bar-content/
map at the root and per-job files (overlay, startscript, wheel,
manifest) under fake-runner-local/.

Requires Docker. On Apple Silicon, enable Rosetta in Docker Desktop so
the amd64 spring-headless binary can run under emulation.
EOF
  exit 2
}

# ---- arg parsing ----

engine_name=""
bar_content_name=""
map_name=""
scenario_name=""
catalog="$DEFAULT_CATALOG"
benchmarks_dir="benchmarks"
workdir="$DEFAULT_WORKDIR"
clean_cache=0
rebuild_image=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine) engine_name="$2"; shift 2 ;;
    --bar-content) bar_content_name="$2"; shift 2 ;;
    --map) map_name="$2"; shift 2 ;;
    --scenario) scenario_name="$2"; shift 2 ;;
    --catalog) catalog="$2"; shift 2 ;;
    --benchmarks-dir) benchmarks_dir="$2"; shift 2 ;;
    --workdir) workdir="$2"; shift 2 ;;
    --clean-cache) clean_cache=1; shift ;;
    --rebuild-image) rebuild_image=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

for var in engine_name bar_content_name map_name scenario_name; do
  if [[ -z "${!var}" ]]; then
    echo "missing required flag: --${var%_name}" >&2
    usage
  fi
done

# ---- pre-flight tooling ----

command -v gcloud  >/dev/null || { echo "gcloud not found on PATH" >&2; exit 1; }
command -v uv      >/dev/null || { echo "uv not found on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found on PATH" >&2; exit 1; }
command -v docker  >/dev/null || { echo "docker not found on PATH" >&2; exit 1; }

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
script_dir="$(cd "$(dirname "$0")" && pwd)"

# Resolve paths (workdir + catalog) relative to the repo root if given as relative.
case "$workdir" in
  /*) ;;
  *)  workdir="$repo_root/$workdir" ;;
esac
case "$catalog" in
  /*) ;;
  *)  catalog="$repo_root/$catalog" ;;
esac
case "$benchmarks_dir" in
  /*) ;;
  *)  benchmarks_dir="$repo_root/$benchmarks_dir" ;;
esac

if [[ ! -f "$catalog" ]]; then
  echo "catalog not found: $catalog" >&2
  exit 1
fi

scenario_dir="$benchmarks_dir/$scenario_name"
if [[ ! -d "$scenario_dir" ]]; then
  echo "scenario not found: $scenario_dir" >&2
  exit 1
fi
if [[ ! -f "$scenario_dir/startscript.txt" ]]; then
  echo "scenario has no startscript.txt: $scenario_dir" >&2
  exit 1
fi

cache_dir="$workdir/cache"
# bucket_dir mirrors the real artifacts-bucket tree: content-addressed
# engine/bar-content/map at the root, per-job entries under <job_uid>/.
# Mounted at /mnt/artifacts-bucket inside the container, while
# <job_uid>/ is also mounted at /mnt/artifacts (same as on Batch).
bucket_dir="$workdir/bucket"
job_uid="fake-runner-local"
artifacts_dir="$bucket_dir/$job_uid"
data_dir="$workdir/data"
run_dir="$workdir/run"
engine_dir="$workdir/engine"
results_dir="$workdir/results"

# ---- resolve artifact URIs from the catalog ----

lookup() {
  # $1=type, $2=name, $3=optional field (e.g. "dest" for table entries)
  python3 "$script_dir/_catalog.py" "$catalog" "$1" "$2" ${3:+"$3"}
}

engine_uri="$(lookup engine "$engine_name" dest)"         || exit 1
bar_content_uri="$(lookup bar_content "$bar_content_name" dest)" || exit 1
# Map entries may also have a source — the runner only reads the
# dest (gs://) URI, which fake-orchestrator.sh will have populated.
map_uri="$(lookup map "$map_name" dest)"                 || exit 1
map_basename="$(basename "$map_uri")"

# ---- cache layout: <cache_dir>/<bucket>/<key> ----

# Convert "gs://bucket/path/to/blob" -> "<cache>/bucket/path/to/blob"
cache_path_for() {
  local uri="$1"
  local stripped="${uri#gs://}"
  printf '%s/%s' "$cache_dir" "$stripped"
}

if [[ $clean_cache -eq 1 ]]; then
  echo "[fake-runner] --clean-cache: wiping $cache_dir" >&2
  rm -rf "$cache_dir"
fi

mkdir -p "$cache_dir"

fetch() {
  # Note: callers consume our stdout via $(), and `set -e` does NOT fire on a
  # failed assignment-from-substitution. So callers must use `|| exit 1`
  # explicitly. We `return 1` here on failure (exit would only kill the
  # subshell, not the parent).
  local uri="$1"
  local dest
  dest="$(cache_path_for "$uri")"
  if [[ -f "$dest" ]]; then
    echo "[fake-runner] cache hit: $uri" >&2
  else
    echo "[fake-runner] downloading: $uri" >&2
    mkdir -p "$(dirname "$dest")"
    if ! gcloud storage cp "$uri" "$dest" >&2; then
      echo "[fake-runner] download failed: $uri" >&2
      return 1
    fi
  fi
  printf '%s' "$dest"
}

engine_local="$(fetch "$engine_uri")"             || exit 1
bar_content_local="$(fetch "$bar_content_uri")"   || exit 1
map_local="$(fetch "$map_uri")"                   || exit 1

# ---- wipe + re-stage runtime dirs ----

echo "[fake-runner] staging working tree at $workdir" >&2
rm -rf "$bucket_dir" "$data_dir" "$run_dir" "$engine_dir" "$results_dir"
mkdir -p "$artifacts_dir" "$data_dir" "$run_dir" "$engine_dir" "$results_dir"

# Stage the three shared artifacts under the same bucket keys the
# production orchestrator writes — i.e. the path portion of each
# catalog dest URI. Keys are name-based (not content-hashed), matching
# scripts/artifacts.toml.
uri_to_key() {
  local uri="$1"
  local stripped="${uri#gs://}"
  printf '%s' "${stripped#*/}"
}

engine_key="$(uri_to_key "$engine_uri")"
bar_content_key="$(uri_to_key "$bar_content_uri")"
map_key="$(uri_to_key "$map_uri")"

mkdir -p "$bucket_dir/$(dirname "$engine_key")" \
         "$bucket_dir/$(dirname "$bar_content_key")" \
         "$bucket_dir/$(dirname "$map_key")"

# Hardlink cached downloads into the bucket-root tree. Hardlinks keep
# the tree cheap without duplicating multi-GB blobs; fall back to cp if
# the cache lives on a different volume.
stage_artifact() {
  local src="$1" dst="$2"
  if ! ln "$src" "$dst" 2>/dev/null; then
    cp "$src" "$dst"
  fi
}

stage_artifact "$engine_local"      "$bucket_dir/$engine_key"
stage_artifact "$bar_content_local" "$bucket_dir/$bar_content_key"
stage_artifact "$map_local"         "$bucket_dir/$map_key"

# Scenario-derived per-job artifacts: the startscript is the scenario's
# file as-is, and overlay.tar.gz is the scenario's bar-data/ tree —
# contents at tarball root so runner.py extracts it directly onto
# /var/bar-data/. If bar-data/ is absent, produce an empty tarball so
# the runner's extract step is a no-op rather than a missing-file error.
cp "$scenario_dir/startscript.txt" "$artifacts_dir/startscript.txt"
if [[ -d "$scenario_dir/bar-data" ]]; then
  echo "[fake-runner] packing scenario bar-data -> overlay.tar.gz" >&2
  tar -C "$scenario_dir/bar-data" -czf "$artifacts_dir/overlay.tar.gz" .
else
  echo "[fake-runner] scenario has no bar-data/ — producing empty overlay.tar.gz" >&2
  tar -czf "$artifacts_dir/overlay.tar.gz" -T /dev/null
fi

# ---- build the bar_benchmarks wheel into artifacts, mirroring what
# orchestrator/run.py uploads to the artifacts bucket on real Batch. The
# container's bootstrap then pip-installs this wheel exactly as
# BOOTSTRAP_SCRIPT does on a Batch VM. ----

echo "[fake-runner] building wheel -> $artifacts_dir" >&2
(cd "$repo_root" && uv build --wheel --out-dir "$artifacts_dir" >&2)

wheel_filename="$(cd "$artifacts_dir" && ls bar_benchmarks-*.whl 2>/dev/null | head -n1 || true)"
if [[ -z "$wheel_filename" ]]; then
  echo "[fake-runner] wheel build produced no bar_benchmarks-*.whl in $artifacts_dir" >&2
  exit 1
fi

# Synthesize a manifest matching what orchestrator/artifacts.py emits:
# artifact_names + paths block (engine/bar_content/map keys under the
# bucket root) + map_filename. Collector surfaces artifact_names via
# results.json for traceability.
cat >"$artifacts_dir/manifest.json" <<EOF
{
  "job_uid": "$job_uid",
  "region": "local",
  "instance_type": "local",
  "map_filename": "$map_basename",
  "artifact_names": {
    "engine": "$engine_name",
    "bar_content": "$bar_content_name",
    "map": "$map_name"
  },
  "paths": {
    "engine": "$engine_key",
    "bar_content": "$bar_content_key",
    "map": "$map_key"
  },
  "wheel_filename": "$wheel_filename"
}
EOF

# ---- extract BOOTSTRAP_SCRIPT verbatim from batch_submitter so the
# container runs the *same* script the real Batch Task runs. Writing it
# to disk (rather than passing via `sh -c`) preserves multi-line semantics
# and `set -eu` cleanly. ----

bootstrap_path="$run_dir/bootstrap.sh"
(cd "$repo_root" && uv run --quiet python -c "
import sys
from bar_benchmarks.orchestrator.batch_submitter import BOOTSTRAP_SCRIPT
sys.stdout.write(BOOTSTRAP_SCRIPT)
" >"$bootstrap_path")
chmod +x "$bootstrap_path"

# ---- docker image: build on first run, rebuild on --rebuild-image. Layer
# caching makes the steady-state `docker build` a fast no-op. ----

dockerfile="$script_dir/fake-runner.Dockerfile"
build_args=()
if [[ $rebuild_image -eq 1 ]]; then
  build_args+=(--no-cache)
  echo "[fake-runner] --rebuild-image: forcing docker build --no-cache" >&2
fi

echo "[fake-runner] docker build -t $IMAGE_TAG" >&2
docker build ${build_args[@]+"${build_args[@]}"} -t "$IMAGE_TAG" -f "$dockerfile" "$script_dir" >&2

# ---- run the task pipeline in the container. Mounts place each host
# scratch dir at the canonical Batch path so the container's view matches
# a real Task's view. Env vars mirror batch_submitter.ENV_VARS. ----

echo "[fake-runner] docker run (impersonating a Batch Task VM)" >&2
set +e
docker run --rm \
  --platform=linux/amd64 \
  -e BAR_ARTIFACTS_DIR=/mnt/artifacts \
  -e BAR_ARTIFACTS_BUCKET_DIR=/mnt/artifacts-bucket \
  -e BAR_RESULTS_DIR=/mnt/results \
  -e BAR_DATA_DIR=/var/bar-data \
  -e BAR_RUN_DIR=/var/bar-run \
  -e BAR_ENGINE_DIR=/opt/recoil \
  -e BAR_BENCHMARK_OUTPUT_PATH=benchmark-results.json \
  -e PYTHONPATH=/var/bar-run/pypkgs \
  -e BATCH_JOB_UID="$job_uid" \
  -e BATCH_TASK_INDEX=0 \
  -v "$artifacts_dir:/mnt/artifacts" \
  -v "$bucket_dir:/mnt/artifacts-bucket" \
  -v "$results_dir:/mnt/results" \
  -v "$data_dir:/var/bar-data" \
  -v "$run_dir:/var/bar-run" \
  -v "$engine_dir:/opt/recoil" \
  "$IMAGE_TAG" \
  sh -c '/var/bar-run/bootstrap.sh && exec python3 -m bar_benchmarks.task.main'
rc=$?
set -e

# ---- summary ----

verdict="$run_dir/verdict.json"
bench_out="$data_dir/benchmark-results.json"
bar_sdd="$data_dir/games/BAR.sdd"

echo >&2
echo "[fake-runner] task exit: $rc" >&2
echo "[fake-runner] verdict:        $verdict $( [[ -f $verdict ]] && echo '(present)' || echo '(missing)')" >&2
echo "[fake-runner] benchmark out:  $bench_out $( [[ -f $bench_out ]] && echo '(present)' || echo '(missing)')" >&2
echo "[fake-runner] BAR.sdd:        $bar_sdd $( [[ -d $bar_sdd ]] && echo '(present)' || echo '(missing)')" >&2

exit "$rc"
