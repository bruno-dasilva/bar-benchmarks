#!/usr/bin/env bash
# Build an engine.tar.gz artifact from a RecoilEngine commit by pulling the
# matching "Build Engine v2" GitHub Actions run and repackaging its
# amd64-linux output so `spring-headless` sits at the tarball root (where
# src/bar_benchmarks/task/runner.py:47 expects it after extraction).
#
# Invoked by the orchestrator (src/bar_benchmarks/orchestrator/build.py)
# on a cache miss to materialize the tarball before it's uploaded to the
# catalog's `dest`. Safe to run by hand for priming.

set -euo pipefail

DEFAULT_REPO="beyond-all-reason/RecoilEngine"
DEFAULT_WORKFLOW="Build Engine v2"
DEFAULT_WORKDIR=".smoke/engine-build"
ARTIFACT_PREFIX="engine-artifacts-amd64-linux"

usage() {
  cat >&2 <<EOF
Usage:
  build-engine.sh --commit SHA
                  --output PATH
                  [--workflow "$DEFAULT_WORKFLOW"]
                  [--repo $DEFAULT_REPO]
                  [--workdir $DEFAULT_WORKDIR]
                  [--rebuild]

Finds the latest successful run of the given workflow for the given commit,
downloads its amd64-linux artifact, unpacks the inner .7z, and repacks the
resulting tree as <output>.tar.gz with spring-headless at the top level.

Requires: gh (logged in), unzip, tar, and 7z or 7zz on PATH.
EOF
  exit 2
}

commit=""
output=""
workflow="$DEFAULT_WORKFLOW"
repo="$DEFAULT_REPO"
workdir="$DEFAULT_WORKDIR"
rebuild=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit)   commit="$2"; shift 2 ;;
    --output)   output="$2"; shift 2 ;;
    --workflow) workflow="$2"; shift 2 ;;
    --repo)     repo="$2"; shift 2 ;;
    --workdir)  workdir="$2"; shift 2 ;;
    --rebuild)  rebuild=1; shift ;;
    -h|--help)  usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

[[ -z "$commit" ]] && { echo "missing required flag: --commit" >&2; usage; }
[[ -z "$output" ]] && { echo "missing required flag: --output" >&2; usage; }

if [[ ! "$commit" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "--commit must be 7-40 lowercase hex chars, got: $commit" >&2
  exit 1
fi

# ---- pre-flight ----
command -v gh    >/dev/null || { echo "gh not found on PATH (install: https://cli.github.com/)" >&2; exit 1; }
command -v unzip >/dev/null || { echo "unzip not found on PATH" >&2; exit 1; }
command -v tar   >/dev/null || { echo "tar not found on PATH" >&2; exit 1; }

if command -v 7zz >/dev/null; then
  SEVENZ="7zz"
elif command -v 7z >/dev/null; then
  SEVENZ="7z"
else
  echo "neither 7zz nor 7z found on PATH (macOS: brew install sevenzip; debian: apt install p7zip-full)" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "gh is not authenticated (run: gh auth login)" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
case "$workdir" in /*) ;; *) workdir="$repo_root/$workdir" ;; esac
case "$output"  in /*) ;; *) output="$(pwd)/$output" ;; esac

cache_dir="$workdir/$commit"
download_dir="$cache_dir/download"
extract_dir="$cache_dir/extract"

if [[ $rebuild -eq 1 && -d "$cache_dir" ]]; then
  echo "[build-engine] --rebuild: wiping $cache_dir" >&2
  rm -rf "$cache_dir"
fi
mkdir -p "$cache_dir"

# ---- resolve run ----
echo "[build-engine] querying $repo for latest successful '$workflow' run at $commit" >&2
run_json="$(gh run list --repo "$repo" --workflow "$workflow" --commit "$commit" \
    --status success --limit 1 --json databaseId,createdAt,headSha,conclusion 2>/dev/null || true)"

run_id="$(printf '%s' "$run_json" | python3 -c '
import json, sys
runs = json.load(sys.stdin) or []
print(runs[0]["databaseId"] if runs else "")')"

if [[ -z "$run_id" ]]; then
  echo "[build-engine] no successful '$workflow' run found for $commit in $repo" >&2
  echo "[build-engine] any runs (all statuses) for diagnostic context:" >&2
  gh run list --repo "$repo" --workflow "$workflow" --commit "$commit" --limit 5 \
    --json databaseId,status,conclusion,createdAt,event 2>&1 | sed 's/^/[build-engine]   /' >&2 || true
  exit 1
fi
echo "[build-engine] run id: $run_id" >&2

# ---- find matching artifact ----
artifacts_json="$(gh api "repos/$repo/actions/runs/$run_id/artifacts" --paginate 2>/dev/null || true)"
artifact_info="$(printf '%s' "$artifacts_json" | python3 -c '
import json, sys
data = json.load(sys.stdin)
prefix = "'"$ARTIFACT_PREFIX"'"
matches = [a for a in data.get("artifacts", []) if a["name"].startswith(prefix)]
if not matches:
    print("NONE")
else:
    live = [a for a in matches if not a.get("expired")]
    if not live:
        print("EXPIRED\t" + matches[0]["name"])
    else:
        # Prefer the newest by created_at
        live.sort(key=lambda a: a.get("created_at", ""), reverse=True)
        a = live[0]
        print("OK\t{}\t{}".format(a["id"], a["name"]))
')"

case "$artifact_info" in
  NONE)
    echo "[build-engine] run $run_id has no artifact starting with '$ARTIFACT_PREFIX'" >&2
    exit 1 ;;
  EXPIRED*)
    name="${artifact_info#EXPIRED	}"
    echo "[build-engine] artifact '$name' on run $run_id has expired (GH Actions retention: 90d default)" >&2
    echo "[build-engine] re-run the workflow upstream to regenerate, or pick a newer commit" >&2
    exit 1 ;;
  OK*)
    artifact_id="$(printf '%s' "$artifact_info" | cut -f2)"
    artifact_name="$(printf '%s' "$artifact_info" | cut -f3)"
    echo "[build-engine] artifact: $artifact_name (id=$artifact_id)" >&2 ;;
  *)
    echo "[build-engine] unexpected artifact lookup output: $artifact_info" >&2
    exit 1 ;;
esac

# ---- download ----
# gh run download unpacks the artifact zip into --dir; files land directly
# in download_dir (no intermediate zip remains).
sevenz_file=""
if [[ -d "$download_dir" ]]; then
  sevenz_file="$(find "$download_dir" -type f -name 'recoil_*_amd64-linux*.7z' 2>/dev/null | head -n1 || true)"
fi

if [[ -n "$sevenz_file" ]]; then
  echo "[build-engine] cache hit (download): $sevenz_file" >&2
else
  echo "[build-engine] downloading artifact -> $download_dir" >&2
  rm -rf "$download_dir"
  mkdir -p "$download_dir"
  gh run download "$run_id" --repo "$repo" --name "$artifact_name" --dir "$download_dir" >&2
  sevenz_file="$(find "$download_dir" -type f -name 'recoil_*_amd64-linux*.7z' 2>/dev/null | head -n1 || true)"
fi

if [[ -z "$sevenz_file" ]]; then
  echo "[build-engine] no recoil_*_amd64-linux*.7z found under $download_dir after download" >&2
  echo "[build-engine] contents:" >&2
  find "$download_dir" -maxdepth 3 2>&1 | sed 's/^/[build-engine]   /' >&2
  exit 1
fi

multi="$(find "$download_dir" -type f -name 'recoil_*_amd64-linux*.7z' | wc -l | tr -d ' ')"
if [[ "$multi" -gt 1 ]]; then
  echo "[build-engine] expected exactly one recoil_*_amd64-linux*.7z, found $multi:" >&2
  find "$download_dir" -type f -name 'recoil_*_amd64-linux*.7z' | sed 's/^/[build-engine]   /' >&2
  exit 1
fi

# ---- extract 7z ----
spring_headless=""
if [[ -d "$extract_dir" ]]; then
  spring_headless="$(find "$extract_dir" -type f -name spring-headless 2>/dev/null | head -n1 || true)"
fi

if [[ -n "$spring_headless" ]]; then
  echo "[build-engine] cache hit (extract): $spring_headless" >&2
else
  echo "[build-engine] extracting $sevenz_file -> $extract_dir" >&2
  rm -rf "$extract_dir"
  mkdir -p "$extract_dir"
  "$SEVENZ" x -y "-o$extract_dir" "$sevenz_file" >&2
  spring_headless="$(find "$extract_dir" -type f -name spring-headless 2>/dev/null | head -n1 || true)"
fi

if [[ -z "$spring_headless" ]]; then
  echo "[build-engine] spring-headless not found after extraction" >&2
  echo "[build-engine] extract_dir contents:" >&2
  find "$extract_dir" -maxdepth 3 2>&1 | sed 's/^/[build-engine]   /' >&2
  exit 1
fi

multi="$(find "$extract_dir" -type f -name spring-headless | wc -l | tr -d ' ')"
if [[ "$multi" -gt 1 ]]; then
  echo "[build-engine] expected exactly one spring-headless, found $multi:" >&2
  find "$extract_dir" -type f -name spring-headless | sed 's/^/[build-engine]   /' >&2
  exit 1
fi

engine_root="$(dirname "$spring_headless")"
echo "[build-engine] engine root: $engine_root" >&2

# ---- repack ----
mkdir -p "$(dirname "$output")"
echo "[build-engine] creating $output" >&2
tar -C "$engine_root" -czf "$output" .

size="$(du -h "$output" | awk '{print $1}')"
echo "[build-engine] done: $output ($size)" >&2
