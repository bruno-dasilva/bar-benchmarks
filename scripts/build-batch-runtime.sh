#!/usr/bin/env bash
# Build and push the Batch runtime image to Artifact Registry.
#
# Usage: scripts/build-batch-runtime.sh [TAG]
#   TAG defaults to today's UTC date (YYYY-MM-DD).
#
# After pushing, update the tag in both:
#   - src/bar_benchmarks/orchestrator/batch_submitter.py (CONTAINER_IMAGE)
#   - scripts/fake-runner.sh (IMAGE_TAG)
# so production and local loops pull the same image.

set -euo pipefail

tag="${1:-$(date -u +%Y-%m-%d)}"
image="us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:$tag"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v gcloud >/dev/null || { echo "gcloud not found on PATH" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker not found on PATH" >&2; exit 1; }

gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

docker build --platform=linux/amd64 \
  -f "$script_dir/batch-runtime.Dockerfile" \
  -t "$image" \
  "$script_dir"

docker push "$image"

echo "$image"
