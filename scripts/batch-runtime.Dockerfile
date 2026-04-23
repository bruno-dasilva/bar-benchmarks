# Runtime image for Batch Tasks. Pushed to
# us-central1-docker.pkg.dev/bar-experiments/benchmarks/batch-runtime:<tag>
# and pulled by the Batch Job (batch_submitter.CONTAINER_IMAGE).
#
# linux/amd64 is hard-pinned: spring-headless is an amd64 binary and Batch
# VMs are amd64.
#
# To roll a new image: edit this file, bump the tag in
# src/bar_benchmarks/orchestrator/batch_submitter.py (CONTAINER_IMAGE),
# then run scripts/build-batch-runtime.sh.

FROM --platform=linux/amd64 python:3.11-slim

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pre-installed Python deps so the VM doesn't need PyPI at task start.
# The bar_benchmarks wheel itself is still installed per-task from the
# GCS mount (it carries job-specific code).
RUN pip install --no-cache-dir pydantic
