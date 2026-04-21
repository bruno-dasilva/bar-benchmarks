# Local-only simulator image for scripts/fake-runner.sh. The container is
# meant to impersonate a GCP Batch Task VM (batch-debian) closely enough
# that a passing fake-runner implies the Batch path will work.
#
# Keep the installed package set aligned with
# src/bar_benchmarks/orchestrator/batch_submitter.py:BOOTSTRAP_SCRIPT.
# batch-debian ships Python 3.11 pre-installed, so we pre-install it here
# too; the bootstrap script's apt-get line is then an idempotent no-op on
# both paths, just like it is on Batch. If spring-headless turns out to
# need additional system libraries at runtime, add them to BOTH this
# Dockerfile and BOOTSTRAP_SCRIPT in the same commit.
#
# linux/amd64 is hard-pinned because spring-headless is an amd64 binary
# and Batch VMs are amd64. On Apple Silicon this requires Docker Desktop
# with Rosetta emulation enabled.

FROM --platform=linux/amd64 debian:12-slim

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-venv \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*
