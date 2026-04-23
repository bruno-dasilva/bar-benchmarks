# GitHub Action: `bar-benchmarks`

Wrap `bar-bench run` as a reusable GitHub Action so any repo can submit a
benchmark batch from CI and read structured results back as step outputs.

The action is a composite wrapper around the CLI in this repo; it does not
bundle GCP auth. Callers authenticate with
[`google-github-actions/auth@v2`](https://github.com/google-github-actions/auth)
before invoking it.

See [`examples/github-action/recoil-pr-benchmark.yml`](../examples/github-action/recoil-pr-benchmark.yml)
for a complete PR-triggered workflow.

## Minimal usage

```yaml
- uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: projects/.../workloadIdentityPools/...
    service_account: bench-operator@bar-experiments.iam.gserviceaccount.com

- uses: beyond-all-reason/bar-benchmarks@v1
  id: bench
  with:
    engine-commit: ${{ github.event.pull_request.head.sha }}
    bar-content: bar-test-29871-90f4bc1
    map: hellas-basin-v1.4
    scenario: lategame1
    count: 10
    gcp-project: bar-experiments
```

## Inputs

| Input                 | Required        | Default                                     | Notes |
|-----------------------|-----------------|---------------------------------------------|-------|
| `scenario`            | yes             | —                                           | Folder under `benchmarks/` in the action's checkout. |
| `map`                 | yes             | —                                           | Map catalog name from `artifacts.toml`. |
| `engine`              | one-of          | —                                           | Pre-registered engine catalog name. |
| `engine-commit`       | one-of          | —                                           | Ad-hoc RecoilEngine SHA (hex, ≥7 chars). |
| `bar-content`         | one-of          | —                                           | Pre-registered bar-content catalog name. |
| `bar-content-version` | one-of          | —                                           | Ad-hoc version like `Beyond All Reason test-29871-90f4bc1`. |
| `count`               | no              | `20`                                        | VMs to spawn. |
| `description`         | no              | —                                           | Shown verbatim in the aggregate report. |
| `gcp-project`         | yes             | —                                           | Project that owns the Batch job + buckets. |
| `gcp-region`          | no              | `us-central1`                               | |
| `artifacts-bucket`    | no              | `gs://bar-experiments-bench-artifacts`      | Staging bucket. |
| `results-bucket`      | no              | `gs://bar-experiments-bench-results`        | Per-task `results.json` destination. |
| `machine-type`        | no              | `n1-standard-8`                             | GCE machine type. |
| `service-account`     | no              | `benchmark-runner@<gcp-project>.iam.gserviceaccount.com` | Attached to Batch VMs. |
| `max-run-duration`    | no              | `1800`                                      | Per-task timeout (seconds). |
| `github-token`        | no              | `${{ github.token }}`                       | Used by `scripts/build-engine.sh` on a cache miss. Cross-repo artifact reads may need a PAT with `actions:read` on `beyond-all-reason/RecoilEngine`. |

Exactly one of `engine` / `engine-commit` is required, and exactly one of
`bar-content` / `bar-content-version`. The ad-hoc forms synthesize a catalog
entry on the fly; the orchestrator's bucket-side cache check means subsequent
runs against the same commit skip the build.

## Outputs

| Output              | Example                          |
|---------------------|----------------------------------|
| `job-uid`           | `bar-bench-1713831234-a1b2c3`    |
| `results-gcs-uri`   | `gs://bar-experiments-bench-results/bar-bench-1713831234-a1b2c3/` |
| `report-json-path`  | `/home/runner/work/_temp/bar-bench-report.json` |
| `mean-ms`           | `16.451`                         |
| `p95-ms`            | `16.821`                         |
| `median-ms`         | `16.378`                         |
| `stddev-ms`         | `0.234`                          |
| `valid-count`       | `10`                             |
| `invalid-count`     | `0`                              |

The full `BatchReport` JSON is also uploaded as a workflow artifact named
`bar-bench-report-<job-uid>`.

## One-time GCP setup

The action expects two buckets, a service account attached to Batch VMs, and
a Workload Identity Federation pool that trusts GitHub's OIDC issuer. See the
"Prerequisites" block in the main [README](../README.md#prerequisites) for
the roles that need to be granted. On top of those, for the action:

1. Create a WIF pool + provider that trusts `token.actions.githubusercontent.com`.
2. Create a separate **operator** service account (distinct from
   `benchmark-runner` which runs on the VMs). Grant it:
   - `roles/batch.jobsEditor` at the project level
   - `roles/storage.objectUser` on both buckets
   - `roles/iam.serviceAccountUser` on the `benchmark-runner` SA
3. Bind the WIF principal (`principalSet://...`) to the operator SA via
   `roles/iam.workloadIdentityUser`, scoping by repository.
4. In the consumer repo, set variables `WIF_PROVIDER` and `BENCH_OPERATOR_SA`.

## Cost

Each run provisions `count` VMs of `machine-type` for up to `max-run-duration`
seconds. See [`COSTS.md`](../COSTS.md) for per-invocation math. Only enable on
a label (`benchmark`) or a manual `workflow_dispatch` unless the project has
budget for running it on every push.

## Extending `artifacts.toml` vs. using ad-hoc inputs

- **Catalog name** (`engine: recoil-892ff9e-master`) — preferred for baselines
  you benchmark repeatedly. Requires a PR to this repo to add the entry.
- **Ad-hoc** (`engine-commit: <SHA>`) — preferred for per-PR engine
  benchmarks where the SHA changes every run. No repo edit required; the
  action's catalog merger generates a synthetic `recoil-<sha[:7]>` name and
  the orchestrator caches the resulting tarball in the artifacts bucket.
