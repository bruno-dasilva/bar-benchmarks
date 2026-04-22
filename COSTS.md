# Costs

Back-of-the-envelope cost for a single invocation of `bar-benchmarks` on GCP.
Numbers are rough planning estimates, not quotes — rerun the math once the
final instance shape is pinned.

## Assumptions

| Parameter                    | Value                         | Notes                                    |
|------------------------------|-------------------------------|------------------------------------------|
| Cloud                        | GCP Batch (on Compute Engine) | Batch itself is free; you pay for the underlying VMs. |
| Region                       | `us-central1`                 | Cheapest tier, same region as artifact bucket. |
| Machine type                 | `c2d-standard-16` (spot)      | 16 vCPU / 64 GB RAM, AMD EPYC Milan.    |
| VM unit price (spot)         | $0.174304 / hr                | us-central1 spot list price.             |
| Boot disk                    | 50 GB, pd-balanced            | $0.10 / GB-month, billed per second.     |
| Runs per invocation          | 10                            | One Batch Task per run, taskCount=parallelism=10. |
| **End-to-end per VM**        | **~6 min** (0.1 hr)           | Includes boot, preflight, scenario, collector, teardown. |

All 10 VMs run in parallel, so wall-clock per invocation is ~6 min and
billable VM-time is 10 × 6 min = **1 VM-hour**.

## Per-invocation math

### Compute

```
10 VMs × 0.1 hr × $0.174304/hr  =  $0.174
```

### Boot disk (pd-balanced, 50 GB, per-second billing)

```
monthly cost per VM   = 50 GB × $0.10/GB-mo  = $5.00 / month
seconds in a month    ≈ 2,628,000 s          (30-day month)
per-second rate       = $5.00 / 2,628,000    ≈ $0.0000019 /s
per VM (360 s)        ≈ $0.00068
10 VMs                ≈ $0.007
```

### Egress

- **GCS → VM (same region):** free. The five artifacts live in a GCS
  bucket in `us-central1`, same as the VMs, so downloads cost $0.
- **VM → GCS (same region):** free. `results.json` upload costs $0.
- **Cross-region or internet egress:** $0.12/GB to internet, $0.01/GB
  between GCP regions. Keep everything co-located and this line is zero.

### Ephemeral external IP

GCE charges ~$0.005/hr per ephemeral external IPv4 while the VM is running:

```
10 VMs × 0.1 hr × $0.005/hr  =  $0.005
```

Avoidable if VMs use internal IPs only and pull artifacts via a VPC
endpoint — probably not worth optimizing for at this scale.

### GCS storage for artifacts + results

Negligible. 5 artifacts × maybe 1 GB each × $0.02/GB-month is $0.10/month
for the whole bucket; per-invocation share is effectively $0.

### Bottom line

| Line item               | Cost per invocation |
|-------------------------|---------------------|
| Compute (10 × 6 min)    | $0.17               |
| Boot disks              | $0.01               |
| Egress (same region)    | $0.00               |
| External IPs            | $0.01               |
| GCS storage             | ~$0.00              |
| GCP Batch service fee   | $0.00               |
| **Total**               | **~$0.19**          |

Round up to **~$0.25 per invocation** to cover preflight re-spawns and slack.

## Per-suite math (5 scenarios)

A full benchmark suite runs one invocation per scenario (`One scenario per
batch` invariant), so suite cost is just 5× the per-invocation numbers:

| Line item               | Per invocation | Per suite (×5) |
|-------------------------|----------------|----------------|
| Compute                 | $0.174         | $0.87          |
| Boot disks              | $0.007         | $0.035         |
| External IPs            | $0.005         | $0.025         |
| Egress / storage / Batch | $0.00         | $0.00          |
| **Total**               | **~$0.19**     | **~$0.93**     |

Round up to **~$1 per suite run**. Wall-clock is still ~6 min per scenario
(scenarios run sequentially as separate batches), so a full suite takes
~30 min end-to-end.

## Scaling

Assuming the per-VM time stays at 6 min and same pricing:

| Runs per invocation | VM-hours | Compute cost |
|---------------------|----------|--------------|
| 10                  | 1.0      | $0.17        |
| 20                  | 2.0      | $0.35        |
| 50                  | 5.0      | $0.87        |
| 100                 | 10.0     | $1.74        |
| 500                 | 50.0     | $8.72        |

Compute is linear in N. Disk and IP scale linearly too but remain rounding
errors. The tool can do a lot of benchmark iterations cheaply as long as
runs stay short and parallel.

## Sensitivities

These are the knobs that most change the bottom line:

- **Scenario duration.** Each extra minute adds ~$0.029 per invocation at
  N=10. Doubling the 6-min end-to-end time roughly doubles compute cost.
- **Instance size.** $0.174304/hr is the `c2d-standard-16` spot rate in
  us-central1; on-demand is ~3× higher. Going up to `c2d-standard-32`
  doubles the rate for 2× the vCPUs — only worth it if the scenario is
  CPU-bound on all 16 cores.
- **Spot preemption.** Short runs (~6 min) have low preemption risk, but
  GCP can reclaim a spot VM with 30 s notice. A preempted run is just an
  invalid run — re-running it is the cost of a normal VM. At N=10 and
  modest preemption rates, expect 0–1 re-runs per invocation on average.
- **Region.** `us-central1` is already in the cheapest tier. Moving to a
  pricier region (`europe-west1`, `asia-*`) adds 10–20%. Keep the artifact
  bucket co-located to preserve free egress.
- **Preflight rejections.** A VM that fails preflight still costs the boot
  + preflight time (~2 min × $0.174304/hr ≈ $0.006 per rejection). Cheap
  enough that aggressive rejection is fine.
- **Machine family choice.** `c2d` (AMD EPYC Milan) gives strong
  single-thread performance and good $/perf for CPU-bound workloads.
  `n2` (Intel Ice Lake) and `c3` (Sapphire Rapids) are nearby alternatives
  — switch only if benchmark parity with a specific CPU vendor matters.

## Ways to reduce cost (if ever needed)

At this scale, cost is not the bottleneck; time-to-signal is. But if future
N gets large:

- Smaller boot disk (20 GB instead of 50) cuts disk cost in half. Check
  that artifacts + working set fit.
- Regional artifact bucket + internal-only IPs to guarantee zero egress.
- Single shared GCS read-through cache if the same artifacts are reused
  across many invocations (avoids re-hashing / re-staging, but does not
  affect per-invocation cost).
- Switch from pd-balanced to pd-standard ($0.04/GB-month) if disk IOPS
  aren't a factor in the benchmark — halves an already tiny line item.
