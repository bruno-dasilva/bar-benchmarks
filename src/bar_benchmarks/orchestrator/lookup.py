"""Find a prior benchmark run in the results bucket whose parameters match.

Every `bar-bench run` writes `<results-bucket>/<job_uid>/run.json` with the
shape-defining parameters (engine, bar_content, map, scenario, count,
machine_type) at submit time, and `report.json` at the very end after
aggregation. A run is cacheable only if BOTH apply:

  1. run.json matches the requested shape exactly, and
  2. report.json exists AND reports zero invalid tasks (`invalid == 0`).

(1) filters the candidate set; (2) rejects orchestrator crashes (no
report.json at all) and fully-or-partially-failed batches (VMs that
never uploaded, infra failures, invalid results). Skip reasons are
written to stderr so the Actions log explains every "no hit".

Job UIDs embed a Unix timestamp (`bar-bench-<epoch>-<rand>`), so lexical
sort-by-timestamp gives us "most recent first" without extra metadata.
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any

_JOB_UID_RE = re.compile(r"^bar-bench-(\d+)-[0-9a-f]+$")


def _list_job_uid_prefixes(client, bucket) -> list[str]:
    """Return top-level `bar-bench-*` directories under the bucket."""
    iterator = client.list_blobs(bucket, prefix="", delimiter="/")
    # Force pagination so iterator.prefixes populates.
    for _ in iterator.pages:
        pass
    out: list[str] = []
    for pref in iterator.prefixes:
        name = pref.rstrip("/")
        if _JOB_UID_RE.match(name):
            out.append(name)
    return out


def find_matching_run(
    *,
    results_bucket: str,
    engine: str,
    bar_content: str,
    map_: str,
    scenario: str,
    count: int,
    iterations: int = 1,
    machine_type: str,
    scan_limit: int = 100,
    project: str | None = None,
    client=None,
) -> dict[str, Any] | None:
    """Return the newest matching run's metadata dict, or None.

    Matches on the submit-time shape: engine, bar_content, map, scenario,
    count, iterations, machine_type. Inspects up to `scan_limit` most-recent
    job_uids. Pre-iterations runs (no `iterations` key in run.json) are
    treated as iterations=1 for match purposes.
    """
    if client is None:
        from google.cloud import storage

        client = storage.Client(project=project)
    bucket = client.bucket(results_bucket.removeprefix("gs://"))

    job_uids = _list_job_uid_prefixes(client, bucket)

    def ts(u: str) -> int:
        m = _JOB_UID_RE.match(u)
        return int(m.group(1)) if m else 0

    recent = sorted(job_uids, key=ts, reverse=True)[:scan_limit]

    for job_uid in recent:
        blob = bucket.blob(f"{job_uid}/run.json")
        try:
            body = blob.download_as_bytes()
        except Exception:
            continue
        try:
            meta = json.loads(body)
        except json.JSONDecodeError:
            continue
        if not (
            meta.get("engine") == engine
            and meta.get("bar_content") == bar_content
            and meta.get("map") == map_
            and meta.get("scenario") == scenario
            and meta.get("count") == count
            and meta.get("iterations", 1) == iterations
            and meta.get("machine_type") == machine_type
        ):
            continue
        # Completion gate — `report.json` is written as the final step of
        # `bar-bench run`, so its presence signals the orchestrator made
        # it through aggregation. Its `invalid` field signals whether the
        # benchmark itself was clean. We require both: the orchestrator
        # finished AND every submitted task produced a valid result.
        try:
            report_body = bucket.blob(f"{job_uid}/report.json").download_as_bytes()
        except Exception as exc:  # noqa: BLE001 — treat any GCS error as "no sentinel"
            print(
                f"[lookup] skipped {job_uid}: no report.json "
                f"(orchestrator likely didn't finish: {type(exc).__name__})",
                file=sys.stderr,
            )
            continue
        try:
            report_data = json.loads(report_body)
        except json.JSONDecodeError as exc:
            print(
                f"[lookup] skipped {job_uid}: report.json is malformed ({exc})",
                file=sys.stderr,
            )
            continue
        valid = report_data.get("valid", 0) or 0
        invalid = report_data.get("invalid", 0) or 0
        expected = count * iterations
        if invalid > 0 or valid < expected:
            print(
                f"[lookup] skipped {job_uid}: run incomplete "
                f"(valid={valid} invalid={invalid} expected={expected})",
                file=sys.stderr,
            )
            continue
        print(
            f"[lookup] candidate {job_uid} passed all gates "
            f"(shape match, report.json present, valid={valid} invalid=0)",
            file=sys.stderr,
        )
        return {**meta, "_report_valid": valid, "_report_invalid": invalid}
    return None
