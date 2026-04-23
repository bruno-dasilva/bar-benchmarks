"""Find a prior benchmark run in the results bucket whose parameters match.

Every `bar-bench run` writes `<results-bucket>/<job_uid>/run.json` with the
shape-defining parameters (engine, bar_content, map, scenario, count,
machine_type). To skip a rerun, we scan the N most recent job_uid prefixes
and return the first whose run.json matches.

Job UIDs embed a Unix timestamp (`bar-bench-<epoch>-<rand>`), so lexical
sort-by-timestamp gives us "most recent first" without extra metadata.
"""

from __future__ import annotations

import json
import re
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
    machine_type: str,
    scan_limit: int = 100,
    project: str | None = None,
    client=None,
) -> dict[str, Any] | None:
    """Return the newest matching run's metadata dict, or None.

    Matches on the submit-time shape: engine, bar_content, map, scenario,
    count, machine_type. Inspects up to `scan_limit` most-recent job_uids.
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
        if (
            meta.get("engine") == engine
            and meta.get("bar_content") == bar_content
            and meta.get("map") == map_
            and meta.get("scenario") == scenario
            and meta.get("count") == count
            and meta.get("machine_type") == machine_type
        ):
            return meta
    return None
