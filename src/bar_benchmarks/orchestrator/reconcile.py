"""Reconcile submitted task count against uploaded results.json objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Reconciliation:
    job_uid: str
    submitted: int
    present_indices: list[int]
    missing_indices: list[int]


def reconcile(results_bucket: str, job_uid: str, submitted: int, *, client=None) -> Reconciliation:
    if client is None:
        from google.cloud import storage

        client = storage.Client()
    bucket_name = results_bucket.removeprefix("gs://")
    bucket = client.bucket(bucket_name)
    prefix = f"{job_uid}/"
    present: set[int] = set()
    for blob in client.list_blobs(bucket, prefix=prefix):
        # Expect key: <job_uid>/<task_index>/results.json
        parts = blob.name[len(prefix):].split("/")
        if len(parts) == 2 and parts[1] == "results.json":
            try:
                present.add(int(parts[0]))
            except ValueError:
                continue
    all_ixs = set(range(submitted))
    return Reconciliation(
        job_uid=job_uid,
        submitted=submitted,
        present_indices=sorted(present & all_ixs),
        missing_indices=sorted(all_ixs - present),
    )
