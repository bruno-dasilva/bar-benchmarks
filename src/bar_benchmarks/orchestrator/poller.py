"""Poll a Batch Job until it reaches a terminal state."""

from __future__ import annotations

import time

from google.cloud import batch_v1

_TERMINAL = {
    batch_v1.JobStatus.State.SUCCEEDED,
    batch_v1.JobStatus.State.FAILED,
    batch_v1.JobStatus.State.CANCELLED,
    batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
}


def wait(
    job_name: str,
    *,
    client: batch_v1.BatchServiceClient | None = None,
    interval_s: float = 15.0,
    on_update=None,
) -> batch_v1.Job:
    """Block until `job_name`'s state is terminal. Returns the final Job."""
    if client is None:
        client = batch_v1.BatchServiceClient()
    while True:
        job = client.get_job(name=job_name)
        if on_update is not None:
            on_update(job)
        if job.status.state in _TERMINAL:
            return job
        time.sleep(interval_s)
