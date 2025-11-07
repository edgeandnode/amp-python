"""Jobs client for Admin API.

This module provides the JobsClient class for monitoring and managing
extraction jobs.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

from . import models

if TYPE_CHECKING:
    from .client import AdminClient


class JobsClient:
    """Client for job operations.

    Provides methods for monitoring, managing, and waiting for extraction jobs.

    Args:
        admin_client: Parent AdminClient instance

    Example:
        >>> client = AdminClient('http://localhost:8080')
        >>> job = client.jobs.get(123)
        >>> print(f'Status: {job.status}')
    """

    def __init__(self, admin_client: 'AdminClient'):
        """Initialize jobs client.

        Args:
            admin_client: Parent AdminClient instance
        """
        self._admin = admin_client

    def get(self, job_id: int) -> models.JobInfo:
        """Get job information by ID.

        Args:
            job_id: Job ID to retrieve

        Returns:
            JobInfo with job details

        Raises:
            JobNotFoundError: If job not found

        Example:
            >>> job = client.jobs.get(123)
            >>> print(f'Status: {job.status}')
            >>> print(f'Dataset: {job.dataset}')
        """
        path = f'/jobs/{job_id}'
        response = self._admin._request('GET', path)
        return models.JobInfo.model_validate(response.json())

    def list(self, limit: int = 50, last_job_id: Optional[int] = None) -> models.JobsResponse:
        """List jobs with pagination.

        Args:
            limit: Maximum number of jobs to return (default: 50, max: 1000)
            last_job_id: Cursor from previous page's next_cursor field

        Returns:
            JobsResponse with jobs and optional next_cursor

        Raises:
            ListJobsError: If listing fails

        Example:
            >>> # First page
            >>> response = client.jobs.list(limit=100)
            >>> for job in response.jobs:
            ...     print(f'{job.id}: {job.status}')
            >>>
            >>> # Next page
            >>> if response.next_cursor:
            ...     next_page = client.jobs.list(limit=100, last_job_id=response.next_cursor)
        """
        params = {'limit': limit}
        if last_job_id is not None:
            params['last_job_id'] = last_job_id

        response = self._admin._request('GET', '/jobs', params=params)
        return models.JobsResponse.model_validate(response.json())

    def wait_for_completion(self, job_id: int, poll_interval: int = 5, timeout: Optional[int] = None) -> models.JobInfo:
        """Poll job until completion or timeout.

        Continuously polls the job status until it reaches a terminal state
        (Completed, Failed, or Stopped).

        Args:
            job_id: Job ID to monitor
            poll_interval: Seconds between status checks (default: 5)
            timeout: Optional timeout in seconds (default: None = infinite)

        Returns:
            Final JobInfo when job completes

        Raises:
            JobNotFoundError: If job not found
            TimeoutError: If timeout is reached before completion

        Example:
            >>> # Deploy and wait
            >>> deploy_resp = client.datasets.deploy('_', 'my_dataset', '1.0.0')
            >>> final_job = client.jobs.wait_for_completion(deploy_resp.job_id, poll_interval=10)
            >>> print(f'Final status: {final_job.status}')
        """
        start_time = time.time()
        terminal_states = {'Completed', 'Failed', 'Stopped'}

        while True:
            job = self.get(job_id)

            # Check if job reached terminal state
            if job.status in terminal_states:
                return job

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(
                        f'Job {job_id} did not complete within {timeout} seconds. Current status: {job.status}'
                    )

            # Wait before next poll
            time.sleep(poll_interval)

    def stop(self, job_id: int) -> None:
        """Stop a running job.

        Requests the job to stop gracefully. The job will transition through
        StopRequested and Stopping states before reaching Stopped.

        Args:
            job_id: Job ID to stop

        Raises:
            JobNotFoundError: If job not found
            JobStopError: If stop request fails

        Example:
            >>> client.jobs.stop(123)
        """
        path = f'/jobs/{job_id}/stop'
        self._admin._request('POST', path)

    def delete(self, job_id: int) -> None:
        """Delete a job in terminal state.

        Only jobs in terminal states (Completed, Failed, Stopped) can be deleted.

        Args:
            job_id: Job ID to delete

        Raises:
            JobNotFoundError: If job not found
            JobDeleteError: If job is not in terminal state or deletion fails

        Example:
            >>> client.jobs.delete(123)
        """
        path = f'/jobs/{job_id}'
        self._admin._request('DELETE', path)

    def delete_many(self, job_ids: list[int]) -> None:
        """Delete multiple jobs in bulk.

        All specified jobs must be in terminal states.

        Args:
            job_ids: List of job IDs to delete

        Raises:
            JobsDeleteError: If any deletion fails

        Example:
            >>> client.jobs.delete_many([123, 124, 125])
        """
        self._admin._request('DELETE', '/jobs', json={'job_ids': job_ids})
