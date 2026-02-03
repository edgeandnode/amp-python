"""Async jobs client for Admin API.

This module provides the AsyncJobsClient class for monitoring and managing
extraction jobs using async/await.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Optional

from . import models

if TYPE_CHECKING:
    from .async_client import AsyncAdminClient


class AsyncJobsClient:
    """Async client for job operations.

    Provides async methods for monitoring, managing, and waiting for extraction jobs.

    Args:
        admin_client: Parent AsyncAdminClient instance

    Example:
        >>> async with AsyncAdminClient('http://localhost:8080') as client:
        ...     job = await client.jobs.get(123)
        ...     print(f'Status: {job.status}')
    """

    def __init__(self, admin_client: 'AsyncAdminClient'):
        """Initialize async jobs client.

        Args:
            admin_client: Parent AsyncAdminClient instance
        """
        self._admin = admin_client

    async def get(self, job_id: int) -> models.JobInfo:
        """Get job information by ID.

        Args:
            job_id: Job ID to retrieve

        Returns:
            JobInfo with job details

        Raises:
            JobNotFoundError: If job not found

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     job = await client.jobs.get(123)
            ...     print(f'Status: {job.status}')
        """
        path = f'/jobs/{job_id}'
        response = await self._admin._request('GET', path)
        return models.JobInfo.model_validate(response.json())

    async def list(self, limit: int = 50, last_job_id: Optional[int] = None) -> models.JobsResponse:
        """List jobs with pagination.

        Args:
            limit: Maximum number of jobs to return (default: 50, max: 1000)
            last_job_id: Cursor from previous page's next_cursor field

        Returns:
            JobsResponse with jobs and optional next_cursor

        Raises:
            ListJobsError: If listing fails

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     response = await client.jobs.list(limit=100)
            ...     for job in response.jobs:
            ...         print(f'{job.id}: {job.status}')
        """
        params = {'limit': limit}
        if last_job_id is not None:
            params['last_job_id'] = last_job_id

        response = await self._admin._request('GET', '/jobs', params=params)
        return models.JobsResponse.model_validate(response.json())

    async def wait_for_completion(
        self, job_id: int, poll_interval: int = 5, timeout: Optional[int] = None
    ) -> models.JobInfo:
        """Poll job until completion or timeout.

        Continuously polls the job status until it reaches a terminal state
        (Completed, Failed, or Stopped). Uses asyncio.sleep for non-blocking waits.

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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     deploy_resp = await client.datasets.deploy('_', 'my_dataset', '1.0.0')
            ...     final_job = await client.jobs.wait_for_completion(deploy_resp.job_id)
            ...     print(f'Final status: {final_job.status}')
        """
        elapsed = 0.0
        terminal_states = {'Completed', 'Failed', 'Stopped'}

        while True:
            job = await self.get(job_id)

            # Check if job reached terminal state
            if job.status in terminal_states:
                return job

            # Check timeout
            if timeout is not None and elapsed >= timeout:
                raise TimeoutError(
                    f'Job {job_id} did not complete within {timeout} seconds. Current status: {job.status}'
                )

            # Wait before next poll (non-blocking)
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

    async def stop(self, job_id: int) -> None:
        """Stop a running job.

        Requests the job to stop gracefully. The job will transition through
        StopRequested and Stopping states before reaching Stopped.

        Args:
            job_id: Job ID to stop

        Raises:
            JobNotFoundError: If job not found
            JobStopError: If stop request fails

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     await client.jobs.stop(123)
        """
        path = f'/jobs/{job_id}/stop'
        await self._admin._request('POST', path)

    async def delete(self, job_id: int) -> None:
        """Delete a job in terminal state.

        Only jobs in terminal states (Completed, Failed, Stopped) can be deleted.

        Args:
            job_id: Job ID to delete

        Raises:
            JobNotFoundError: If job not found
            JobDeleteError: If job is not in terminal state or deletion fails

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     await client.jobs.delete(123)
        """
        path = f'/jobs/{job_id}'
        await self._admin._request('DELETE', path)

    async def delete_many(self, job_ids: list[int]) -> None:
        """Delete multiple jobs in bulk.

        All specified jobs must be in terminal states.

        Args:
            job_ids: List of job IDs to delete

        Raises:
            JobsDeleteError: If any deletion fails

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     await client.jobs.delete_many([123, 124, 125])
        """
        await self._admin._request('DELETE', '/jobs', json={'job_ids': job_ids})
