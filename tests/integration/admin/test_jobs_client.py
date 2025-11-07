"""Integration tests for JobsClient with HTTP mocking."""

import pytest
import respx
from httpx import Response

from amp.admin import AdminClient
from amp.admin.errors import JobNotFoundError


@pytest.mark.integration
class TestJobsClient:
    """Test JobsClient operations with mocked HTTP responses."""

    @respx.mock
    def test_get_job(self):
        """Test getting job by ID."""
        job_response = {'id': 123, 'status': 'Running', 'descriptor': {}, 'node_id': 'worker-1'}
        respx.get('http://localhost:8080/jobs/123').mock(return_value=Response(200, json=job_response))

        client = AdminClient('http://localhost:8080')
        job = client.jobs.get(123)

        assert job.id == 123
        assert job.status == 'Running'
        assert job.node_id == 'worker-1'

    @respx.mock
    def test_get_job_not_found(self):
        """Test getting non-existent job."""
        error_response = {'error_code': 'JOB_NOT_FOUND', 'error_message': 'Job 999 not found'}
        respx.get('http://localhost:8080/jobs/999').mock(return_value=Response(404, json=error_response))

        client = AdminClient('http://localhost:8080')

        with pytest.raises(JobNotFoundError):
            client.jobs.get(999)

    @respx.mock
    def test_list_jobs(self):
        """Test listing jobs with pagination."""
        jobs_response = {
            'jobs': [
                {'id': 123, 'status': 'Running', 'descriptor': {}, 'node_id': 'worker-1'},
                {'id': 124, 'status': 'Completed', 'descriptor': {}, 'node_id': 'worker-2'},
            ],
            'next_cursor': 125,
        }
        respx.get('http://localhost:8080/jobs').mock(return_value=Response(200, json=jobs_response))

        client = AdminClient('http://localhost:8080')
        response = client.jobs.list(limit=50)

        assert len(response.jobs) == 2
        assert response.next_cursor == 125
        assert response.jobs[0].id == 123
        assert response.jobs[1].status == 'Completed'

    @respx.mock
    def test_list_jobs_with_cursor(self):
        """Test listing jobs with cursor for pagination."""
        jobs_response = {'jobs': [], 'next_cursor': None}
        respx.get('http://localhost:8080/jobs').mock(return_value=Response(200, json=jobs_response))

        client = AdminClient('http://localhost:8080')
        response = client.jobs.list(limit=50, last_job_id=125)

        assert len(response.jobs) == 0
        assert response.next_cursor is None

    @respx.mock
    def test_wait_for_completion_success(self):
        """Test waiting for job completion."""
        # First call: job is Running
        # Second call: job is Completed
        job_running = {'id': 123, 'status': 'Running', 'descriptor': {}, 'node_id': 'worker-1'}
        job_completed = {'id': 123, 'status': 'Completed', 'descriptor': {}, 'node_id': 'worker-1'}

        route = respx.get('http://localhost:8080/jobs/123')
        route.side_effect = [Response(200, json=job_running), Response(200, json=job_completed)]

        client = AdminClient('http://localhost:8080')
        final_job = client.jobs.wait_for_completion(123, poll_interval=0.1, timeout=5)

        assert final_job.status == 'Completed'

    @respx.mock
    def test_wait_for_completion_timeout(self):
        """Test waiting for job with timeout."""
        job_running = {'id': 123, 'status': 'Running', 'descriptor': {}, 'node_id': 'worker-1'}
        respx.get('http://localhost:8080/jobs/123').mock(return_value=Response(200, json=job_running))

        client = AdminClient('http://localhost:8080')

        with pytest.raises(TimeoutError) as exc_info:
            client.jobs.wait_for_completion(123, poll_interval=0.1, timeout=0.3)

        assert 'did not complete within' in str(exc_info.value)

    @respx.mock
    def test_stop_job(self):
        """Test stopping a job."""
        respx.post('http://localhost:8080/jobs/123/stop').mock(return_value=Response(200))

        client = AdminClient('http://localhost:8080')
        client.jobs.stop(123)

        # Should complete without error

    @respx.mock
    def test_delete_job(self):
        """Test deleting a job."""
        respx.delete('http://localhost:8080/jobs/123').mock(return_value=Response(204))

        client = AdminClient('http://localhost:8080')
        client.jobs.delete(123)

        # Should complete without error

    @respx.mock
    def test_delete_many_jobs(self):
        """Test deleting multiple jobs."""
        respx.delete('http://localhost:8080/jobs').mock(return_value=Response(204))

        client = AdminClient('http://localhost:8080')
        client.jobs.delete_many([123, 124, 125])

        # Should complete without error
