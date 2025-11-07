"""Integration tests for AdminClient with HTTP mocking."""

import pytest
import respx
from httpx import Response

from amp.admin import AdminClient
from amp.admin.errors import DatasetNotFoundError


@pytest.mark.integration
class TestAdminClientHTTP:
    """Test AdminClient HTTP operations with mocked responses."""

    @respx.mock
    def test_admin_client_initialization(self):
        """Test AdminClient can be initialized."""
        client = AdminClient('http://localhost:8080')

        assert client.base_url == 'http://localhost:8080'
        assert client._http is not None

    @respx.mock
    def test_admin_client_with_auth_token(self):
        """Test AdminClient with authentication token."""
        client = AdminClient('http://localhost:8080', auth_token='test-token')

        assert 'Authorization' in client._http.headers
        assert client._http.headers['Authorization'] == 'Bearer test-token'

    @respx.mock
    def test_request_success(self):
        """Test successful HTTP request."""
        respx.get('http://localhost:8080/datasets').mock(return_value=Response(200, json={'datasets': []}))

        client = AdminClient('http://localhost:8080')
        response = client._request('GET', '/datasets')

        assert response.status_code == 200
        assert response.json() == {'datasets': []}

    @respx.mock
    def test_request_error_response(self):
        """Test HTTP request with error response."""
        error_response = {'error_code': 'DATASET_NOT_FOUND', 'error_message': 'Dataset not found'}
        respx.get('http://localhost:8080/datasets/_/missing/versions/1.0.0').mock(
            return_value=Response(404, json=error_response)
        )

        client = AdminClient('http://localhost:8080')

        with pytest.raises(DatasetNotFoundError) as exc_info:
            client._request('GET', '/datasets/_/missing/versions/1.0.0')

        assert exc_info.value.error_code == 'DATASET_NOT_FOUND'
        assert exc_info.value.status_code == 404

    @respx.mock
    def test_base_url_trailing_slash_removal(self):
        """Test that trailing slash is removed from base_url."""
        client = AdminClient('http://localhost:8080/')

        assert client.base_url == 'http://localhost:8080'

    @respx.mock
    def test_context_manager(self):
        """Test AdminClient as context manager."""
        with AdminClient('http://localhost:8080') as client:
            assert client._http is not None

        # After exiting context, connection should be closed
        # (httpx client will be closed)

    @respx.mock
    def test_datasets_property(self):
        """Test accessing datasets client via property."""
        client = AdminClient('http://localhost:8080')
        datasets_client = client.datasets

        assert datasets_client is not None
        assert datasets_client._admin is client

    @respx.mock
    def test_jobs_property(self):
        """Test accessing jobs client via property."""
        client = AdminClient('http://localhost:8080')
        jobs_client = client.jobs

        assert jobs_client is not None
        assert jobs_client._admin is client

    @respx.mock
    def test_schema_property(self):
        """Test accessing schema client via property."""
        client = AdminClient('http://localhost:8080')
        schema_client = client.schema

        assert schema_client is not None
        assert schema_client._admin is client
