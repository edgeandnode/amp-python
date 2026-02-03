"""Async HTTP client for Amp Admin API.

This module provides the async AdminClient class for communicating
with the Amp Admin API over HTTP using asyncio and httpx.
"""

import os
from typing import Optional

import httpx

from .errors import map_error_response


class AsyncAdminClient:
    """Async HTTP client for Amp Admin API.

    Provides access to Admin API endpoints through sub-clients for
    datasets, jobs, and schema operations using async/await.

    Args:
        base_url: Base URL for Admin API (e.g., 'http://localhost:8080')
        auth_token: Optional Bearer token for authentication (highest priority)
        auth: If True, load auth token from ~/.amp/cache (shared with TS CLI)

    Authentication Priority (highest to lowest):
        1. Explicit auth_token parameter
        2. AMP_AUTH_TOKEN environment variable
        3. auth=True - reads from ~/.amp/cache/amp_cli_auth

    Example:
        >>> # Use amp auth from file
        >>> async with AsyncAdminClient('http://localhost:8080', auth=True) as client:
        ...     datasets = await client.datasets.list_all()
        >>>
        >>> # Use manual token
        >>> async with AsyncAdminClient('http://localhost:8080', auth_token='your-token') as client:
        ...     job = await client.jobs.get(123)
    """

    def __init__(self, base_url: str, auth_token: Optional[str] = None, auth: bool = False):
        """Initialize async Admin API client.

        Args:
            base_url: Base URL for Admin API (e.g., 'http://localhost:8080')
            auth_token: Optional Bearer token for authentication
            auth: If True, load auth token from ~/.amp/cache

        Raises:
            ValueError: If both auth=True and auth_token are provided
        """
        if auth and auth_token:
            raise ValueError('Cannot specify both auth=True and auth_token. Choose one authentication method.')

        self.base_url = base_url.rstrip('/')

        # Resolve auth token provider with priority: explicit param > env var > auth file
        self._get_token = None
        if auth_token:
            # Priority 1: Explicit auth_token parameter (static token)
            self._get_token = lambda: auth_token
        elif os.getenv('AMP_AUTH_TOKEN'):
            # Priority 2: AMP_AUTH_TOKEN environment variable (static token)
            env_token = os.getenv('AMP_AUTH_TOKEN')
            self._get_token = lambda: env_token
        elif auth:
            # Priority 3: Load from ~/.amp-cli-config/amp_cli_auth (auto-refreshing)
            from amp.auth import AuthService

            auth_service = AuthService()
            self._get_token = auth_service.get_token  # Callable that auto-refreshes

        # Create async HTTP client (no auth header yet - will be added per-request)
        self._http = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            follow_redirects=True,
        )

    async def _request(
        self, method: str, path: str, json: Optional[dict] = None, params: Optional[dict] = None, **kwargs
    ) -> httpx.Response:
        """Make async HTTP request with error handling.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            path: API endpoint path (e.g., '/datasets')
            json: Optional JSON request body
            params: Optional query parameters
            **kwargs: Additional arguments passed to httpx.request()

        Returns:
            HTTP response object

        Raises:
            AdminAPIError: If the API returns an error response
        """
        # Add auth header dynamically (auto-refreshes if needed)
        headers = kwargs.get('headers', {})
        if self._get_token:
            headers['Authorization'] = f'Bearer {self._get_token()}'
            kwargs['headers'] = headers

        response = await self._http.request(method, path, json=json, params=params, **kwargs)

        # Handle error responses
        if response.status_code >= 400:
            try:
                error_data = response.json()
                raise map_error_response(response.status_code, error_data)
            except ValueError:
                # Response is not JSON, fall back to generic HTTP error
                response.raise_for_status()

        return response

    @property
    def datasets(self):
        """Access async datasets client.

        Returns:
            AsyncDatasetsClient for dataset operations
        """
        from .async_datasets import AsyncDatasetsClient

        return AsyncDatasetsClient(self)

    @property
    def jobs(self):
        """Access async jobs client.

        Returns:
            AsyncJobsClient for job operations
        """
        from .async_jobs import AsyncJobsClient

        return AsyncJobsClient(self)

    @property
    def schema(self):
        """Access async schema client.

        Returns:
            AsyncSchemaClient for schema operations
        """
        from .async_schema import AsyncSchemaClient

        return AsyncSchemaClient(self)

    async def close(self):
        """Close the HTTP client and release resources.

        Example:
            >>> client = AsyncAdminClient('http://localhost:8080')
            >>> try:
            ...     datasets = await client.datasets.list_all()
            ... finally:
            ...     await client.close()
        """
        await self._http.aclose()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
