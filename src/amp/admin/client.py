"""Base HTTP client for Amp Admin API.

This module provides the core AdminClient class for communicating
with the Amp Admin API over HTTP.
"""

import os
from typing import Optional

import httpx

from .errors import map_error_response


class AdminClient:
    """HTTP client for Amp Admin API.

    Provides access to Admin API endpoints through sub-clients for
    datasets, jobs, and schema operations.

    Args:
        base_url: Base URL for Admin API (e.g., 'http://localhost:8080')
        auth_token: Optional Bearer token for authentication (highest priority)
        auth: If True, load auth token from ~/.amp-cli-config (shared with TS CLI)

    Authentication Priority (highest to lowest):
        1. Explicit auth_token parameter
        2. AMP_AUTH_TOKEN environment variable
        3. auth=True - reads from ~/.amp-cli-config/amp_cli_auth

    Example:
        >>> # Use amp auth from file
        >>> client = AdminClient('http://localhost:8080', auth=True)
        >>>
        >>> # Use manual token
        >>> client = AdminClient('http://localhost:8080', auth_token='your-token')
        >>>
        >>> # Use environment variable
        >>> # export AMP_AUTH_TOKEN="eyJhbGci..."
        >>> client = AdminClient('http://localhost:8080')
    """

    def __init__(self, base_url: str, auth_token: Optional[str] = None, auth: bool = False):
        """Initialize Admin API client.

        Args:
            base_url: Base URL for Admin API (e.g., 'http://localhost:8080')
            auth_token: Optional Bearer token for authentication
            auth: If True, load auth token from ~/.amp-cli-config

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

        # Create HTTP client (no auth header yet - will be added per-request)
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=30.0,
            follow_redirects=True,
        )

    def _request(
        self, method: str, path: str, json: Optional[dict] = None, params: Optional[dict] = None, **kwargs
    ) -> httpx.Response:
        """Make HTTP request with error handling.

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

        response = self._http.request(method, path, json=json, params=params, **kwargs)

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
        """Access datasets client.

        Returns:
            DatasetsClient for dataset operations
        """
        from .datasets import DatasetsClient

        return DatasetsClient(self)

    @property
    def jobs(self):
        """Access jobs client.

        Returns:
            JobsClient for job operations
        """
        from .jobs import JobsClient

        return JobsClient(self)

    @property
    def schema(self):
        """Access schema client.

        Returns:
            SchemaClient for schema operations
        """
        from .schema import SchemaClient

        return SchemaClient(self)

    def close(self):
        """Close the HTTP client and release resources.

        Example:
            >>> client = AdminClient('http://localhost:8080')
            >>> try:
            ...     datasets = client.datasets.list_all()
            ... finally:
            ...     client.close()
        """
        self._http.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
