"""Registry API client."""

import logging
import os
from typing import Optional

import httpx

from . import errors

logger = logging.getLogger(__name__)


class RegistryClient:
    """Client for interacting with the Amp Registry API.

    The Registry API provides dataset discovery, search, and publishing capabilities.

    Args:
        base_url: Base URL for the Registry API (default: staging registry)
        auth_token: Optional Bearer token for authenticated operations (highest priority)
        auth: If True, load auth token from ~/.amp/cache (shared with TS CLI)

    Authentication Priority (highest to lowest):
        1. Explicit auth_token parameter
        2. AMP_AUTH_TOKEN environment variable
        3. auth=True - reads from ~/.amp/cache/amp_cli_auth

    Example:
        >>> # Read-only operations (no auth required)
        >>> client = RegistryClient()
        >>> datasets = client.datasets.search('ethereum')
        >>>
        >>> # Authenticated operations with explicit token
        >>> client = RegistryClient(auth_token='your-token')
        >>> client.datasets.publish(...)
        >>>
        >>> # Authenticated operations with auth file (auto-refresh)
        >>> client = RegistryClient(auth=True)
        >>> client.datasets.publish(...)
    """

    def __init__(
        self,
        base_url: str = 'https://api.registry.amp.staging.thegraph.com',
        auth_token: Optional[str] = None,
        auth: bool = False,
    ):
        """Initialize Registry client.

        Args:
            base_url: Base URL for the Registry API
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
            def get_token():
                return auth_token

            self._get_token = get_token
        elif os.getenv('AMP_AUTH_TOKEN'):
            # Priority 2: AMP_AUTH_TOKEN environment variable (static token)
            env_token = os.getenv('AMP_AUTH_TOKEN')

            def get_token():
                return env_token

            self._get_token = get_token
        elif auth:
            # Priority 3: Load from ~/.amp/cache/amp_cli_auth (auto-refreshing)
            from amp.auth import AuthService

            auth_service = AuthService()
            self._get_token = auth_service.get_token  # Callable that auto-refreshes

        # Create HTTP client (no auth header yet - will be added per-request)
        self._http = httpx.Client(
            base_url=self.base_url,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            timeout=30.0,
        )

        logger.info(f'Initialized Registry client for {base_url}')

    @property
    def datasets(self):
        """Access the datasets client.

        Returns:
            RegistryDatasetsClient: Client for dataset operations
        """
        from .datasets import RegistryDatasetsClient

        return RegistryDatasetsClient(self)

    def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request to the Registry API.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path (without base URL)
            **kwargs: Additional arguments to pass to httpx

        Returns:
            httpx.Response: HTTP response

        Raises:
            RegistryError: If the request fails
        """
        url = path if path.startswith('http') else f'{self.base_url}{path}'

        # Add auth header dynamically (auto-refreshes if needed)
        headers = kwargs.get('headers', {})
        if self._get_token:
            headers['Authorization'] = f'Bearer {self._get_token()}'
            kwargs['headers'] = headers

        try:
            response = self._http.request(method, url, **kwargs)

            # Handle error responses
            if response.status_code >= 400:
                self._handle_error(response)

            return response

        except httpx.RequestError as e:
            raise errors.RegistryError(f'Request failed: {e}') from e

    def _handle_error(self, response: httpx.Response) -> None:
        """Handle error responses from the API.

        Args:
            response: HTTP error response

        Raises:
            RegistryError: Mapped exception for the error
        """
        try:
            error_data = response.json()
            error_code = error_data.get('error_code', '')
            error_message = error_data.get('error_message', response.text)
            request_id = error_data.get('request_id', '')

            # Map to specific exception
            raise errors.map_error(error_code, error_message, request_id)

        except (ValueError, KeyError):
            # Couldn't parse error response, raise generic error
            raise errors.RegistryError(
                f'HTTP {response.status_code}: {response.text}',
                error_code=str(response.status_code),
            ) from None

    def close(self):
        """Close the HTTP client."""
        self._http.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
