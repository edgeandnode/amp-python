"""Registry API client."""

import logging
from typing import Optional

import httpx

from . import errors

logger = logging.getLogger(__name__)


class RegistryClient:
    """Client for interacting with the Amp Registry API.

    The Registry API provides dataset discovery, search, and publishing capabilities.

    Args:
        base_url: Base URL for the Registry API (default: staging registry)
        auth_token: Optional Bearer token for authenticated operations

    Example:
        >>> # Read-only operations (no auth required)
        >>> client = RegistryClient()
        >>> datasets = client.datasets.search('ethereum')
        >>>
        >>> # Authenticated operations (publishing, etc.)
        >>> client = RegistryClient(auth_token='your-token')
        >>> client.datasets.publish(...)
    """

    def __init__(
        self,
        base_url: str = 'https://api.registry.amp.staging.thegraph.com',
        auth_token: Optional[str] = None,
    ):
        """Initialize Registry client.

        Args:
            base_url: Base URL for the Registry API
            auth_token: Optional Bearer token for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.auth_token = auth_token

        # Build headers
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        if auth_token:
            headers['Authorization'] = f'Bearer {auth_token}'

        # Create HTTP client
        self._http = httpx.Client(
            base_url=self.base_url,
            headers=headers,
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
