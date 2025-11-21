"""Auth service for managing Privy authentication.

Handles loading, refreshing, and persisting auth tokens from ~/.amp/cache.
Compatible with the TypeScript CLI authentication system.
"""

import json
import time
from pathlib import Path
from typing import Optional

import httpx

from .models import AuthStorage, RefreshTokenResponse

# Auth platform URL (matches TypeScript implementation)
AUTH_PLATFORM_URL = 'https://auth.amp.thegraph.com/'

# Storage location (matches TypeScript implementation)
# TypeScript CLI uses: ~/.amp/cache/amp_cli_auth (directory with file inside)
AUTH_CONFIG_DIR = Path.home() / '.amp' / 'cache'
AUTH_CONFIG_FILE = AUTH_CONFIG_DIR / 'amp_cli_auth'


class AuthService:
    """Service for managing Privy authentication tokens.

    Loads tokens from ~/.amp/cache (shared with TypeScript CLI),
    automatically refreshes expired tokens, and persists updates.

    Example:
        >>> auth = AuthService()
        >>> if auth.is_authenticated():
        ...     token = auth.get_token()  # Auto-refreshes if needed
    """

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize auth service.

        Args:
            config_path: Optional custom path to config file (defaults to ~/.amp/cache/amp_cli_auth)
        """
        self.config_path = config_path or AUTH_CONFIG_FILE
        self._http = httpx.Client(timeout=30.0)

    def is_authenticated(self) -> bool:
        """Check if user is authenticated.

        Returns:
            True if valid auth exists in ~/.amp/cache
        """
        try:
            auth = self.load_auth()
            return auth is not None
        except Exception:
            return False

    def load_auth(self) -> Optional[AuthStorage]:
        """Load auth from ~/.amp/cache/amp_cli_auth file.

        Returns:
            AuthStorage if found, None if not authenticated

        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
            ValueError: If auth data is invalid
        """
        if not self.config_path.exists():
            return None

        with open(self.config_path) as f:
            auth_data = json.load(f)

        return AuthStorage.model_validate(auth_data)

    def save_auth(self, auth: AuthStorage) -> None:
        """Save auth to ~/.amp/cache/amp_cli_auth file.

        Args:
            auth: Auth data to persist

        Raises:
            IOError: If unable to write to config file
        """
        # Ensure directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write auth data directly to file (no wrapper object)
        with open(self.config_path, 'w') as f:
            json.dump(auth.model_dump(mode='json', exclude_none=False), f, indent=2)

    def get_token(self) -> str:
        """Get valid access token, refreshing if needed.

        Returns:
            Valid access token string

        Raises:
            FileNotFoundError: If not authenticated (no ~/.amp/cache)
            ValueError: If auth data is invalid or refresh fails
        """
        auth = self.load_auth()
        if not auth:
            raise FileNotFoundError(
                'Not authenticated. Please run authentication first.\n'
                "Use the TypeScript CLI 'amp auth login' or authenticate via the Python client."
            )

        # Check if we need to refresh the token
        needs_refresh = self._needs_refresh(auth)

        if needs_refresh:
            auth = self.refresh_token(auth)

        return auth.accessToken

    def _needs_refresh(self, auth: AuthStorage) -> bool:
        """Check if token needs to be refreshed.

        Token needs refresh if:
        - Missing expiry field (old format, need to refresh to populate)
        - Missing accounts field (old format, need to refresh to populate)
        - Token is expired
        - Token is expiring within 5 minutes

        Args:
            auth: Auth storage to check

        Returns:
            True if token needs refresh
        """
        # Missing expiry or accounts - refresh to populate
        if auth.expiry is None or auth.accounts is None:
            return True

        # Get current time in milliseconds (matching TypeScript)
        now_ms = int(time.time() * 1000)

        # Token is expired
        if auth.expiry < now_ms:
            return True

        # Token is expiring within 5 minutes
        five_minutes_ms = 5 * 60 * 1000
        if auth.expiry - now_ms <= five_minutes_ms:
            return True

        return False

    def refresh_token(self, auth: AuthStorage) -> AuthStorage:
        """Refresh an expired or expiring access token.

        Args:
            auth: Current auth storage with refresh token

        Returns:
            Updated auth storage with new tokens

        Raises:
            httpx.HTTPStatusError: If refresh request fails
            ValueError: If response validation fails or user ID mismatch
        """
        # Build refresh request (matches TypeScript implementation)
        url = f'{AUTH_PLATFORM_URL}api/v1/auth/refresh'
        headers = {
            'Authorization': f'Bearer {auth.accessToken}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        body = {'refresh_token': auth.refreshToken, 'user_id': auth.userId}

        # Make request
        response = self._http.post(url, headers=headers, json=body)

        # Handle errors
        if response.status_code == 401 or response.status_code == 403:
            raise ValueError('Token refresh failed: Authentication expired. Please log in again.')

        if response.status_code == 429:
            retry_after = response.headers.get('retry-after', '60')
            raise ValueError(f'Token refresh rate limited. Retry after {retry_after} seconds.')

        if response.status_code != 200:
            error_msg = 'Failed to refresh token'
            try:
                error_data = response.json()
                if 'error_description' in error_data:
                    error_msg = error_data['error_description']
            except Exception:
                pass
            raise ValueError(f'{error_msg} (status: {response.status_code})')

        # Parse response
        refresh_response = RefreshTokenResponse.model_validate(response.json())

        # Validate user ID matches (security check)
        if refresh_response.user.id != auth.userId:
            raise ValueError(f'User ID mismatch after refresh. Expected {auth.userId}, got {refresh_response.user.id}')

        # Calculate new expiry
        now_ms = int(time.time() * 1000)
        expiry_ms = now_ms + (refresh_response.expires_in * 1000)

        # Create updated auth storage
        refreshed_auth = AuthStorage(
            accessToken=refresh_response.token,
            refreshToken=refresh_response.refresh_token or auth.refreshToken,
            userId=refresh_response.user.id,
            accounts=refresh_response.user.accounts,
            expiry=expiry_ms,
        )

        # Persist updated tokens
        self.save_auth(refreshed_auth)

        return refreshed_auth

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._http.close()

    def login(self, verbose: bool = True, auto_open_browser: bool = True) -> None:
        """Perform interactive browser-based login.

        Opens browser for OAuth2 device authorization flow with PKCE.
        Saves authentication tokens to ~/.amp/cache/amp_cli_auth.

        Args:
            verbose: Print progress messages
            auto_open_browser: Automatically open browser

        Raises:
            ValueError: If authentication fails

        Example:
            >>> auth = AuthService()
            >>> auth.login()  # Opens browser for authentication
            >>> # Auth tokens saved to ~/.amp/cache/amp_cli_auth
        """
        from .device_flow import interactive_device_login

        # Perform device authorization flow
        auth_storage = interactive_device_login(verbose=verbose, auto_open_browser=auto_open_browser)

        # Save to config file
        self.save_auth(auth_storage)

        if verbose:
            print(f'✓ Authentication saved to {self.config_path}')
