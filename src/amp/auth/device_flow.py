"""OAuth2 Device Authorization Flow for Privy authentication.

Implements the device authorization grant flow with PKCE for secure authentication.
Matches the TypeScript CLI implementation.
"""

import base64
import hashlib
import secrets
import time
import webbrowser
from typing import Optional, Tuple

import httpx
from pydantic import BaseModel, Field

from .models import AuthStorage
from .service import AUTH_PLATFORM_URL


class DeviceAuthorizationResponse(BaseModel):
    """Response from device authorization endpoint."""

    device_code: str = Field(..., description='Device verification code for polling')
    user_code: str = Field(..., description='Code for user to enter in browser')
    verification_uri: str = Field(..., description='URL where user enters the code')
    expires_in: int = Field(..., description='Seconds until device code expires')
    interval: int = Field(..., description='Minimum polling interval in seconds')


class DeviceTokenResponse(BaseModel):
    """Response from device token endpoint (success case)."""

    access_token: str = Field(..., description='Access token for authenticated requests')
    refresh_token: str = Field(..., description='Refresh token for renewing access')
    user_id: str = Field(..., description='Authenticated user ID')
    user_accounts: list[str] = Field(..., description='List of user accounts/wallets')
    expires_in: int = Field(..., description='Seconds until token expires')


class DeviceTokenPendingResponse(BaseModel):
    """Response when authorization is still pending."""

    error: str = Field('authorization_pending', description='Error code')


class DeviceTokenExpiredResponse(BaseModel):
    """Response when device code has expired."""

    error: str = Field('expired_token', description='Error code')


def generate_pkce_pair() -> Tuple[str, str]:
    """Generate PKCE code_verifier and code_challenge.

    Returns:
        Tuple of (code_verifier, code_challenge)
    """
    # Generate cryptographically random code_verifier
    # Must be 43-128 characters using unreserved chars [A-Za-z0-9-._~]
    code_verifier_bytes = secrets.token_bytes(32)
    code_verifier = base64.urlsafe_b64encode(code_verifier_bytes).decode('utf-8').rstrip('=')

    # Generate code_challenge = BASE64URL(SHA256(code_verifier))
    challenge_bytes = hashlib.sha256(code_verifier.encode('utf-8')).digest()
    code_challenge = base64.urlsafe_b64encode(challenge_bytes).decode('utf-8').rstrip('=')

    return code_verifier, code_challenge


def request_device_authorization(http_client: httpx.Client) -> Tuple[DeviceAuthorizationResponse, str]:
    """Request device authorization from auth platform.

    Args:
        http_client: HTTP client to use for request

    Returns:
        Tuple of (DeviceAuthorizationResponse, code_verifier)

    Raises:
        httpx.HTTPStatusError: If request fails
        ValueError: If response is invalid
    """
    # Generate PKCE parameters
    code_verifier, code_challenge = generate_pkce_pair()

    # Request device authorization
    url = f'{AUTH_PLATFORM_URL}api/v1/device/authorize'
    response = http_client.post(
        url, json={'code_challenge': code_challenge, 'code_challenge_method': 'S256'}, timeout=30.0
    )

    if response.status_code != 200:
        raise ValueError(f'Device authorization failed: {response.status_code} - {response.text}')

    device_auth = DeviceAuthorizationResponse.model_validate(response.json())
    return device_auth, code_verifier


def poll_for_token(http_client: httpx.Client, device_code: str, code_verifier: str) -> Optional[DeviceTokenResponse]:
    """Poll device token endpoint once.

    Args:
        http_client: HTTP client to use for request
        device_code: Device code from authorization response
        code_verifier: PKCE code verifier

    Returns:
        DeviceTokenResponse if auth complete, None if still pending

    Raises:
        ValueError: If device code expired or other error
    """
    url = f'{AUTH_PLATFORM_URL}api/v1/device/token'
    response = http_client.get(url, params={'device_code': device_code, 'code_verifier': code_verifier}, timeout=10.0)

    data = response.json()

    # Check for error responses
    if 'error' in data:
        error = data['error']
        if error == 'authorization_pending':
            return None  # Still pending
        elif error == 'expired_token':
            raise ValueError('Device code expired. Please try again.')
        else:
            raise ValueError(f'Token polling error: {error}')

    # Success - parse token response
    return DeviceTokenResponse.model_validate(data)


def poll_until_authenticated(
    http_client: httpx.Client,
    device_code: str,
    code_verifier: str,
    interval: int,
    expires_in: int,
    on_poll: Optional[callable] = None,
    verbose: bool = False,
) -> DeviceTokenResponse:
    """Poll for token until authenticated or timeout.

    Args:
        http_client: HTTP client to use for requests
        device_code: Device code from authorization
        code_verifier: PKCE code verifier
        interval: Minimum polling interval in seconds
        expires_in: Seconds until device code expires
        on_poll: Optional callback called on each poll attempt

    Returns:
        DeviceTokenResponse when authentication completes

    Raises:
        ValueError: If authentication times out or fails
    """
    start_time = time.time()
    poll_count = 0
    max_polls = int(expires_in / interval) + 5  # Add some buffer

    while poll_count < max_polls:
        elapsed = time.time() - start_time
        if elapsed > expires_in:
            raise ValueError('Authentication timed out. Please try again.')

        if on_poll:
            on_poll(poll_count, elapsed)

        # Poll for token
        try:
            token_response = poll_for_token(http_client, device_code, code_verifier)
            if token_response:
                return token_response
        except ValueError as e:
            if 'expired' in str(e).lower():
                raise
            # Other errors, log and continue polling
            if verbose:
                print(f'\n⚠ Polling error (will retry): {e}')
            pass
        except Exception as e:
            # Log unexpected errors
            if verbose:
                print(f'\n⚠ Unexpected error (will retry): {type(e).__name__}: {e}')
            pass

        # Wait before next poll
        time.sleep(interval)
        poll_count += 1

    raise ValueError('Authentication timed out. Please try again.')


def open_browser(url: str) -> bool:
    """Open URL in browser.

    Args:
        url: URL to open

    Returns:
        True if browser opened successfully
    """
    try:
        webbrowser.open(url)
        return True
    except Exception:
        return False


def interactive_device_login(verbose: bool = True, auto_open_browser: bool = True) -> AuthStorage:
    """Perform interactive device authorization flow.

    Args:
        verbose: Print progress messages
        auto_open_browser: Automatically open browser for user

    Returns:
        AuthStorage with tokens

    Raises:
        ValueError: If authentication fails
    """
    http_client = httpx.Client()

    try:
        # Step 1: Request device authorization
        if verbose:
            print('🔐 Starting authentication...\n')

        device_auth, code_verifier = request_device_authorization(http_client)

        # Step 2: Display user code and open browser
        if verbose:
            print(f'📱 Verification Code: {device_auth.user_code}')
            print(f'🌐 Verification URL: {device_auth.verification_uri}\n')

        if auto_open_browser:
            if verbose:
                print('Opening browser...')
            if open_browser(device_auth.verification_uri):
                if verbose:
                    print('✓ Browser opened')
            else:
                if verbose:
                    print('✗ Could not open browser automatically')
                    print(f'  Please open: {device_auth.verification_uri}')

        if verbose:
            print(f'\n⏳ Waiting for authentication (expires in {device_auth.expires_in}s)...')
            print('   Complete the authentication in your browser.\n')

        # Step 3: Poll for token
        spinner_frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

        def poll_callback(count: int, elapsed: float):
            if verbose:
                spinner = spinner_frames[count % len(spinner_frames)]
                print(f'\r{spinner} Polling... ({int(elapsed)}s elapsed)', end='', flush=True)

        token_response = poll_until_authenticated(
            http_client,
            device_auth.device_code,
            code_verifier,
            device_auth.interval,
            device_auth.expires_in,
            poll_callback,
            verbose=verbose,
        )

        if verbose:
            print('\r✓ Authentication successful!     \n')

        # Step 4: Create auth storage
        now_ms = int(time.time() * 1000)
        expiry_ms = now_ms + (token_response.expires_in * 1000)

        auth_storage = AuthStorage(
            accessToken=token_response.access_token,
            refreshToken=token_response.refresh_token,
            userId=token_response.user_id,
            accounts=token_response.user_accounts,
            expiry=expiry_ms,
        )

        return auth_storage

    finally:
        http_client.close()
