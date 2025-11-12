"""Data models for Privy authentication.

Models match the TypeScript CLI schema for compatibility.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class AuthStorage(BaseModel):
    """Auth storage schema for ~/.amp-cli-config file.

    Matches the TypeScript AuthStorageSchema so they can share auth.
    """

    accessToken: str = Field(..., description='Access token for authenticated requests')
    refreshToken: str = Field(..., description='Refresh token for renewing access')
    userId: str = Field(..., description="User's Privy DID (format: did:privy:c...)")
    accounts: Optional[List[str]] = Field(None, description='List of connected accounts/wallets')
    expiry: Optional[int] = Field(None, description='Token expiry timestamp in milliseconds')


class RefreshTokenResponseUser(BaseModel):
    """User information in the refresh token response."""

    id: str = Field(..., description='User ID (Privy DID)')
    accounts: List[str] = Field(..., description='List of connected accounts/wallets')


class RefreshTokenResponse(BaseModel):
    """Response from the token refresh endpoint.

    Matches the TypeScript RefreshTokenResponse schema.
    """

    token: str = Field(..., description='The refreshed access token')
    refresh_token: Optional[str] = Field(None, description='New refresh token (if rotated)')
    session_update_action: str = Field(..., description='Session update action')
    expires_in: int = Field(..., description='Seconds until token expires')
    user: RefreshTokenResponseUser = Field(..., description='User information')
