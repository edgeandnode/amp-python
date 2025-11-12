"""Authentication module for amp Python client.

Provides Privy authentication support compatible with the TypeScript CLI.
Reads and manages auth tokens from ~/.amp-cli-config.
"""

from .models import AuthStorage, RefreshTokenResponse
from .service import AuthService

__all__ = ['AuthService', 'AuthStorage', 'RefreshTokenResponse']
