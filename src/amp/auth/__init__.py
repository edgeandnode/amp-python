"""Authentication module for amp Python client.

Provides Privy authentication support compatible with the TypeScript CLI.
Reads and manages auth tokens from ~/.amp/cache.
"""

from .device_flow import interactive_device_login
from .models import AuthStorage, RefreshTokenResponse
from .service import AuthService

__all__ = ['AuthService', 'AuthStorage', 'RefreshTokenResponse', 'interactive_device_login']
