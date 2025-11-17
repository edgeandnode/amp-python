"""Unit tests for Privy authentication service."""

import json
import time
from unittest.mock import Mock, patch

import pytest

from src.amp.auth.models import AuthStorage
from src.amp.auth.service import AuthService


@pytest.mark.unit
class TestAuthService:
    """Test AuthService functionality."""

    def test_is_authenticated_when_no_file(self, tmp_path):
        """Test is_authenticated returns False when config file doesn't exist."""
        config_path = tmp_path / 'nonexistent.json'
        auth = AuthService(config_path=config_path)

        assert auth.is_authenticated() is False

    def test_is_authenticated_when_no_auth_data(self, tmp_path):
        """Test is_authenticated returns False when config exists but has no auth data."""
        config_path = tmp_path / 'config.json'
        config_path.write_text('{}')

        auth = AuthService(config_path=config_path)

        assert auth.is_authenticated() is False

    def test_is_authenticated_when_auth_exists(self, tmp_path):
        """Test is_authenticated returns True when valid auth exists."""
        config_path = tmp_path / 'amp_cli_auth'
        auth_data = {
            'accessToken': 'test-access-token',
            'refreshToken': 'test-refresh-token',
            'userId': 'did:privy:test123',
            'accounts': ['0x123'],
            'expiry': int(time.time() * 1000) + 3600000,  # 1 hour from now
        }
        config_path.write_text(json.dumps(auth_data))

        auth = AuthService(config_path=config_path)

        assert auth.is_authenticated() is True

    def test_load_auth_success(self, tmp_path):
        """Test loading auth from config file."""
        config_path = tmp_path / 'amp_cli_auth'
        auth_data = {
            'accessToken': 'test-access-token',
            'refreshToken': 'test-refresh-token',
            'userId': 'did:privy:test123',
            'accounts': ['0x123'],
            'expiry': 1234567890000,
        }
        config_path.write_text(json.dumps(auth_data))

        auth = AuthService(config_path=config_path)
        result = auth.load_auth()

        assert result is not None
        assert result.accessToken == 'test-access-token'
        assert result.refreshToken == 'test-refresh-token'
        assert result.userId == 'did:privy:test123'
        assert result.accounts == ['0x123']
        assert result.expiry == 1234567890000

    def test_load_auth_returns_none_when_missing(self, tmp_path):
        """Test load_auth returns None when config doesn't exist."""
        config_path = tmp_path / 'nonexistent.json'
        auth = AuthService(config_path=config_path)

        result = auth.load_auth()

        assert result is None

    def test_save_auth(self, tmp_path):
        """Test saving auth to config file."""
        config_path = tmp_path / 'amp_cli_auth'
        auth = AuthService(config_path=config_path)

        auth_storage = AuthStorage(
            accessToken='new-access-token',
            refreshToken='new-refresh-token',
            userId='did:privy:test456',
            accounts=['0x456'],
            expiry=9876543210000,
        )

        auth.save_auth(auth_storage)

        # Verify file was created
        assert config_path.exists()

        # Verify content
        with open(config_path) as f:
            saved_data = json.load(f)

        assert saved_data['accessToken'] == 'new-access-token'
        assert saved_data['userId'] == 'did:privy:test456'

    def test_save_auth_overwrites_existing(self, tmp_path):
        """Test saving auth overwrites existing auth data."""
        config_path = tmp_path / 'amp_cli_auth'
        initial_data = {
            'accessToken': 'old-token',
            'refreshToken': 'old-refresh',
            'userId': 'did:privy:old',
            'accounts': [],
            'expiry': 12345,
        }
        config_path.write_text(json.dumps(initial_data))

        auth = AuthService(config_path=config_path)
        auth_storage = AuthStorage(
            accessToken='new-token',
            refreshToken='new-refresh',
            userId='did:privy:new',
            accounts=['0x123'],
            expiry=67890,
        )

        auth.save_auth(auth_storage)

        # Verify new data saved
        with open(config_path) as f:
            saved_data = json.load(f)

        assert saved_data['accessToken'] == 'new-token'
        assert saved_data['userId'] == 'did:privy:new'
        assert saved_data['accounts'] == ['0x123']

    def test_get_token_raises_when_not_authenticated(self, tmp_path):
        """Test get_token raises error when not authenticated."""
        config_path = tmp_path / 'nonexistent.json'
        auth = AuthService(config_path=config_path)

        with pytest.raises(FileNotFoundError, match='Not authenticated'):
            auth.get_token()

    def test_get_token_returns_valid_token(self, tmp_path):
        """Test get_token returns token when valid and not expired."""
        config_path = tmp_path / 'amp_cli_auth'
        future_expiry = int(time.time() * 1000) + 3600000  # 1 hour from now
        auth_data = {
            'accessToken': 'valid-token',
            'refreshToken': 'refresh-token',
            'userId': 'did:privy:test',
            'accounts': ['0x123'],
            'expiry': future_expiry,
        }
        config_path.write_text(json.dumps(auth_data))

        auth = AuthService(config_path=config_path)
        token = auth.get_token()

        assert token == 'valid-token'


@pytest.mark.unit
class TestAuthServiceRefresh:
    """Test token refresh functionality."""

    def test_needs_refresh_when_missing_expiry(self):
        """Test needs refresh when expiry field is missing."""
        auth = AuthService()
        auth_storage = AuthStorage(
            accessToken='token',
            refreshToken='refresh',
            userId='did:privy:test',
            accounts=['0x123'],
            expiry=None,  # Missing expiry
        )

        assert auth._needs_refresh(auth_storage) is True

    def test_needs_refresh_when_missing_accounts(self):
        """Test needs refresh when accounts field is missing."""
        auth = AuthService()
        auth_storage = AuthStorage(
            accessToken='token',
            refreshToken='refresh',
            userId='did:privy:test',
            accounts=None,  # Missing accounts
            expiry=int(time.time() * 1000) + 3600000,
        )

        assert auth._needs_refresh(auth_storage) is True

    def test_needs_refresh_when_expired(self):
        """Test needs refresh when token is expired."""
        auth = AuthService()
        past_expiry = int(time.time() * 1000) - 1000  # 1 second ago
        auth_storage = AuthStorage(
            accessToken='token',
            refreshToken='refresh',
            userId='did:privy:test',
            accounts=['0x123'],
            expiry=past_expiry,
        )

        assert auth._needs_refresh(auth_storage) is True

    def test_needs_refresh_when_expiring_soon(self):
        """Test needs refresh when token expires within 5 minutes."""
        auth = AuthService()
        soon_expiry = int(time.time() * 1000) + (4 * 60 * 1000)  # 4 minutes from now
        auth_storage = AuthStorage(
            accessToken='token',
            refreshToken='refresh',
            userId='did:privy:test',
            accounts=['0x123'],
            expiry=soon_expiry,
        )

        assert auth._needs_refresh(auth_storage) is True

    def test_does_not_need_refresh_when_valid(self):
        """Test does not need refresh when token is valid and not expiring soon."""
        auth = AuthService()
        future_expiry = int(time.time() * 1000) + (10 * 60 * 1000)  # 10 minutes from now
        auth_storage = AuthStorage(
            accessToken='token',
            refreshToken='refresh',
            userId='did:privy:test',
            accounts=['0x123'],
            expiry=future_expiry,
        )

        assert auth._needs_refresh(auth_storage) is False

    @patch('httpx.Client.post')
    def test_refresh_token_success(self, mock_post, tmp_path):
        """Test successful token refresh."""
        config_path = tmp_path / 'config.json'

        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'token': 'new-access-token',
            'refresh_token': 'new-refresh-token',
            'session_update_action': 'update',
            'expires_in': 3600,
            'user': {'id': 'did:privy:test', 'accounts': ['0x123', '0x456']},
        }
        mock_post.return_value = mock_response

        auth = AuthService(config_path=config_path)
        old_auth = AuthStorage(
            accessToken='old-token',
            refreshToken='old-refresh',
            userId='did:privy:test',
            accounts=['0x123'],
            expiry=12345,
        )

        new_auth = auth.refresh_token(old_auth)

        # Verify new tokens
        assert new_auth.accessToken == 'new-access-token'
        assert new_auth.refreshToken == 'new-refresh-token'
        assert new_auth.userId == 'did:privy:test'
        assert new_auth.accounts == ['0x123', '0x456']
        assert new_auth.expiry > int(time.time() * 1000)

        # Verify saved to file
        assert config_path.exists()

    @patch('httpx.Client.post')
    def test_refresh_token_user_id_mismatch(self, mock_post, tmp_path):
        """Test refresh fails when user ID doesn't match."""
        config_path = tmp_path / 'config.json'

        # Mock HTTP response with different user ID
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'token': 'new-token',
            'refresh_token': 'new-refresh',
            'session_update_action': 'update',
            'expires_in': 3600,
            'user': {'id': 'did:privy:different-user', 'accounts': []},
        }
        mock_post.return_value = mock_response

        auth = AuthService(config_path=config_path)
        old_auth = AuthStorage(
            accessToken='old-token',
            refreshToken='old-refresh',
            userId='did:privy:original-user',
            accounts=[],
            expiry=12345,
        )

        with pytest.raises(ValueError, match='User ID mismatch'):
            auth.refresh_token(old_auth)

    @patch('httpx.Client.post')
    def test_refresh_token_401_error(self, mock_post, tmp_path):
        """Test refresh handles 401 authentication error."""
        config_path = tmp_path / 'config.json'

        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        auth = AuthService(config_path=config_path)
        old_auth = AuthStorage(
            accessToken='old-token',
            refreshToken='old-refresh',
            userId='did:privy:test',
            accounts=[],
            expiry=12345,
        )

        with pytest.raises(ValueError, match='Authentication expired'):
            auth.refresh_token(old_auth)

    @patch('httpx.Client.post')
    def test_refresh_token_429_rate_limit(self, mock_post, tmp_path):
        """Test refresh handles 429 rate limit error."""
        config_path = tmp_path / 'config.json'

        # Mock 429 response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {'retry-after': '120'}
        mock_post.return_value = mock_response

        auth = AuthService(config_path=config_path)
        old_auth = AuthStorage(
            accessToken='old-token',
            refreshToken='old-refresh',
            userId='did:privy:test',
            accounts=[],
            expiry=12345,
        )

        with pytest.raises(ValueError, match='rate limited'):
            auth.refresh_token(old_auth)
