"""Unit tests for admin error mapping."""

import pytest

from amp.admin.errors import (
    AdminAPIError,
    DatasetNotFoundError,
    InvalidManifestError,
    JobNotFoundError,
    SchedulerError,
    map_error_response,
)


class TestErrorMapping:
    """Test error response mapping to typed exceptions."""

    def test_map_dataset_not_found(self):
        """Test mapping DATASET_NOT_FOUND error code."""
        error_data = {'error_code': 'DATASET_NOT_FOUND', 'error_message': 'Dataset not found'}

        exc = map_error_response(404, error_data)

        assert isinstance(exc, DatasetNotFoundError)
        assert exc.error_code == 'DATASET_NOT_FOUND'
        assert exc.message == 'Dataset not found'
        assert exc.status_code == 404
        assert '[DATASET_NOT_FOUND]' in str(exc)

    def test_map_invalid_manifest(self):
        """Test mapping INVALID_MANIFEST error code."""
        error_data = {'error_code': 'INVALID_MANIFEST', 'error_message': 'Manifest validation failed'}

        exc = map_error_response(400, error_data)

        assert isinstance(exc, InvalidManifestError)
        assert exc.error_code == 'INVALID_MANIFEST'
        assert exc.status_code == 400

    def test_map_job_not_found(self):
        """Test mapping JOB_NOT_FOUND error code."""
        error_data = {'error_code': 'JOB_NOT_FOUND', 'error_message': 'Job 123 not found'}

        exc = map_error_response(404, error_data)

        assert isinstance(exc, JobNotFoundError)
        assert 'Job 123 not found' in exc.message

    def test_map_scheduler_error(self):
        """Test mapping SCHEDULER_ERROR error code."""
        error_data = {'error_code': 'SCHEDULER_ERROR', 'error_message': 'Failed to schedule job'}

        exc = map_error_response(500, error_data)

        assert isinstance(exc, SchedulerError)
        assert exc.status_code == 500

    def test_map_unknown_error_code(self):
        """Test mapping unknown error code falls back to base AdminAPIError."""
        error_data = {'error_code': 'UNKNOWN_ERROR', 'error_message': 'Something went wrong'}

        exc = map_error_response(500, error_data)

        assert isinstance(exc, AdminAPIError)
        assert not isinstance(exc, DatasetNotFoundError)  # Should be base class only
        assert exc.error_code == 'UNKNOWN_ERROR'

    def test_map_missing_error_code(self):
        """Test mapping when error_code is missing."""
        error_data = {'error_message': 'Error occurred'}

        exc = map_error_response(500, error_data)

        assert isinstance(exc, AdminAPIError)
        assert exc.error_code == 'UNKNOWN'
        assert exc.message == 'Error occurred'

    def test_map_missing_error_message(self):
        """Test mapping when error_message is missing."""
        error_data = {'error_code': 'DATASET_NOT_FOUND'}

        exc = map_error_response(404, error_data)

        assert isinstance(exc, DatasetNotFoundError)
        assert exc.message == 'Unknown error'


class TestAdminAPIError:
    """Test AdminAPIError base class."""

    def test_error_initialization(self):
        """Test error initialization with all fields."""
        error = AdminAPIError('TEST_ERROR', 'Test message', 404)

        assert error.error_code == 'TEST_ERROR'
        assert error.message == 'Test message'
        assert error.status_code == 404

    def test_error_string_representation(self):
        """Test error string includes error code and message."""
        error = AdminAPIError('TEST_ERROR', 'Test message', 404)

        error_str = str(error)
        assert 'TEST_ERROR' in error_str
        assert 'Test message' in error_str

    def test_error_is_exception(self):
        """Test that AdminAPIError is an Exception."""
        error = AdminAPIError('TEST_ERROR', 'Test message', 404)

        assert isinstance(error, Exception)
        with pytest.raises(AdminAPIError):
            raise error
