"""Exception hierarchy for Admin API errors.

This module defines typed exceptions for Admin API error responses,
mapped from the error_code field in API responses.
"""


class AdminAPIError(Exception):
    """Base exception for Admin API errors.

    Attributes:
        error_code: Machine-readable error code (SCREAMING_SNAKE_CASE)
        message: Human-readable error description
        status_code: HTTP status code
    """

    def __init__(self, error_code: str, message: str, status_code: int):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code
        super().__init__(f'[{error_code}] {message}')


# Dataset-related errors
class InvalidPayloadError(AdminAPIError):
    """Request JSON is malformed or invalid."""

    pass


class InvalidManifestError(AdminAPIError):
    """Manifest JSON parsing or structure error."""

    pass


class DatasetNotFoundError(AdminAPIError):
    """Requested dataset does not exist."""

    pass


class DependencyValidationError(AdminAPIError):
    """SQL queries are invalid or reference undeclared dependencies."""

    pass


class ManifestRegistrationError(AdminAPIError):
    """Failed to register manifest in system."""

    pass


class ManifestLinkingError(AdminAPIError):
    """Failed to link manifest to dataset."""

    pass


class ManifestNotFoundError(AdminAPIError):
    """Manifest hash provided but manifest doesn't exist."""

    pass


class VersionTaggingError(AdminAPIError):
    """Failed to tag the manifest with the version."""

    pass


class UnsupportedDatasetKindError(AdminAPIError):
    """Dataset kind is not supported."""

    pass


class StoreError(AdminAPIError):
    """Failed to load or access dataset store."""

    pass


class UnlinkDatasetManifestsError(AdminAPIError):
    """Failed to unlink dataset manifests from dataset store."""

    pass


class ListAllDatasetsError(AdminAPIError):
    """Failed to list all datasets from dataset store."""

    pass


class ListDatasetVersionsError(AdminAPIError):
    """Failed to list dataset versions from dataset store."""

    pass


class GetDatasetVersionError(AdminAPIError):
    """Failed to get dataset version from dataset store."""

    pass


class GetManifestError(AdminAPIError):
    """Failed to retrieve manifest from dataset store."""

    pass


# Job-related errors
class JobNotFoundError(AdminAPIError):
    """Requested job does not exist."""

    pass


class ListJobsError(AdminAPIError):
    """Failed to list jobs."""

    pass


class SchedulerError(AdminAPIError):
    """Failed to schedule or manage extraction job."""

    pass


class JobStopError(AdminAPIError):
    """Failed to stop job."""

    pass


class JobDeleteError(AdminAPIError):
    """Failed to delete job (may be in non-terminal state)."""

    pass


class JobsDeleteError(AdminAPIError):
    """Failed to delete multiple jobs."""

    pass


# Location-related errors
class LocationNotFoundError(AdminAPIError):
    """Requested location does not exist."""

    pass


class ListLocationsError(AdminAPIError):
    """Failed to list storage locations."""

    pass


class DeleteLocationError(AdminAPIError):
    """Failed to delete location."""

    pass


# File-related errors
class ListLocationFilesError(AdminAPIError):
    """Failed to list files for location."""

    pass


class FileNotFoundError(AdminAPIError):
    """Requested file does not exist."""

    pass


class GetFileInfoError(AdminAPIError):
    """Failed to retrieve file information."""

    pass


# Provider-related errors
class ProviderNotFoundError(AdminAPIError):
    """Requested provider does not exist."""

    pass


class CreateProviderError(AdminAPIError):
    """Failed to create provider configuration."""

    pass


class ListProvidersError(AdminAPIError):
    """Failed to list providers."""

    pass


class DeleteProviderError(AdminAPIError):
    """Failed to delete provider."""

    pass


# Schema-related errors
class GetOutputSchemaError(AdminAPIError):
    """Failed to get output schema for SQL query."""

    pass


# General errors
class InvalidPathError(AdminAPIError):
    """Invalid path parameters."""

    pass


class DatabaseError(AdminAPIError):
    """Database operation error."""

    pass


class InternalServerError(AdminAPIError):
    """Internal server error."""

    pass


def map_error_response(status_code: int, error_data: dict) -> AdminAPIError:
    """Map API error response to typed exception.

    Args:
        status_code: HTTP status code
        error_data: Error response JSON with error_code and error_message

    Returns:
        Appropriate AdminAPIError subclass instance

    Example:
        >>> error_data = {'error_code': 'DATASET_NOT_FOUND', 'error_message': 'Dataset not found'}
        >>> exc = map_error_response(404, error_data)
        >>> isinstance(exc, DatasetNotFoundError)
        True
    """
    error_code = error_data.get('error_code', 'UNKNOWN')
    message = error_data.get('error_message', 'Unknown error')

    # Map error codes to exception classes
    error_mapping = {
        # Dataset errors
        'INVALID_PAYLOAD_FORMAT': InvalidPayloadError,
        'INVALID_MANIFEST': InvalidManifestError,
        'DATASET_NOT_FOUND': DatasetNotFoundError,
        'DEPENDENCY_VALIDATION_ERROR': DependencyValidationError,
        'MANIFEST_REGISTRATION_ERROR': ManifestRegistrationError,
        'MANIFEST_LINKING_ERROR': ManifestLinkingError,
        'MANIFEST_NOT_FOUND': ManifestNotFoundError,
        'VERSION_TAGGING_ERROR': VersionTaggingError,
        'UNSUPPORTED_DATASET_KIND': UnsupportedDatasetKindError,
        'STORE_ERROR': StoreError,
        'UNLINK_DATASET_MANIFESTS_ERROR': UnlinkDatasetManifestsError,
        'LIST_ALL_DATASETS_ERROR': ListAllDatasetsError,
        'LIST_DATASET_VERSIONS_ERROR': ListDatasetVersionsError,
        'GET_DATASET_VERSION_ERROR': GetDatasetVersionError,
        'GET_MANIFEST_ERROR': GetManifestError,
        # Job errors
        'JOB_NOT_FOUND': JobNotFoundError,
        'LIST_JOBS_ERROR': ListJobsError,
        'SCHEDULER_ERROR': SchedulerError,
        'JOB_STOP_ERROR': JobStopError,
        'JOB_DELETE_ERROR': JobDeleteError,
        'JOBS_DELETE_ERROR': JobsDeleteError,
        # Location errors
        'LOCATION_NOT_FOUND': LocationNotFoundError,
        'LIST_LOCATIONS_ERROR': ListLocationsError,
        'DELETE_LOCATION_ERROR': DeleteLocationError,
        # File errors
        'LIST_LOCATION_FILES_ERROR': ListLocationFilesError,
        'FILE_NOT_FOUND': FileNotFoundError,
        'GET_FILE_INFO_ERROR': GetFileInfoError,
        # Provider errors
        'PROVIDER_NOT_FOUND': ProviderNotFoundError,
        'CREATE_PROVIDER_ERROR': CreateProviderError,
        'LIST_PROVIDERS_ERROR': ListProvidersError,
        'DELETE_PROVIDER_ERROR': DeleteProviderError,
        # Schema errors
        'GET_OUTPUT_SCHEMA_ERROR': GetOutputSchemaError,
        # General errors
        'INVALID_PATH': InvalidPathError,
        'DATABASE_ERROR': DatabaseError,
        'INTERNAL_SERVER_ERROR': InternalServerError,
    }

    error_class = error_mapping.get(error_code, AdminAPIError)
    return error_class(error_code, message, status_code)
