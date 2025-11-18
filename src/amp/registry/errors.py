"""Registry API error types."""


class RegistryError(Exception):
    """Base exception for all Registry API errors."""

    def __init__(self, message: str, error_code: str = '', request_id: str = ''):
        super().__init__(message)
        self.error_code = error_code
        self.request_id = request_id


class DatasetNotFoundError(RegistryError):
    """Raised when a requested dataset does not exist."""

    pass


class DatasetVersionNotFoundError(RegistryError):
    """Raised when a requested dataset version does not exist."""

    pass


class UnauthorizedError(RegistryError):
    """Raised when authentication is missing or invalid."""

    pass


class ForbiddenError(RegistryError):
    """Raised when the user lacks permission for the requested operation."""

    pass


class ValidationError(RegistryError):
    """Raised when request parameters are invalid."""

    pass


class RegistryDatabaseError(RegistryError):
    """Raised when a database operation fails."""

    pass


# Error code mapping from API responses
ERROR_CODE_MAP = {
    'NOT_FOUND': DatasetNotFoundError,
    'DATASET_NOT_FOUND': DatasetNotFoundError,
    'DATASET_VERSION_NOT_FOUND': DatasetVersionNotFoundError,
    'UNAUTHORIZED': UnauthorizedError,
    'FORBIDDEN': ForbiddenError,
    'INVALID_QUERY_PARAMETERS': ValidationError,
    'LIMIT_TOO_LARGE': ValidationError,
    'LIMIT_INVALID': ValidationError,
    'AMP_REGISTRY_DB_ERROR': RegistryDatabaseError,
    'DATASET_CONVERSION_ERROR': RegistryDatabaseError,
}


def map_error(error_code: str, error_message: str, request_id: str = '') -> RegistryError:
    """Map an error code to the appropriate exception type.

    Args:
        error_code: Machine-readable error code from API response
        error_message: Human-readable error message
        request_id: Optional request ID for tracing

    Returns:
        RegistryError: Appropriate exception subclass for the error code
    """
    exception_cls = ERROR_CODE_MAP.get(error_code, RegistryError)
    return exception_cls(error_message, error_code=error_code, request_id=request_id)
