"""Admin API client for Amp.

This module provides HTTP client functionality for interacting with the Amp Admin API,
enabling dataset registration, deployment, manifest generation, and job monitoring.

Example:
    >>> from amp.admin import AdminClient
    >>> client = AdminClient('http://localhost:8080')
    >>> datasets = client.datasets.list_all()
"""

from .client import AdminClient
from .datasets import DatasetsClient
from .deployment import DeploymentContext
from .errors import (
    AdminAPIError,
    CreateProviderError,
    DatabaseError,
    DatasetNotFoundError,
    DeleteLocationError,
    DeleteProviderError,
    DependencyValidationError,
    FileNotFoundError,
    GetDatasetVersionError,
    GetFileInfoError,
    GetManifestError,
    GetOutputSchemaError,
    InternalServerError,
    InvalidManifestError,
    InvalidPathError,
    InvalidPayloadError,
    JobDeleteError,
    JobNotFoundError,
    JobsDeleteError,
    JobStopError,
    ListAllDatasetsError,
    ListDatasetVersionsError,
    ListJobsError,
    ListLocationFilesError,
    ListLocationsError,
    ListProvidersError,
    LocationNotFoundError,
    ManifestLinkingError,
    ManifestNotFoundError,
    ManifestRegistrationError,
    ProviderNotFoundError,
    SchedulerError,
    StoreError,
    UnlinkDatasetManifestsError,
    UnsupportedDatasetKindError,
    VersionTaggingError,
)
from .jobs import JobsClient
from .schema import SchemaClient

__all__ = [
    # Core clients
    'AdminClient',
    'DatasetsClient',
    'JobsClient',
    'SchemaClient',
    'DeploymentContext',
    # Exceptions
    'AdminAPIError',
    'InvalidPayloadError',
    'InvalidManifestError',
    'DatasetNotFoundError',
    'DependencyValidationError',
    'ManifestRegistrationError',
    'ManifestLinkingError',
    'ManifestNotFoundError',
    'VersionTaggingError',
    'UnsupportedDatasetKindError',
    'StoreError',
    'UnlinkDatasetManifestsError',
    'ListAllDatasetsError',
    'ListDatasetVersionsError',
    'GetDatasetVersionError',
    'GetManifestError',
    'JobNotFoundError',
    'ListJobsError',
    'SchedulerError',
    'JobStopError',
    'JobDeleteError',
    'JobsDeleteError',
    'LocationNotFoundError',
    'ListLocationsError',
    'DeleteLocationError',
    'ListLocationFilesError',
    'FileNotFoundError',
    'GetFileInfoError',
    'ProviderNotFoundError',
    'CreateProviderError',
    'ListProvidersError',
    'DeleteProviderError',
    'GetOutputSchemaError',
    'InvalidPathError',
    'DatabaseError',
    'InternalServerError',
]
