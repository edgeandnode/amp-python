"""Datasets client for Admin API.

This module provides the DatasetsClient class for managing datasets,
including registration, deployment, versioning, and manifest operations.
"""

from typing import TYPE_CHECKING, Optional

from . import models

if TYPE_CHECKING:
    from .client import AdminClient


class DatasetsClient:
    """Client for dataset operations.

    Provides methods for registering, deploying, listing, and managing datasets
    through the Admin API.

    Args:
        admin_client: Parent AdminClient instance

    Example:
        >>> client = AdminClient('http://localhost:8080')
        >>> client.datasets.list_all()
    """

    def __init__(self, admin_client: 'AdminClient'):
        """Initialize datasets client.

        Args:
            admin_client: Parent AdminClient instance
        """
        self._admin = admin_client

    def register(self, namespace: str, name: str, version: str, manifest: dict) -> None:
        """Register a dataset manifest.

        Registers a new dataset configuration in the server's local registry.
        The manifest defines tables, dependencies, and extraction logic.

        Args:
            namespace: Dataset namespace (e.g., '_')
            name: Dataset name
            version: Semantic version (e.g., '1.0.0') or tag ('latest', 'dev')
            manifest: Dataset manifest dict (kind='manifest')

        Raises:
            InvalidManifestError: If manifest is invalid
            DependencyValidationError: If dependencies are invalid
            ManifestRegistrationError: If registration fails

        Example:
            >>> manifest = {
            ...     'kind': 'manifest',
            ...     'dependencies': {'eth': '_/eth_firehose@0.0.0'},
            ...     'tables': {...},
            ...     'functions': {}
            ... }
            >>> client.datasets.register('_', 'my_dataset', '1.0.0', manifest)
        """
        request_data = models.RegisterRequest(namespace=namespace, name=name, version=version, manifest=manifest)

        self._admin._request('POST', '/datasets', json=request_data.model_dump(mode='json', exclude_none=True))

    def deploy(
        self,
        namespace: str,
        name: str,
        revision: str,
        end_block: Optional[str] = None,
        parallelism: Optional[int] = None,
    ) -> models.DeployResponse:
        """Deploy a dataset version.

        Triggers data extraction for the specified dataset version.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            revision: Version tag ('latest', 'dev', '1.0.0', etc.)
            end_block: Optional end block ('latest', '-100', '1000000', or null)
            parallelism: Optional number of parallel workers

        Returns:
            DeployResponse with job_id

        Raises:
            DatasetNotFoundError: If dataset/version not found
            SchedulerError: If deployment fails

        Example:
            >>> response = client.datasets.deploy('_', 'my_dataset', '1.0.0', parallelism=4)
            >>> print(f'Job ID: {response.job_id}')
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}/deploy'

        # Build request body (POST requires JSON body, not query params)
        body = {}
        if end_block is not None:
            body['end_block'] = end_block
        if parallelism is not None:
            body['parallelism'] = parallelism

        response = self._admin._request('POST', path, json=body if body else {})
        return models.DeployResponse.model_validate(response.json())

    def list_all(self) -> models.DatasetsResponse:
        """List all registered datasets.

        Returns all datasets across all namespaces with version information.

        Returns:
            DatasetsResponse with list of datasets

        Raises:
            ListAllDatasetsError: If listing fails

        Example:
            >>> datasets = client.datasets.list_all()
            >>> for ds in datasets.datasets:
            ...     print(f'{ds.namespace}/{ds.name}: {ds.latest_version}')
        """
        response = self._admin._request('GET', '/datasets')
        return models.DatasetsResponse.model_validate(response.json())

    def get_versions(self, namespace: str, name: str) -> models.VersionsResponse:
        """List all versions of a dataset.

        Returns version information including semantic versions and special tags.

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Returns:
            VersionsResponse with version list

        Raises:
            DatasetNotFoundError: If dataset not found
            ListDatasetVersionsError: If listing fails

        Example:
            >>> versions = client.datasets.get_versions('_', 'eth_firehose')
            >>> print(f'Latest: {versions.special_tags.latest}')
            >>> print(f'Versions: {versions.versions}')
        """
        path = f'/datasets/{namespace}/{name}/versions'
        response = self._admin._request('GET', path)
        return models.VersionsResponse.model_validate(response.json())

    def get_version(self, namespace: str, name: str, revision: str) -> models.VersionInfo:
        """Get detailed information about a specific dataset version.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            revision: Version tag or semantic version

        Returns:
            VersionInfo with dataset details

        Raises:
            DatasetNotFoundError: If dataset/version not found
            GetDatasetVersionError: If retrieval fails

        Example:
            >>> info = client.datasets.get_version('_', 'eth_firehose', '1.0.0')
            >>> print(f'Kind: {info.kind}')
            >>> print(f'Hash: {info.manifest_hash}')
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}'
        response = self._admin._request('GET', path)
        return models.VersionInfo.model_validate(response.json())

    def get_manifest(self, namespace: str, name: str, revision: str) -> dict:
        """Get the manifest for a specific dataset version.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            revision: Version tag or semantic version

        Returns:
            Manifest dict

        Raises:
            DatasetNotFoundError: If dataset/version not found
            GetManifestError: If retrieval fails

        Example:
            >>> manifest = client.datasets.get_manifest('_', 'eth_firehose', '1.0.0')
            >>> print(manifest['kind'])
            >>> print(manifest['tables'].keys())
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}/manifest'
        response = self._admin._request('GET', path)
        return response.json()

    def delete(self, namespace: str, name: str) -> None:
        """Delete all versions and metadata for a dataset.

        Removes all manifest links and version tags for the dataset.
        Orphaned manifests (not referenced by other datasets) are also deleted.

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Raises:
            InvalidPathError: If namespace/name invalid
            UnlinkDatasetManifestsError: If deletion fails

        Example:
            >>> client.datasets.delete('_', 'my_old_dataset')
        """
        path = f'/datasets/{namespace}/{name}'
        self._admin._request('DELETE', path)
