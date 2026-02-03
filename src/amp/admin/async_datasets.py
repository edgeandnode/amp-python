"""Async datasets client for Admin API.

This module provides the AsyncDatasetsClient class for managing datasets,
including registration, deployment, versioning, and manifest operations.
"""

from typing import TYPE_CHECKING, Dict, Optional

from amp.utils.manifest_inspector import describe_manifest

from . import models

if TYPE_CHECKING:
    from .async_client import AsyncAdminClient


class AsyncDatasetsClient:
    """Async client for dataset operations.

    Provides async methods for registering, deploying, listing, and managing datasets
    through the Admin API.

    Args:
        admin_client: Parent AsyncAdminClient instance

    Example:
        >>> async with AsyncAdminClient('http://localhost:8080') as client:
        ...     datasets = await client.datasets.list_all()
    """

    def __init__(self, admin_client: 'AsyncAdminClient'):
        """Initialize async datasets client.

        Args:
            admin_client: Parent AsyncAdminClient instance
        """
        self._admin = admin_client

    async def register(self, namespace: str, name: str, version: str, manifest: dict) -> None:
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     await client.datasets.register('_', 'my_dataset', '1.0.0', manifest)
        """
        request_data = models.RegisterRequest(namespace=namespace, name=name, version=version, manifest=manifest)

        await self._admin._request('POST', '/datasets', json=request_data.model_dump(mode='json', exclude_none=True))

    async def deploy(
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     response = await client.datasets.deploy('_', 'my_dataset', '1.0.0', parallelism=4)
            ...     print(f'Job ID: {response.job_id}')
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}/deploy'

        # Build request body (POST requires JSON body, not query params)
        body = {}
        if end_block is not None:
            body['end_block'] = end_block
        if parallelism is not None:
            body['parallelism'] = parallelism

        response = await self._admin._request('POST', path, json=body if body else {})
        return models.DeployResponse.model_validate(response.json())

    async def list_all(self) -> models.DatasetsResponse:
        """List all registered datasets.

        Returns all datasets across all namespaces with version information.

        Returns:
            DatasetsResponse with list of datasets

        Raises:
            ListAllDatasetsError: If listing fails

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     datasets = await client.datasets.list_all()
            ...     for ds in datasets.datasets:
            ...         print(f'{ds.namespace}/{ds.name}: {ds.latest_version}')
        """
        response = await self._admin._request('GET', '/datasets')
        return models.DatasetsResponse.model_validate(response.json())

    async def get_versions(self, namespace: str, name: str) -> models.VersionsResponse:
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     versions = await client.datasets.get_versions('_', 'eth_firehose')
            ...     print(f'Latest: {versions.special_tags.latest}')
        """
        path = f'/datasets/{namespace}/{name}/versions'
        response = await self._admin._request('GET', path)
        return models.VersionsResponse.model_validate(response.json())

    async def get_version(self, namespace: str, name: str, revision: str) -> models.VersionInfo:
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     info = await client.datasets.get_version('_', 'eth_firehose', '1.0.0')
            ...     print(f'Kind: {info.kind}')
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}'
        response = await self._admin._request('GET', path)
        return models.VersionInfo.model_validate(response.json())

    async def get_manifest(self, namespace: str, name: str, revision: str) -> dict:
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     manifest = await client.datasets.get_manifest('_', 'eth_firehose', '1.0.0')
            ...     print(manifest['kind'])
        """
        path = f'/datasets/{namespace}/{name}/versions/{revision}/manifest'
        response = await self._admin._request('GET', path)
        return response.json()

    async def describe(
        self, namespace: str, name: str, revision: str = 'latest'
    ) -> Dict[str, list[Dict[str, str | bool]]]:
        """Get a structured summary of tables and columns in a dataset.

        Returns a dictionary mapping table names to lists of column information,
        making it easy to programmatically inspect the dataset schema.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            revision: Version tag (default: 'latest')

        Returns:
            dict: Mapping of table names to column information.

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     schema = await client.datasets.describe('_', 'eth_firehose', 'latest')
            ...     for table_name, columns in schema.items():
            ...         print(f"Table: {table_name}")
        """
        manifest = await self.get_manifest(namespace, name, revision)
        return describe_manifest(manifest)

    async def delete(self, namespace: str, name: str) -> None:
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
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     await client.datasets.delete('_', 'my_old_dataset')
        """
        path = f'/datasets/{namespace}/{name}'
        await self._admin._request('DELETE', path)
