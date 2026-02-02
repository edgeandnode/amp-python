"""Async Registry datasets client."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from amp.utils.manifest_inspector import describe_manifest

from . import models

if TYPE_CHECKING:
    from .async_client import AsyncRegistryClient

logger = logging.getLogger(__name__)


class AsyncRegistryDatasetsClient:
    """Async client for dataset operations in the Registry API.

    Provides async methods for:
    - Searching and discovering datasets
    - Fetching dataset details and manifests
    - Publishing datasets (requires authentication)
    - Managing dataset visibility and versions

    Args:
        registry_client: Parent AsyncRegistryClient instance
    """

    def __init__(self, registry_client: AsyncRegistryClient):
        """Initialize async datasets client.

        Args:
            registry_client: Parent AsyncRegistryClient instance
        """
        self._registry = registry_client

    # Read Operations (Public - No Auth Required)

    async def list(
        self, limit: int = 50, page: int = 1, sort_by: Optional[str] = None, direction: Optional[str] = None
    ) -> models.DatasetListResponse:
        """List all published datasets with pagination.

        Args:
            limit: Maximum number of datasets to return (default: 50, max: 1000)
            page: Page number (1-indexed, default: 1)
            sort_by: Field to sort by (e.g., 'name', 'created_at', 'updated_at')
            direction: Sort direction ('asc' or 'desc')

        Returns:
            DatasetListResponse: Paginated list of datasets

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     response = await client.datasets.list(limit=10, page=1)
            ...     print(f"Found {response.total_count} datasets")
        """
        params: Dict[str, Any] = {'limit': limit, 'page': page}
        if sort_by:
            params['sort_by'] = sort_by
        if direction:
            params['direction'] = direction

        response = await self._registry._request('GET', '/api/v1/datasets', params=params)
        return models.DatasetListResponse.model_validate(response.json())

    async def search(self, query: str, limit: int = 50, page: int = 1) -> models.DatasetSearchResponse:
        """Search datasets using full-text keyword search.

        Results are ranked by relevance score.

        Args:
            query: Search query string
            limit: Maximum number of results (default: 50, max: 1000)
            page: Page number (1-indexed, default: 1)

        Returns:
            DatasetSearchResponse: Search results with relevance scores

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     results = await client.datasets.search('ethereum blocks')
            ...     for dataset in results.datasets:
            ...         print(f"[{dataset.score}] {dataset.namespace}/{dataset.name}")
        """
        params = {'search': query, 'limit': limit, 'page': page}
        response = await self._registry._request('GET', '/api/v1/datasets/search', params=params)
        return models.DatasetSearchResponse.model_validate(response.json())

    async def ai_search(self, query: str, limit: int = 50) -> list[models.DatasetWithScore]:
        """Search datasets using AI-powered semantic search.

        Uses embeddings and natural language processing for better matching.

        Args:
            query: Natural language search query
            limit: Maximum number of results (default: 50)

        Returns:
            list[DatasetWithScore]: List of datasets with relevance scores

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     results = await client.datasets.ai_search('find NFT transfer data')
            ...     for dataset in results:
            ...         print(f"[{dataset.score}] {dataset.namespace}/{dataset.name}")
        """
        params = {'search': query, 'limit': limit}
        response = await self._registry._request('GET', '/api/v1/datasets/search/ai', params=params)
        return [models.DatasetWithScore.model_validate(d) for d in response.json()]

    async def get(self, namespace: str, name: str) -> models.Dataset:
        """Get detailed information about a specific dataset.

        Args:
            namespace: Dataset namespace (e.g., 'edgeandnode', 'edgeandnode')
            name: Dataset name (e.g., 'ethereum-mainnet')

        Returns:
            Dataset: Complete dataset information

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     dataset = await client.datasets.get('edgeandnode', 'ethereum-mainnet')
            ...     print(f"Latest version: {dataset.latest_version}")
        """
        path = f'/api/v1/datasets/{namespace}/{name}'
        response = await self._registry._request('GET', path)
        return models.Dataset.model_validate(response.json())

    async def list_versions(self, namespace: str, name: str) -> list[models.DatasetVersion]:
        """List all versions of a dataset.

        Versions are returned sorted by latest first.

        Args:
            namespace: Dataset namespace
            name: Dataset name

        Returns:
            list[DatasetVersion]: List of dataset versions

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     versions = await client.datasets.list_versions('edgeandnode', 'ethereum-mainnet')
            ...     for version in versions:
            ...         print(f"  - v{version.version} ({version.status})")
        """
        path = f'/api/v1/datasets/{namespace}/{name}/versions'
        response = await self._registry._request('GET', path)
        return [models.DatasetVersion.model_validate(v) for v in response.json()]

    async def get_version(self, namespace: str, name: str, version: str) -> models.DatasetVersion:
        """Get details of a specific dataset version.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version tag (e.g., '1.0.0', 'latest')

        Returns:
            DatasetVersion: Version details

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     version = await client.datasets.get_version('edgeandnode', 'ethereum-mainnet', 'latest')
            ...     print(f"Version: {version.version}")
        """
        path = f'/api/v1/datasets/{namespace}/{name}/versions/{version}'
        response = await self._registry._request('GET', path)
        return models.DatasetVersion.model_validate(response.json())

    async def get_manifest(self, namespace: str, name: str, version: str) -> dict:
        """Fetch the manifest JSON for a specific dataset version.

        Manifests define the dataset structure, dependencies, and ETL logic.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version tag (e.g., '1.0.0', 'latest')

        Returns:
            dict: Manifest JSON content

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     manifest = await client.datasets.get_manifest('edgeandnode', 'ethereum-mainnet', 'latest')
            ...     print(f"Tables: {list(manifest.get('tables', {}).keys())}")
        """
        path = f'/api/v1/datasets/{namespace}/{name}/versions/{version}/manifest'
        response = await self._registry._request('GET', path)
        return response.json()

    async def describe(
        self, namespace: str, name: str, version: str = 'latest'
    ) -> Dict[str, list[Dict[str, str | bool]]]:
        """Get a structured summary of tables and columns in a dataset.

        Returns a dictionary mapping table names to lists of column information,
        making it easy to programmatically inspect the dataset schema.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version tag (default: 'latest')

        Returns:
            dict: Mapping of table names to column information.

        Example:
            >>> async with AsyncRegistryClient() as client:
            ...     schema = await client.datasets.describe('edgeandnode', 'ethereum-mainnet', 'latest')
            ...     for table_name, columns in schema.items():
            ...         print(f"Table: {table_name}")
        """
        manifest = await self.get_manifest(namespace, name, version)
        return describe_manifest(manifest)

    # Write Operations (Require Authentication)

    async def publish(
        self,
        namespace: str,
        name: str,
        version: str,
        manifest: dict,
        visibility: str = 'public',
        description: Optional[str] = None,
        tags: Optional[list[str]] = None,
        chains: Optional[list[str]] = None,
        sources: Optional[list[str]] = None,
    ) -> models.Dataset:
        """Publish a new dataset to the registry.

        Requires authentication (Bearer token).

        Args:
            namespace: Dataset namespace (owner's username or org)
            name: Dataset name
            version: Initial version tag (e.g., '1.0.0')
            manifest: Dataset manifest JSON
            visibility: Dataset visibility ('public' or 'private', default: 'public')
            description: Dataset description
            tags: Optional list of tags/keywords
            chains: Optional list of blockchain networks
            sources: Optional list of data sources

        Returns:
            Dataset: Created dataset

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     dataset = await client.datasets.publish(
            ...         namespace='myuser',
            ...         name='my_dataset',
            ...         version='1.0.0',
            ...         manifest=manifest,
            ...         description='My custom dataset'
            ...     )
        """
        body = {
            'name': name,
            'version': version,
            'manifest': manifest,
            'visibility': visibility,
        }
        if description:
            body['description'] = description
        if tags:
            body['tags'] = tags
        if chains:
            body['chains'] = chains
        if sources:
            body['sources'] = sources

        response = await self._registry._request('POST', '/api/v1/owners/@me/datasets/publish', json=body)
        return models.Dataset.model_validate(response.json())

    async def publish_version(
        self,
        namespace: str,
        name: str,
        version: str,
        manifest: dict,
        description: Optional[str] = None,
    ) -> models.DatasetVersion:
        """Publish a new version of an existing dataset.

        Requires authentication and ownership of the dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: New version tag (e.g., '1.1.0')
            manifest: Dataset manifest JSON for this version
            description: Optional version description

        Returns:
            DatasetVersion: Created version

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     version = await client.datasets.publish_version(
            ...         namespace='myuser',
            ...         name='my_dataset',
            ...         version='1.1.0',
            ...         manifest=manifest
            ...     )
        """
        body = {'version': version, 'manifest': manifest}
        if description:
            body['description'] = description

        path = f'/api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish'
        response = await self._registry._request('POST', path, json=body)
        return models.DatasetVersion.model_validate(response.json())

    async def update(
        self,
        namespace: str,
        name: str,
        description: Optional[str] = None,
        tags: Optional[list[str]] = None,
        chains: Optional[list[str]] = None,
        sources: Optional[list[str]] = None,
    ) -> models.Dataset:
        """Update dataset metadata.

        Requires authentication and ownership of the dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            description: Updated description
            tags: Updated tags
            chains: Updated chains
            sources: Updated sources

        Returns:
            Dataset: Updated dataset

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     dataset = await client.datasets.update(
            ...         namespace='myuser',
            ...         name='my_dataset',
            ...         description='Updated description'
            ...     )
        """
        body = {}
        if description is not None:
            body['description'] = description
        if tags is not None:
            body['tags'] = tags
        if chains is not None:
            body['chains'] = chains
        if sources is not None:
            body['sources'] = sources

        path = f'/api/v1/owners/@me/datasets/{namespace}/{name}'
        response = await self._registry._request('PUT', path, json=body)
        return models.Dataset.model_validate(response.json())

    async def update_visibility(self, namespace: str, name: str, visibility: str) -> models.Dataset:
        """Update dataset visibility (public/private).

        Requires authentication and ownership of the dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            visibility: New visibility ('public' or 'private')

        Returns:
            Dataset: Updated dataset

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     dataset = await client.datasets.update_visibility('myuser', 'my_dataset', 'private')
        """
        body = {'visibility': visibility}
        path = f'/api/v1/owners/@me/datasets/{namespace}/{name}/visibility'
        response = await self._registry._request('PATCH', path, json=body)
        return models.Dataset.model_validate(response.json())

    async def update_version_status(
        self, namespace: str, name: str, version: str, status: str
    ) -> models.DatasetVersion:
        """Update the status of a dataset version.

        Requires authentication and ownership of the dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version tag
            status: New status ('draft', 'published', 'deprecated', or 'archived')

        Returns:
            DatasetVersion: Updated version

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     version = await client.datasets.update_version_status(
            ...         'myuser', 'my_dataset', '1.0.0', 'deprecated'
            ...     )
        """
        body = {'status': status}
        path = f'/api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}'
        response = await self._registry._request('PATCH', path, json=body)
        return models.DatasetVersion.model_validate(response.json())

    async def delete_version(
        self, namespace: str, name: str, version: str
    ) -> models.ArchiveDatasetVersionResponse:
        """Delete (archive) a dataset version.

        Requires authentication and ownership of the dataset.

        Args:
            namespace: Dataset namespace
            name: Dataset name
            version: Version tag to delete

        Returns:
            ArchiveDatasetVersionResponse: Confirmation of deletion

        Example:
            >>> async with AsyncRegistryClient(auth_token='your-token') as client:
            ...     response = await client.datasets.delete_version('myuser', 'my_dataset', '0.1.0')
        """
        path = f'/api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}'
        response = await self._registry._request('DELETE', path)
        return models.ArchiveDatasetVersionResponse.model_validate(response.json())
