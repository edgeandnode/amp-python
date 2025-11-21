"""Integration tests for Registry datasets operations.

These tests interact with the real staging Registry API.
Read operations don't require authentication.
"""

import pytest

from amp.registry import RegistryClient
from amp.registry.errors import DatasetNotFoundError


@pytest.fixture
def registry_client():
    """Create registry client for staging environment."""
    client = RegistryClient()
    yield client
    client.close()


class TestDatasetsList:
    """Test dataset listing operations."""

    def test_list_datasets_default(self, registry_client):
        """Test listing datasets with default parameters."""
        response = registry_client.datasets.list()

        assert response.total_count >= 0
        assert isinstance(response.datasets, list)
        assert response.has_next_page is not None

        if response.datasets:
            dataset = response.datasets[0]
            assert dataset.namespace
            assert dataset.name
            assert dataset.latest_version

    def test_list_datasets_with_limit(self, registry_client):
        """Test listing with custom limit."""
        response = registry_client.datasets.list(limit=5, page=1)

        assert response.total_count >= 0
        assert len(response.datasets) <= 5

    def test_list_datasets_pagination(self, registry_client):
        """Test pagination works correctly."""
        page1 = registry_client.datasets.list(limit=10, page=1)
        page2 = registry_client.datasets.list(limit=10, page=2)

        # If there are enough datasets, pages should differ
        if page1.has_next_page and page2.datasets:
            # Pages should have different datasets (by ID or name)
            page1_ids = {(d.namespace, d.name) for d in page1.datasets}
            page2_ids = {(d.namespace, d.name) for d in page2.datasets}
            assert page1_ids != page2_ids


class TestDatasetsGet:
    """Test getting dataset details."""

    def test_get_dataset_not_found(self, registry_client):
        """Test getting non-existent dataset raises appropriate error."""
        with pytest.raises(DatasetNotFoundError):
            registry_client.datasets.get('nonexistent', 'nonexistent_dataset')

    def test_get_dataset_success(self, registry_client):
        """Test getting a known dataset."""
        # First list datasets to find one that exists
        response = registry_client.datasets.list(limit=1)

        if not response.datasets:
            pytest.skip('No datasets available in registry')

        dataset = response.datasets[0]

        # Get full dataset details
        full_dataset = registry_client.datasets.get(dataset.namespace, dataset.name)

        from amp.registry.models import DatasetVisibility

        assert full_dataset.namespace == dataset.namespace
        assert full_dataset.name == dataset.name
        assert full_dataset.latest_version
        assert full_dataset.visibility in [DatasetVisibility.public, DatasetVisibility.private]


class TestDatasetVersions:
    """Test dataset version operations."""

    def test_list_versions(self, registry_client):
        """Test listing versions for a dataset."""
        # Get a dataset first
        response = registry_client.datasets.list(limit=1)

        if not response.datasets:
            pytest.skip('No datasets available in registry')

        dataset = response.datasets[0]

        # List versions
        versions = registry_client.datasets.list_versions(dataset.namespace, dataset.name)

        from amp.registry.models import DatasetVersionStatus

        assert isinstance(versions, list)
        assert len(versions) > 0

        version = versions[0]
        assert version.version_tag
        assert version.status in [
            DatasetVersionStatus.draft,
            DatasetVersionStatus.published,
            DatasetVersionStatus.deprecated,
            DatasetVersionStatus.archived,
        ]

    def test_get_version(self, registry_client):
        """Test getting specific version details."""
        # Get a dataset and its versions
        response = registry_client.datasets.list(limit=1)

        if not response.datasets:
            pytest.skip('No datasets available in registry')

        dataset = response.datasets[0]
        versions = registry_client.datasets.list_versions(dataset.namespace, dataset.name)

        if not versions:
            pytest.skip('No versions available for dataset')

        # Get specific version
        version_detail = registry_client.datasets.get_version(dataset.namespace, dataset.name, versions[0].version_tag)

        assert version_detail.version_tag == versions[0].version_tag
        assert version_detail.status


class TestDatasetManifest:
    """Test manifest operations."""

    def test_get_manifest(self, registry_client):
        """Test fetching dataset manifest."""
        # Get a dataset first
        response = registry_client.datasets.list(limit=1)

        if not response.datasets:
            pytest.skip('No datasets available in registry')

        dataset = response.datasets[0]

        # Skip if dataset has no latest_version
        if not dataset.latest_version or not dataset.latest_version.version_tag:
            pytest.skip('Dataset has no latest version')

        # Fetch manifest
        manifest = registry_client.datasets.get_manifest(
            dataset.namespace, dataset.name, dataset.latest_version.version_tag
        )

        assert isinstance(manifest, dict)
        assert 'kind' in manifest or 'tables' in manifest or 'dependencies' in manifest

    def test_get_manifest_for_specific_version(self, registry_client):
        """Test fetching manifest for a specific version."""
        # Get a dataset and list versions
        response = registry_client.datasets.list(limit=1)

        if not response.datasets:
            pytest.skip('No datasets available in registry')

        dataset = response.datasets[0]
        versions = registry_client.datasets.list_versions(dataset.namespace, dataset.name)

        if not versions:
            pytest.skip('No versions available for dataset')

        # Get manifest for specific version
        manifest = registry_client.datasets.get_manifest(dataset.namespace, dataset.name, versions[0].version_tag)

        assert isinstance(manifest, dict)
