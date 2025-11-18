"""Integration tests for Registry search operations.

These tests interact with the real staging Registry API.
Search operations don't require authentication.
"""

import pytest

from amp.registry import RegistryClient


@pytest.fixture
def registry_client():
    """Create registry client for staging environment."""
    client = RegistryClient()
    yield client
    client.close()


class TestDatasetSearch:
    """Test full-text search functionality."""

    def test_search_empty_query(self, registry_client):
        """Test search with empty query returns results."""
        results = registry_client.datasets.search('', limit=10)

        assert results.total_count >= 0
        assert isinstance(results.datasets, list)

    def test_search_with_keyword(self, registry_client):
        """Test search with a common keyword."""
        results = registry_client.datasets.search('ethereum', limit=10)

        assert results.total_count >= 0
        assert isinstance(results.datasets, list)

        # If results exist, verify they have required fields
        if results.datasets:
            dataset = results.datasets[0]
            assert dataset.namespace
            assert dataset.name
            assert hasattr(dataset, 'score')
            assert isinstance(dataset.score, (int, float))

    def test_search_results_have_scores(self, registry_client):
        """Test that search results include relevance scores."""
        results = registry_client.datasets.search('blocks', limit=5)

        if results.datasets:
            for dataset in results.datasets:
                assert hasattr(dataset, 'score')
                # Scores should be numeric
                assert isinstance(dataset.score, (int, float))

    def test_search_pagination(self, registry_client):
        """Test search pagination."""
        page1 = registry_client.datasets.search('ethereum', limit=5, page=1)
        page2 = registry_client.datasets.search('ethereum', limit=5, page=2)

        # Both pages should have valid responses
        assert page1.total_count >= 0
        assert page2.total_count >= 0

        # Total count should be the same across pages
        if page1.total_count > 0:
            assert page1.total_count == page2.total_count

    def test_search_limit_parameter(self, registry_client):
        """Test that limit parameter is respected."""
        limit = 3
        results = registry_client.datasets.search('ethereum', limit=limit)

        # Results should not exceed limit
        assert len(results.datasets) <= limit

    def test_search_no_results(self, registry_client):
        """Test search with query unlikely to match anything."""
        results = registry_client.datasets.search('xyznonexistentqueryfoobar123', limit=10)

        # Should return valid response even with no matches
        assert results.total_count >= 0
        assert isinstance(results.datasets, list)


class TestAISearch:
    """Test AI semantic search functionality."""

    def test_ai_search_basic(self, registry_client):
        """Test basic AI search."""
        from amp.registry.errors import RegistryDatabaseError

        try:
            results = registry_client.datasets.ai_search('find blockchain data', limit=5)

            assert isinstance(results, list)

            # If results exist, verify structure
            if results:
                dataset = results[0]
                assert dataset.namespace
                assert dataset.name
                assert hasattr(dataset, 'score')

        except RegistryDatabaseError as e:
            # AI search might not be configured on staging environment
            if 'openai' in str(e).lower() or 'not been configured' in str(e).lower():
                pytest.skip(f'AI search not configured: {e}')
            else:
                raise
        except Exception as e:
            # Other potential errors
            if 'not available' in str(e).lower() or 'not implemented' in str(e).lower():
                pytest.skip(f'AI search not available: {e}')
            else:
                raise

    def test_ai_search_natural_language(self, registry_client):
        """Test AI search with natural language query."""
        from amp.registry.errors import RegistryDatabaseError

        try:
            # Natural language queries should work
            results = registry_client.datasets.ai_search('I need NFT transfer data', limit=5)

            assert isinstance(results, list)

            if results:
                # Results should have scores
                for dataset in results:
                    assert hasattr(dataset, 'score')
                    assert isinstance(dataset.score, (int, float))

        except RegistryDatabaseError as e:
            if 'openai' in str(e).lower() or 'not been configured' in str(e).lower():
                pytest.skip(f'AI search not configured: {e}')
            else:
                raise
        except Exception as e:
            if 'not available' in str(e).lower() or 'not implemented' in str(e).lower():
                pytest.skip(f'AI search not available: {e}')
            else:
                raise

    def test_ai_search_limit(self, registry_client):
        """Test AI search respects limit parameter."""
        from amp.registry.errors import RegistryDatabaseError

        try:
            limit = 3
            results = registry_client.datasets.ai_search('ethereum blocks', limit=limit)

            assert isinstance(results, list)
            assert len(results) <= limit

        except RegistryDatabaseError as e:
            if 'openai' in str(e).lower() or 'not been configured' in str(e).lower():
                pytest.skip(f'AI search not configured: {e}')
            else:
                raise
        except Exception as e:
            if 'not available' in str(e).lower() or 'not implemented' in str(e).lower():
                pytest.skip(f'AI search not available: {e}')
            else:
                raise


class TestSearchIntegration:
    """Test integration between search and other operations."""

    def test_search_then_get_dataset(self, registry_client):
        """Test workflow: search → get dataset details."""
        # Search for datasets
        results = registry_client.datasets.search('ethereum', limit=1)

        if not results.datasets:
            pytest.skip('No search results available')

        dataset = results.datasets[0]

        # Get full dataset details
        full_dataset = registry_client.datasets.get(dataset.namespace, dataset.name)

        assert full_dataset.namespace == dataset.namespace
        assert full_dataset.name == dataset.name

    def test_search_then_get_manifest(self, registry_client):
        """Test workflow: search → get manifest."""
        # Search for datasets
        results = registry_client.datasets.search('blocks', limit=1)

        if not results.datasets:
            pytest.skip('No search results available')

        dataset = results.datasets[0]

        # Get full dataset to get latest version
        full_dataset = registry_client.datasets.get(dataset.namespace, dataset.name)

        # Skip if no latest version
        if not full_dataset.latest_version or not full_dataset.latest_version.version_tag:
            pytest.skip('Dataset has no latest version')

        # Fetch manifest
        manifest = registry_client.datasets.get_manifest(
            full_dataset.namespace, full_dataset.name, full_dataset.latest_version.version_tag
        )

        assert isinstance(manifest, dict)
