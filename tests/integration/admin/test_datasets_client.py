"""Integration tests for DatasetsClient with HTTP mocking."""

import pytest
import respx
from httpx import Response

from amp.admin import AdminClient
from amp.admin.errors import DatasetNotFoundError, InvalidManifestError


@pytest.mark.integration
class TestDatasetsClient:
    """Test DatasetsClient operations with mocked HTTP responses."""

    @respx.mock
    def test_register_dataset(self):
        """Test dataset registration."""
        manifest = {
            'kind': 'manifest',
            'dependencies': {'eth': '_/eth_firehose@0.0.0'},
            'tables': {
                'blocks': {
                    'input': {'sql': 'SELECT * FROM eth.blocks'},
                    'schema': {'arrow': {'fields': []}},
                    'network': 'mainnet',
                }
            },
            'functions': {},
        }

        respx.post('http://localhost:8080/datasets').mock(return_value=Response(201))

        client = AdminClient('http://localhost:8080')
        client.datasets.register('_', 'test_dataset', '1.0.0', manifest)

        # Should complete without error

    @respx.mock
    def test_register_dataset_invalid_manifest(self):
        """Test dataset registration with invalid manifest."""
        error_response = {'error_code': 'INVALID_MANIFEST', 'error_message': 'Manifest validation failed'}
        respx.post('http://localhost:8080/datasets').mock(return_value=Response(400, json=error_response))

        client = AdminClient('http://localhost:8080')

        with pytest.raises(InvalidManifestError):
            client.datasets.register('_', 'test_dataset', '1.0.0', {})

    @respx.mock
    def test_deploy_dataset(self):
        """Test dataset deployment."""
        deploy_response = {'job_id': 123}
        respx.post('http://localhost:8080/datasets/_/test_dataset/versions/1.0.0/deploy').mock(
            return_value=Response(200, json=deploy_response)
        )

        client = AdminClient('http://localhost:8080')
        response = client.datasets.deploy('_', 'test_dataset', '1.0.0', parallelism=4)

        assert response.job_id == 123

    @respx.mock
    def test_deploy_dataset_with_end_block(self):
        """Test dataset deployment with end_block parameter."""
        deploy_response = {'job_id': 456}
        respx.post('http://localhost:8080/datasets/_/test_dataset/versions/1.0.0/deploy').mock(
            return_value=Response(200, json=deploy_response)
        )

        client = AdminClient('http://localhost:8080')
        response = client.datasets.deploy('_', 'test_dataset', '1.0.0', end_block='latest', parallelism=2)

        assert response.job_id == 456

    @respx.mock
    def test_list_all_datasets(self):
        """Test listing all datasets."""
        datasets_response = {
            'datasets': [
                {'namespace': '_', 'name': 'eth_firehose', 'latest_version': '1.0.0', 'versions': ['1.0.0']},
                {'namespace': '_', 'name': 'base_firehose', 'latest_version': '0.1.0', 'versions': ['0.1.0']},
            ]
        }
        respx.get('http://localhost:8080/datasets').mock(return_value=Response(200, json=datasets_response))

        client = AdminClient('http://localhost:8080')
        response = client.datasets.list_all()

        assert len(response.datasets) == 2
        assert response.datasets[0].name == 'eth_firehose'
        assert response.datasets[1].name == 'base_firehose'

    @respx.mock
    def test_get_versions(self):
        """Test getting dataset versions."""
        versions_response = {
            'namespace': '_',
            'name': 'eth_firehose',
            'versions': [
                {
                    'version': '1.0.0',
                    'manifest_hash': 'hash1',
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                },
                {
                    'version': '0.9.0',
                    'manifest_hash': 'hash2',
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z',
                },
            ],
            'special_tags': {'latest': '1.0.0', 'dev': '1.0.0'},
        }
        respx.get('http://localhost:8080/datasets/_/eth_firehose/versions').mock(
            return_value=Response(200, json=versions_response)
        )

        client = AdminClient('http://localhost:8080')
        response = client.datasets.get_versions('_', 'eth_firehose')

        assert len(response.versions) == 2
        assert response.special_tags.latest == '1.0.0'

    @respx.mock
    def test_get_version_info(self):
        """Test getting specific version info."""
        version_info = {
            'version': '1.0.0',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z',
            'manifest_hash': 'abc123',
        }
        respx.get('http://localhost:8080/datasets/_/eth_firehose/versions/1.0.0').mock(
            return_value=Response(200, json=version_info)
        )

        client = AdminClient('http://localhost:8080')
        response = client.datasets.get_version('_', 'eth_firehose', '1.0.0')

        assert response.manifest_hash == 'abc123'
        assert response.version == '1.0.0'

    @respx.mock
    def test_get_manifest(self):
        """Test getting dataset manifest."""
        manifest = {
            'kind': 'manifest',
            'dependencies': {},
            'tables': {'blocks': {'input': {'sql': 'SELECT * FROM blocks'}, 'network': 'mainnet'}},
            'functions': {},
        }
        respx.get('http://localhost:8080/datasets/_/eth_firehose/versions/1.0.0/manifest').mock(
            return_value=Response(200, json=manifest)
        )

        client = AdminClient('http://localhost:8080')
        response = client.datasets.get_manifest('_', 'eth_firehose', '1.0.0')

        assert response['kind'] == 'manifest'
        assert 'blocks' in response['tables']

    @respx.mock
    def test_delete_dataset(self):
        """Test deleting a dataset."""
        respx.delete('http://localhost:8080/datasets/_/old_dataset').mock(return_value=Response(204))

        client = AdminClient('http://localhost:8080')
        client.datasets.delete('_', 'old_dataset')

        # Should complete without error

    @respx.mock
    def test_dataset_not_found(self):
        """Test handling dataset not found error."""
        error_response = {'error_code': 'DATASET_NOT_FOUND', 'error_message': 'Dataset not found'}
        respx.get('http://localhost:8080/datasets/_/missing/versions/1.0.0').mock(
            return_value=Response(404, json=error_response)
        )

        client = AdminClient('http://localhost:8080')

        with pytest.raises(DatasetNotFoundError):
            client.datasets.get_version('_', 'missing', '1.0.0')
