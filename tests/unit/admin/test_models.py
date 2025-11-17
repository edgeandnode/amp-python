"""Unit tests for admin Pydantic models."""

from amp.admin import models


class TestDatasetModels:
    """Test dataset-related models."""

    def test_dataset_model(self):
        """Test Dataset model validation."""
        dataset = models.Dataset(namespace='_', name='eth_firehose', version='1.0.0')

        assert dataset.namespace == '_'
        assert dataset.name == 'eth_firehose'
        assert dataset.version == '1.0.0'

    def test_register_request_with_dict_manifest(self):
        """Test RegisterRequest with full manifest dict."""
        manifest = {
            'kind': 'manifest',
            'dependencies': {'eth': '_/eth_firehose@0.0.0'},
            'tables': {'blocks': {'input': {'sql': 'SELECT * FROM eth.blocks'}, 'network': 'mainnet'}},
            'functions': {},
        }

        request = models.RegisterRequest(namespace='_', name='test_dataset', version='1.0.0', manifest=manifest)

        assert request.namespace == '_'
        assert request.name == 'test_dataset'
        assert request.version == '1.0.0'
        assert isinstance(request.manifest, dict)
        assert request.manifest['kind'] == 'manifest'

    def test_register_request_optional_version(self):
        """Test RegisterRequest with optional version."""
        manifest = {'kind': 'manifest', 'dependencies': {}, 'tables': {}, 'functions': {}}

        request = models.RegisterRequest(namespace='_', name='test_dataset', manifest=manifest)

        assert request.version is None


class TestJobModels:
    """Test job-related models."""

    def test_deploy_response(self):
        """Test DeployResponse model."""
        response = models.DeployResponse(job_id=123)

        assert response.job_id == 123

    def test_deploy_response_job_id(self):
        """Test DeployResponse has job_id field."""
        response = models.DeployResponse(job_id=456)

        assert response.job_id == 456


class TestSchemaModels:
    """Test schema-related models."""

    def test_output_schema_request(self):
        """Test OutputSchemaRequest model."""
        request = models.OutputSchemaRequest(sql_query='SELECT * FROM eth.blocks', is_sql_dataset=True)

        assert request.sql_query == 'SELECT * FROM eth.blocks'
        assert request.is_sql_dataset is True

    def test_output_schema_request_defaults(self):
        """Test OutputSchemaRequest with default values."""
        request = models.OutputSchemaRequest(sql_query='SELECT 1')

        assert request.sql_query == 'SELECT 1'
        # is_sql_dataset should have a default if defined in the model


class TestEndBlockModel:
    """Test EndBlock model."""

    def test_end_block_with_value(self):
        """Test EndBlock with a value."""
        end_block = models.EndBlock(value='latest')

        assert end_block.value == 'latest'

    def test_end_block_none(self):
        """Test EndBlock with None (continuous)."""
        end_block = models.EndBlock(value=None)

        assert end_block.value is None

    def test_end_block_default(self):
        """Test EndBlock with default (no value provided)."""
        end_block = models.EndBlock()

        assert end_block.value is None
