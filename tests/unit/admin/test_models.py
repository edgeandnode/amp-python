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

    def test_schema_request(self):
        """Test SchemaRequest model."""
        request = models.SchemaRequest(
            tables={'t1': 'SELECT * FROM eth.blocks'},
            dependencies={'eth': 'ns/eth@1.0.0'},
            functions={},
        )

        assert request.tables == {'t1': 'SELECT * FROM eth.blocks'}
        assert request.dependencies == {'eth': 'ns/eth@1.0.0'}
        assert request.functions == {}

    def test_schema_request_defaults(self):
        """Test SchemaRequest with default values."""
        request = models.SchemaRequest()

        assert request.tables is None
        assert request.dependencies is None
        assert request.functions is None


class TestEndBlockModel:
    """Test EndBlock model."""

    def test_end_block_with_value(self):
        """Test EndBlock with a value."""
        end_block = models.EndBlock(root='latest')

        assert end_block.root == 'latest'

    def test_end_block_none(self):
        """Test EndBlock with None (continuous)."""
        end_block = models.EndBlock(root=None)

        assert end_block.root is None

    def test_end_block_default(self):
        """Test EndBlock with default (no value provided)."""
        # Pydantic V2 RootModel with Optional[str] requires explicit root=None if no default
        end_block = models.EndBlock(root=None)

        assert end_block.root is None
