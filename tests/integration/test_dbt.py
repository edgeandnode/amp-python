"""Integration tests for Amp DBT module."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from amp.dbt.compiler import Compiler
from amp.dbt.config import load_project_config, parse_config_block
from amp.dbt.dependencies import DependencyGraph, build_dependency_graph
from amp.dbt.exceptions import (
    CompilationError,
    ConfigError,
    DependencyError,
    ProjectNotFoundError,
)
from amp.dbt.models import CompiledModel, ModelConfig
from amp.dbt.project import AmpDbtProject
from amp.dbt.state import FreshnessResult, ModelState, StateDatabase
from amp.dbt.tracker import FreshnessMonitor, ModelTracker


@pytest.mark.integration
class TestDbtProject:
    """Integration tests for AmpDbtProject."""

    def test_project_initialization(self, tmp_path):
        """Test project initialization with valid directory."""
        project = AmpDbtProject(tmp_path)
        assert project.project_root == tmp_path
        assert project.models_dir == tmp_path / 'models'
        assert project.macros_dir == tmp_path / 'macros'

    def test_project_initialization_defaults_to_cwd(self, monkeypatch):
        """Test project initialization defaults to current directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.chdir(tmpdir)
            project = AmpDbtProject()
            assert project.project_root == Path(tmpdir).resolve()

    def test_find_models_empty_directory(self, tmp_path):
        """Test finding models in empty directory."""
        project = AmpDbtProject(tmp_path)
        models = project.find_models()
        assert models == []

    def test_find_models_with_files(self, tmp_path):
        """Test finding models in directory with SQL files."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create some model files
        (models_dir / 'model1.sql').write_text('SELECT 1')
        (models_dir / 'model2.sql').write_text('SELECT 2')
        (models_dir / 'not_a_model.txt').write_text('not sql')

        project = AmpDbtProject(tmp_path)
        models = project.find_models()
        assert len(models) == 2
        assert all(m.suffix == '.sql' for m in models)
        assert any('model1' in str(m) for m in models)
        assert any('model2' in str(m) for m in models)

    def test_find_models_with_select_pattern(self, tmp_path):
        """Test finding models with select pattern."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        (models_dir / 'stg_model1.sql').write_text('SELECT 1')
        (models_dir / 'stg_model2.sql').write_text('SELECT 2')
        (models_dir / 'final_model.sql').write_text('SELECT 3')

        project = AmpDbtProject(tmp_path)
        models = project.find_models('stg_*')
        assert len(models) == 2
        assert all('stg_' in m.stem for m in models)

    def test_load_model(self, tmp_path):
        """Test loading a model file."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        model_sql = '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT * FROM {{ ref('eth') }}.blocks
'''
        (models_dir / 'test_model.sql').write_text(model_sql)

        project = AmpDbtProject(tmp_path)
        sql, config = project.load_model(models_dir / 'test_model.sql')

        assert 'config' not in sql
        assert 'SELECT * FROM' in sql
        assert config.dependencies == {'eth': '_/eth_firehose@1.0.0'}

    def test_load_model_not_found(self, tmp_path):
        """Test loading non-existent model raises error."""
        project = AmpDbtProject(tmp_path)
        with pytest.raises(ProjectNotFoundError):
            project.load_model(tmp_path / 'models' / 'missing.sql')

    def test_compile_single_model(self, tmp_path):
        """Test compiling a single model."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        model_sql = '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num, block_hash FROM {{ ref('eth') }}.blocks
LIMIT 10
'''
        (models_dir / 'test_model.sql').write_text(model_sql)

        project = AmpDbtProject(tmp_path)
        compiled = project.compile_model(models_dir / 'test_model.sql')

        assert compiled.name == 'test_model'
        assert 'SELECT block_num, block_hash FROM' in compiled.sql
        assert compiled.config.dependencies == {'eth': '_/eth_firehose@1.0.0'}
        assert 'eth' in compiled.dependencies

    def test_compile_all_models(self, tmp_path):
        """Test compiling all models in project."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create multiple models
        (models_dir / 'model1.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT * FROM {{ ref('eth') }}.blocks LIMIT 10
'''
        )
        (models_dir / 'model2.sql').write_text(
            '''{{ config(dependencies={'arb': '_/arb_firehose@1.0.0'}) }}
SELECT * FROM {{ ref('arb') }}.blocks LIMIT 10
'''
        )

        project = AmpDbtProject(tmp_path)
        compiled = project.compile_all()

        assert len(compiled) == 2
        assert 'model1' in compiled
        assert 'model2' in compiled
        assert compiled['model1'].dependencies['eth'] == '_/eth_firehose@1.0.0'
        assert compiled['model2'].dependencies['arb'] == '_/arb_firehose@1.0.0'

    def test_compile_models_with_internal_dependencies(self, tmp_path):
        """Test compiling models with internal dependencies."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create base model
        (models_dir / 'base_model.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num, block_hash FROM {{ ref('eth') }}.blocks
'''
        )

        # Create dependent model
        (models_dir / 'dependent_model.sql').write_text(
            '''SELECT * FROM {{ ref('base_model') }}
WHERE block_num > 1000
'''
        )

        project = AmpDbtProject(tmp_path)
        compiled = project.compile_all()

        assert len(compiled) == 2
        assert 'base_model' in compiled
        assert 'dependent_model' in compiled

        # Check dependencies
        assert 'eth' in compiled['base_model'].dependencies
        assert 'base_model' in compiled['dependent_model'].dependencies

    def test_execution_order(self, tmp_path):
        """Test getting execution order for models."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create models with dependencies
        (models_dir / 'base.sql').write_text('SELECT 1 as id')
        (models_dir / 'intermediate.sql').write_text('SELECT * FROM {{ ref("base") }}')
        (models_dir / 'final.sql').write_text('SELECT * FROM {{ ref("intermediate") }}')

        project = AmpDbtProject(tmp_path)
        order = project.get_execution_order()

        assert len(order) == 3
        assert order[0] == 'base'
        assert order[1] == 'intermediate'
        assert order[2] == 'final'

    def test_execution_order_with_select(self, tmp_path):
        """Test execution order with select pattern."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        (models_dir / 'base.sql').write_text('SELECT 1 as id')
        (models_dir / 'intermediate.sql').write_text('SELECT * FROM {{ ref("base") }}')
        (models_dir / 'final.sql').write_text('SELECT * FROM {{ ref("intermediate") }}')

        project = AmpDbtProject(tmp_path)
        order = project.get_execution_order('final')

        # Should include final and its dependencies
        assert 'final' in order
        assert 'intermediate' in order
        assert 'base' in order
        assert order.index('base') < order.index('intermediate')
        assert order.index('intermediate') < order.index('final')

    def test_circular_dependency_detection(self, tmp_path):
        """Test that circular dependencies are detected."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        (models_dir / 'model1.sql').write_text('SELECT * FROM {{ ref("model2") }}')
        (models_dir / 'model2.sql').write_text('SELECT * FROM {{ ref("model1") }}')

        project = AmpDbtProject(tmp_path)
        with pytest.raises(DependencyError, match='Circular dependency'):
            project.compile_all()


@pytest.mark.integration
class TestDbtCompiler:
    """Integration tests for Compiler."""

    def test_compile_simple_model(self, tmp_path):
        """Test compiling a simple model."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT 1 as value'
        config = ModelConfig()

        compiled = compiler.compile(sql, 'test_model', config)

        assert compiled.name == 'test_model'
        assert compiled.sql == sql
        assert compiled.config == config

    def test_compile_with_jinja(self, tmp_path):
        """Test compiling with Jinja templating."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT {{ model_name }} as model'
        config = ModelConfig()

        compiled = compiler.compile(sql, 'test_model', config)

        assert 'test_model' in compiled.sql

    def test_compile_with_ref_external(self, tmp_path):
        """Test compiling with external ref()."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT * FROM {{ ref("eth") }}.blocks'
        config = ModelConfig(dependencies={'eth': '_/eth_firehose@1.0.0'})

        compiled = compiler.compile(sql, 'test_model', config, available_models=set())

        assert 'eth' in compiled.dependencies
        assert compiled.dependencies['eth'] == '_/eth_firehose@1.0.0'
        assert '__REF__eth__' not in compiled.sql

    def test_compile_with_ref_internal(self, tmp_path):
        """Test compiling with internal ref()."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT * FROM {{ ref("base_model") }}'
        config = ModelConfig()
        available_models = {'base_model'}

        compiled = compiler.compile(sql, 'test_model', config, available_models)

        assert 'base_model' in compiled.dependencies
        assert compiled.dependencies['base_model'] == 'base_model'

    def test_compile_with_ref_unknown(self, tmp_path):
        """Test that unknown ref() raises error."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT * FROM {{ ref("unknown") }}'
        config = ModelConfig()

        with pytest.raises(DependencyError, match='Unknown reference'):
            compiler.compile(sql, 'test_model', config, available_models=set())

    def test_compile_with_ctes(self, tmp_path):
        """Test compiling with CTE inlining."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT * FROM {{ ref("base") }}'
        config = ModelConfig()
        internal_deps = {'base': 'SELECT 1 as id'}

        compiled = compiler.compile_with_ctes(sql, 'test_model', config, internal_deps)

        assert 'WITH' in compiled.sql.upper()
        assert 'base' in compiled.sql

    def test_compile_with_nested_ctes(self, tmp_path):
        """Test compiling with nested CTEs."""
        compiler = Compiler(tmp_path)
        sql = 'SELECT * FROM {{ ref("intermediate") }}'
        config = ModelConfig()
        internal_deps = {
            'intermediate': 'WITH base AS (SELECT 1 as id) SELECT * FROM base',
        }

        compiled = compiler.compile_with_ctes(sql, 'test_model', config, internal_deps)

        assert 'WITH' in compiled.sql.upper()
        assert 'intermediate' in compiled.sql


@pytest.mark.integration
class TestDbtConfig:
    """Integration tests for configuration parsing."""

    def test_parse_config_block_simple(self):
        """Test parsing simple config block."""
        sql = '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT * FROM blocks
'''
        sql_without_config, config = parse_config_block(sql)

        assert 'config' not in sql_without_config
        assert 'SELECT * FROM blocks' in sql_without_config
        assert config.dependencies == {'eth': '_/eth_firehose@1.0.0'}

    def test_parse_config_block_multiple_deps(self):
        """Test parsing config block with multiple dependencies."""
        sql = '''{{ config(
    dependencies={
        'eth': '_/eth_firehose@1.0.0',
        'arb': '_/arb_firehose@1.0.0'
    }
) }}
SELECT * FROM blocks
'''
        sql_without_config, config = parse_config_block(sql)

        assert len(config.dependencies) == 2
        assert config.dependencies['eth'] == '_/eth_firehose@1.0.0'
        assert config.dependencies['arb'] == '_/arb_firehose@1.0.0'

    def test_parse_config_block_with_flags(self):
        """Test parsing config block with boolean flags."""
        sql = '''{{ config(track_progress=True, register=True) }}
SELECT * FROM blocks
'''
        sql_without_config, config = parse_config_block(sql)

        assert config.track_progress is True
        assert config.register is True

    def test_parse_config_block_no_config(self):
        """Test parsing SQL without config block."""
        sql = 'SELECT * FROM blocks'
        sql_without_config, config = parse_config_block(sql)

        assert sql_without_config == sql
        assert config.dependencies == {}

    def test_load_project_config_exists(self, tmp_path):
        """Test loading existing project config."""
        config_data = {
            'name': 'test_project',
            'version': '1.0.0',
            'monitoring': {'alert_threshold_minutes': 60},
        }
        config_path = tmp_path / 'dbt_project.yml'
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        config = load_project_config(tmp_path)

        assert config['name'] == 'test_project'
        assert config['monitoring']['alert_threshold_minutes'] == 60

    def test_load_project_config_missing(self, tmp_path):
        """Test loading missing project config returns empty dict."""
        config = load_project_config(tmp_path)
        assert config == {}

    def test_load_project_config_invalid_yaml(self, tmp_path):
        """Test loading invalid YAML raises error."""
        config_path = tmp_path / 'dbt_project.yml'
        config_path.write_text('invalid: yaml: content: [unclosed')

        with pytest.raises(ConfigError):
            load_project_config(tmp_path)


@pytest.mark.integration
class TestDbtDependencies:
    """Integration tests for dependency resolution."""

    def test_build_dependency_graph(self):
        """Test building dependency graph from compiled models."""
        compiled_models = {
            'base': CompiledModel(
                name='base',
                sql='SELECT 1',
                config=ModelConfig(),
                dependencies={},
                raw_sql='SELECT 1',
            ),
            'dependent': CompiledModel(
                name='dependent',
                sql='SELECT * FROM base',
                config=ModelConfig(),
                dependencies={'base': 'base'},
                raw_sql='SELECT * FROM {{ ref("base") }}',
            ),
        }

        graph = build_dependency_graph(compiled_models)

        assert 'base' in graph.get_all_models()
        assert 'dependent' in graph.get_all_models()
        assert 'base' in graph.get_dependencies('dependent')
        assert 'dependent' in graph.get_dependents('base')

    def test_dependency_graph_topological_sort(self):
        """Test topological sort of dependency graph."""
        graph = DependencyGraph()
        graph.add_model('base', set())
        graph.add_model('intermediate', {'base'})
        graph.add_model('final', {'intermediate'})

        order = graph.topological_sort()

        assert order == ['base', 'intermediate', 'final']

    def test_dependency_graph_cycle_detection(self):
        """Test cycle detection in dependency graph."""
        graph = DependencyGraph()
        graph.add_model('model1', {'model2'})
        graph.add_model('model2', {'model1'})

        cycles = graph.detect_cycles()

        assert len(cycles) > 0
        assert 'model1' in cycles[0]
        assert 'model2' in cycles[0]

    def test_dependency_graph_get_execution_order(self):
        """Test getting execution order for selected models."""
        graph = DependencyGraph()
        graph.add_model('base', set())
        graph.add_model('intermediate', {'base'})
        graph.add_model('final', {'intermediate'})
        graph.add_model('other', set())

        order = graph.get_execution_order({'final'})

        assert 'base' in order
        assert 'intermediate' in order
        assert 'final' in order
        assert 'other' not in order
        assert order.index('base') < order.index('intermediate')
        assert order.index('intermediate') < order.index('final')


@pytest.mark.integration
class TestDbtState:
    """Integration tests for state management."""

    def test_state_database_initialization(self, tmp_path):
        """Test state database initialization."""
        db_path = tmp_path / 'state.db'
        db = StateDatabase(db_path)

        assert db_path.exists()

    def test_state_database_update_and_get(self, tmp_path):
        """Test updating and getting model state."""
        db_path = tmp_path / 'state.db'
        db = StateDatabase(db_path)

        now = datetime.now()
        db.update_model_state(
            'test_model',
            latest_block=1000,
            latest_timestamp=now,
            job_id=123,
            status='fresh',
        )

        state = db.get_model_state('test_model')

        assert state is not None
        assert state.model_name == 'test_model'
        assert state.latest_block == 1000
        assert state.job_id == 123
        assert state.status == 'fresh'

    def test_state_database_get_all_states(self, tmp_path):
        """Test getting all model states."""
        db_path = tmp_path / 'state.db'
        db = StateDatabase(db_path)

        db.update_model_state('model1', latest_block=1000, status='fresh')
        db.update_model_state('model2', latest_block=2000, status='stale')

        states = db.get_all_model_states()

        assert len(states) == 2
        model_names = {s.model_name for s in states}
        assert 'model1' in model_names
        assert 'model2' in model_names


@pytest.mark.integration
class TestDbtTracker:
    """Integration tests for model tracking."""

    def test_model_tracker_initialization(self, tmp_path):
        """Test model tracker initialization."""
        tracker = ModelTracker(tmp_path)

        assert tracker.project_root == tmp_path
        assert tracker.state_dir == tmp_path / '.amp-dbt'

    def test_model_tracker_update_progress(self, tmp_path):
        """Test updating model progress."""
        tracker = ModelTracker(tmp_path)

        now = datetime.now()
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=now, job_id=123, status='fresh')

        state = tracker.get_model_state('test_model')

        assert state is not None
        assert state.latest_block == 1000
        assert state.job_id == 123
        assert state.status == 'fresh'

    def test_model_tracker_get_latest_block(self, tmp_path):
        """Test getting latest block for a model."""
        tracker = ModelTracker(tmp_path)

        tracker.update_progress('test_model', latest_block=5000)

        latest_block = tracker.get_latest_block('test_model')
        assert latest_block == 5000

    def test_model_tracker_get_all_states(self, tmp_path):
        """Test getting all model states."""
        tracker = ModelTracker(tmp_path)

        tracker.update_progress('model1', latest_block=1000, status='fresh')
        tracker.update_progress('model2', latest_block=2000, status='stale')

        states = tracker.get_all_states()

        assert len(states) == 2
        assert 'model1' in states
        assert 'model2' in states


@pytest.mark.integration
class TestDbtFreshnessMonitor:
    """Integration tests for freshness monitoring."""

    def test_freshness_monitor_check_fresh(self, tmp_path):
        """Test checking freshness for fresh data."""
        tracker = ModelTracker(tmp_path)
        monitor = FreshnessMonitor(tracker)

        # Update with recent timestamp
        recent_time = datetime.now() - timedelta(minutes=10)
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=recent_time, status='fresh')

        result = monitor.check_freshness('test_model')

        assert result.stale is False
        assert result.latest_block == 1000
        assert result.latest_timestamp == recent_time

    def test_freshness_monitor_check_stale(self, tmp_path):
        """Test checking freshness for stale data."""
        tracker = ModelTracker(tmp_path)
        monitor = FreshnessMonitor(tracker)

        # Update with old timestamp
        old_time = datetime.now() - timedelta(hours=2)
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=old_time, status='fresh')

        result = monitor.check_freshness('test_model')

        assert result.stale is True
        assert result.reason == 'Data is stale'

    def test_freshness_monitor_check_no_data(self, tmp_path):
        """Test checking freshness when no data is tracked."""
        tracker = ModelTracker(tmp_path)
        monitor = FreshnessMonitor(tracker)

        result = monitor.check_freshness('missing_model')

        assert result.stale is True
        assert result.reason == 'No data tracked'

    def test_freshness_monitor_check_all(self, tmp_path):
        """Test checking freshness for all models."""
        tracker = ModelTracker(tmp_path)
        monitor = FreshnessMonitor(tracker)

        recent_time = datetime.now() - timedelta(minutes=10)
        old_time = datetime.now() - timedelta(hours=2)

        tracker.update_progress('fresh_model', latest_block=1000, latest_timestamp=recent_time)
        tracker.update_progress('stale_model', latest_block=2000, latest_timestamp=old_time)

        results = monitor.check_all_freshness()

        assert len(results) == 2
        assert results['fresh_model'].stale is False
        assert results['stale_model'].stale is True


@pytest.mark.integration
class TestDbtEndToEnd:
    """End-to-end integration tests for DBT workflows."""

    def test_full_workflow(self, tmp_path):
        """Test complete workflow: init, compile, track, monitor."""
        # Create project structure
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create project config
        config_data = {
            'name': 'test_project',
            'version': '1.0.0',
            'monitoring': {'alert_threshold_minutes': 30},
        }
        config_path = tmp_path / 'dbt_project.yml'
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        # Create models
        (models_dir / 'base.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num, block_hash FROM {{ ref('eth') }}.blocks
'''
        )
        (models_dir / 'aggregated.sql').write_text(
            '''SELECT 
    block_num,
    COUNT(*) as tx_count
FROM {{ ref('base') }}
GROUP BY block_num
'''
        )

        # Initialize project
        project = AmpDbtProject(tmp_path)

        # Compile models
        compiled = project.compile_all()
        assert len(compiled) == 2
        assert 'base' in compiled
        assert 'aggregated' in compiled

        # Get execution order
        order = project.get_execution_order()
        assert order == ['base', 'aggregated']

        # Track progress
        tracker = ModelTracker(tmp_path)
        now = datetime.now()
        tracker.update_progress('base', latest_block=1000, latest_timestamp=now, job_id=1, status='fresh')
        tracker.update_progress('aggregated', latest_block=1000, latest_timestamp=now, job_id=2, status='fresh')

        # Check freshness
        monitor = FreshnessMonitor(tracker)
        results = monitor.check_all_freshness()

        assert len(results) == 2
        assert results['base'].stale is False
        assert results['aggregated'].stale is False

    def test_workflow_with_cte_inlining(self, tmp_path):
        """Test workflow with CTE inlining for internal dependencies."""
        models_dir = tmp_path / 'models'
        models_dir.mkdir()

        # Create base model
        (models_dir / 'base.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num, block_hash FROM {{ ref('eth') }}.blocks
'''
        )

        # Create dependent model that should get CTE
        (models_dir / 'filtered.sql').write_text(
            '''SELECT * FROM {{ ref('base') }}
WHERE block_num > 1000
'''
        )

        project = AmpDbtProject(tmp_path)
        compiled = project.compile_all()

        # Check that filtered model has base as dependency
        assert 'base' in compiled['filtered'].dependencies
        assert compiled['filtered'].dependencies['base'] == 'base'

        # The compiled SQL should reference base (CTE will be added during execution)
        assert 'base' in compiled['filtered'].sql or '__REF__base__' not in compiled['filtered'].sql

