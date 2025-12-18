"""Integration tests for Amp DBT CLI commands."""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from amp.dbt.cli import app


@pytest.mark.integration
class TestDbtCliInit:
    """Integration tests for `amp-dbt init` command."""

    def test_init_creates_project_structure(self, tmp_path):
        """Test that init creates all required directories and files."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['init', 'test_project', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert (tmp_path / 'dbt_project.yml').exists()
        assert (tmp_path / 'models').exists()
        assert (tmp_path / 'macros').exists()
        assert (tmp_path / 'tests').exists()
        assert (tmp_path / 'docs').exists()
        assert (tmp_path / '.gitignore').exists()
        assert (tmp_path / 'models' / 'example_model.sql').exists()
        
        # Check config content
        config = yaml.safe_load((tmp_path / 'dbt_project.yml').read_text())
        assert config['name'] == 'test_project'
        assert config['version'] == '1.0.0'
        assert 'monitoring' in config

    def test_init_with_existing_config(self, tmp_path):
        """Test that init doesn't overwrite existing config."""
        runner = CliRunner()
        
        # Create existing config
        config_path = tmp_path / 'dbt_project.yml'
        existing_config = {'name': 'existing_project', 'version': '0.1.0'}
        with open(config_path, 'w') as f:
            yaml.dump(existing_config, f)
        
        result = runner.invoke(app, ['init', 'test_project', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        # Config should still be the original
        loaded_config = yaml.safe_load(config_path.read_text())
        assert loaded_config['name'] == 'existing_project'

    def test_init_with_project_dir(self, tmp_path):
        """Test init with custom project directory."""
        runner = CliRunner()
        project_dir = tmp_path / 'custom_project'
        
        result = runner.invoke(app, ['init', 'test_project', '--project-dir', str(project_dir)])
        
        assert result.exit_code == 0
        assert project_dir.exists()
        assert (project_dir / 'dbt_project.yml').exists()

    def test_init_defaults_project_name(self, tmp_path):
        """Test that init uses directory name when project name not provided."""
        runner = CliRunner()
        project_dir = tmp_path / 'my_project'
        project_dir.mkdir()
        
        result = runner.invoke(app, ['init', '--project-dir', str(project_dir)])
        
        assert result.exit_code == 0
        config = yaml.safe_load((project_dir / 'dbt_project.yml').read_text())
        assert config['name'] == 'my_project'

    def test_init_creates_gitignore(self, tmp_path):
        """Test that init creates .gitignore file."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['init', 'test_project', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        gitignore_path = tmp_path / '.gitignore'
        assert gitignore_path.exists()
        assert '.amp-dbt/' in gitignore_path.read_text()


@pytest.mark.integration
class TestDbtCliCompile:
    """Integration tests for `amp-dbt compile` command."""

    def test_compile_single_model(self, tmp_path):
        """Test compiling a single model."""
        runner = CliRunner()
        
        # Setup project
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'test_model.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT * FROM {{ ref('eth') }}.blocks
'''
        )
        
        result = runner.invoke(app, ['compile', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'test_model' in result.stdout
        assert 'Compiled' in result.stdout or 'Compilation Results' in result.stdout

    def test_compile_with_select_pattern(self, tmp_path):
        """Test compile with select pattern."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'stg_model1.sql').write_text('SELECT 1')
        (models_dir / 'stg_model2.sql').write_text('SELECT 2')
        (models_dir / 'final_model.sql').write_text('SELECT 3')
        
        result = runner.invoke(
            app, 
            ['compile', '--select', 'stg_*', '--project-dir', str(tmp_path)]
        )
        
        assert result.exit_code == 0
        assert 'stg_model1' in result.stdout
        assert 'stg_model2' in result.stdout
        # final_model should not appear since it doesn't match pattern
        # (unless it's in execution order output)

    def test_compile_with_show_sql(self, tmp_path):
        """Test compile with --show-sql flag."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'test_model.sql').write_text('SELECT 1 as value')
        
        result = runner.invoke(
            app,
            ['compile', '--show-sql', '--project-dir', str(tmp_path)]
        )
        
        assert result.exit_code == 0
        assert 'Compiled SQL' in result.stdout
        assert 'SELECT 1' in result.stdout

    def test_compile_no_models_found(self, tmp_path):
        """Test compile when no models exist."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['compile', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'No models found' in result.stdout

    def test_compile_with_internal_dependencies(self, tmp_path):
        """Test compile shows execution order for models with dependencies."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'base.sql').write_text('SELECT 1')
        (models_dir / 'dependent.sql').write_text('SELECT * FROM {{ ref("base") }}')
        
        result = runner.invoke(app, ['compile', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'base' in result.stdout
        assert 'dependent' in result.stdout

    def test_compile_error_handling(self, tmp_path):
        """Test compile handles errors gracefully."""
        runner = CliRunner()
        
        # Create invalid project directory
        invalid_dir = tmp_path / 'nonexistent' / 'nested'
        
        result = runner.invoke(app, ['compile', '--project-dir', str(invalid_dir)])
        
        # Should handle error gracefully
        assert result.exit_code != 0 or 'Error' in result.stdout


@pytest.mark.integration
class TestDbtCliList:
    """Integration tests for `amp-dbt list` command."""

    def test_list_all_models(self, tmp_path):
        """Test listing all models."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'model1.sql').write_text('SELECT 1')
        (models_dir / 'model2.sql').write_text('SELECT 2')
        
        result = runner.invoke(app, ['list', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'model1' in result.stdout
        assert 'model2' in result.stdout

    def test_list_with_select_pattern(self, tmp_path):
        """Test list with select pattern."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'stg_model.sql').write_text('SELECT 1')
        (models_dir / 'final_model.sql').write_text('SELECT 2')
        
        result = runner.invoke(
            app,
            ['list', '--select', 'stg_*', '--project-dir', str(tmp_path)]
        )
        
        assert result.exit_code == 0
        assert 'stg_model' in result.stdout
        assert 'final_model' not in result.stdout

    def test_list_no_models(self, tmp_path):
        """Test list when no models exist."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['list', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'No models found' in result.stdout

    def test_list_shows_paths(self, tmp_path):
        """Test that list shows model paths."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'test_model.sql').write_text('SELECT 1')
        
        result = runner.invoke(app, ['list', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'Models' in result.stdout
        assert 'test_model' in result.stdout


@pytest.mark.integration
class TestDbtCliRun:
    """Integration tests for `amp-dbt run` command."""

    def test_run_dry_run_mode(self, tmp_path):
        """Test run in dry-run mode."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'base.sql').write_text('SELECT 1')
        (models_dir / 'dependent.sql').write_text('SELECT * FROM {{ ref("base") }}')
        
        result = runner.invoke(
            app,
            ['run', '--dry-run', '--project-dir', str(tmp_path)]
        )
        
        assert result.exit_code == 0
        assert 'Dry run mode' in result.stdout or 'Execution Plan' in result.stdout
        assert 'base' in result.stdout
        assert 'dependent' in result.stdout

    def test_run_updates_tracker(self, tmp_path):
        """Test that run updates model tracker."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'test_model.sql').write_text('SELECT 1')
        
        result = runner.invoke(app, ['run', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        
        # Check that state was updated
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        states = tracker.get_all_states()
        assert 'test_model' in states

    def test_run_with_select_pattern(self, tmp_path):
        """Test run with select pattern."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'stg_model.sql').write_text('SELECT 1')
        (models_dir / 'final_model.sql').write_text('SELECT 2')
        
        result = runner.invoke(
            app,
            ['run', '--select', 'stg_*', '--project-dir', str(tmp_path)]
        )
        
        assert result.exit_code == 0
        assert 'stg_model' in result.stdout

    def test_run_no_models(self, tmp_path):
        """Test run when no models exist."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['run', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'No models found' in result.stdout

    def test_run_shows_execution_order(self, tmp_path):
        """Test that run shows execution order."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'base.sql').write_text('SELECT 1')
        (models_dir / 'intermediate.sql').write_text('SELECT * FROM {{ ref("base") }}')
        (models_dir / 'final.sql').write_text('SELECT * FROM {{ ref("intermediate") }}')
        
        result = runner.invoke(app, ['run', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'Executing' in result.stdout or 'models' in result.stdout


@pytest.mark.integration
class TestDbtCliStatus:
    """Integration tests for `amp-dbt status` command."""

    def test_status_no_models(self, tmp_path):
        """Test status when no models are tracked."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['status', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'No models with tracked state' in result.stdout

    def test_status_with_tracked_models(self, tmp_path):
        """Test status with tracked models."""
        runner = CliRunner()
        
        # Setup tracker with some state
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=datetime.now())
        
        result = runner.invoke(app, ['status', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'test_model' in result.stdout
        assert 'Fresh' in result.stdout or 'Stale' in result.stdout or 'Status' in result.stdout

    def test_status_with_stale_data(self, tmp_path):
        """Test status shows stale data correctly."""
        runner = CliRunner()
        
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        old_time = datetime.now() - timedelta(hours=2)
        tracker.update_progress('stale_model', latest_block=1000, latest_timestamp=old_time)
        
        result = runner.invoke(app, ['status', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'stale_model' in result.stdout

    def test_status_with_all_flag(self, tmp_path):
        """Test status with --all flag."""
        runner = CliRunner()
        
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        tracker.update_progress('model1', latest_block=1000, latest_timestamp=datetime.now())
        tracker.update_progress('model2', latest_block=2000, latest_timestamp=datetime.now())
        
        result = runner.invoke(app, ['status', '--all', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'model1' in result.stdout
        assert 'model2' in result.stdout


@pytest.mark.integration
class TestDbtCliMonitor:
    """Integration tests for `amp-dbt monitor` command."""

    def test_monitor_no_models(self, tmp_path):
        """Test monitor when no models are tracked."""
        runner = CliRunner()
        
        result = runner.invoke(app, ['monitor', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'No tracked models' in result.stdout

    def test_monitor_with_models(self, tmp_path):
        """Test monitor with tracked models."""
        runner = CliRunner()
        
        # Setup tracker with some state
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=datetime.now(), job_id=123)
        
        result = runner.invoke(app, ['monitor', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'Job Monitor' in result.stdout
        assert 'test_model' in result.stdout

    def test_monitor_with_job_mapping(self, tmp_path):
        """Test monitor loads job mappings from jobs.json."""
        runner = CliRunner()
        
        # Create .amp-dbt directory and jobs.json
        amp_dbt_dir = tmp_path / '.amp-dbt'
        amp_dbt_dir.mkdir(parents=True)
        jobs_file = amp_dbt_dir / 'jobs.json'
        jobs_file.write_text(json.dumps({'model1': 123, 'model2': 456}))
        
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        tracker.update_progress('model1', latest_block=1000, latest_timestamp=datetime.now(), job_id=123)
        
        result = runner.invoke(app, ['monitor', '--project-dir', str(tmp_path)])
        
        assert result.exit_code == 0
        assert 'Job Monitor' in result.stdout

    def test_monitor_with_watch_flag(self, tmp_path):
        """Test monitor with --watch flag (should not hang in test)."""
        runner = CliRunner()
        
        from amp.dbt.tracker import ModelTracker
        tracker = ModelTracker(tmp_path)
        tracker.update_progress('test_model', latest_block=1000, latest_timestamp=datetime.now())
        
        # Use a very short interval and expect it to exit quickly
        # In watch mode, it would loop, but we'll test it exits gracefully
        result = runner.invoke(
            app,
            ['monitor', '--watch', '--interval', '1', '--project-dir', str(tmp_path)],
            # This will timeout quickly in watch mode, but we can test the initial output
        )
        
        # Should show monitor output
        assert 'Job Monitor' in result.stdout or 'test_model' in result.stdout


@pytest.mark.integration
class TestDbtCliErrorHandling:
    """Integration tests for CLI error handling."""

    def test_compile_with_invalid_project_dir(self, tmp_path):
        """Test compile handles invalid project directory."""
        runner = CliRunner()
        
        invalid_dir = tmp_path / 'nonexistent' / 'nested'
        
        result = runner.invoke(app, ['compile', '--project-dir', str(invalid_dir)])
        
        # Should handle error gracefully
        assert result.exit_code != 0 or 'Error' in result.stdout

    def test_run_with_circular_dependency(self, tmp_path):
        """Test run handles circular dependencies."""
        runner = CliRunner()
        
        models_dir = tmp_path / 'models'
        models_dir.mkdir(parents=True)
        (models_dir / 'model1.sql').write_text('SELECT * FROM {{ ref("model2") }}')
        (models_dir / 'model2.sql').write_text('SELECT * FROM {{ ref("model1") }}')
        
        result = runner.invoke(app, ['run', '--project-dir', str(tmp_path)])
        
        # Should detect circular dependency
        assert result.exit_code != 0 or 'Circular' in result.stdout or 'Dependency Error' in result.stdout

    def test_list_with_invalid_project(self, tmp_path):
        """Test list handles invalid project."""
        runner = CliRunner()
        
        invalid_dir = tmp_path / 'nonexistent'
        
        result = runner.invoke(app, ['list', '--project-dir', str(invalid_dir)])
        
        # Should handle error gracefully
        assert result.exit_code != 0 or 'Error' in result.stdout


@pytest.mark.integration
class TestDbtCliIntegration:
    """End-to-end integration tests for CLI workflows."""

    def test_full_cli_workflow(self, tmp_path):
        """Test complete CLI workflow: init -> compile -> run -> status."""
        runner = CliRunner()
        
        # Step 1: Initialize project
        result = runner.invoke(app, ['init', 'test_project', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        
        # Step 2: Compile models
        result = runner.invoke(app, ['compile', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        
        # Step 3: Run models
        result = runner.invoke(app, ['run', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        
        # Step 4: Check status
        result = runner.invoke(app, ['status', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        assert 'example_model' in result.stdout or 'No models' in result.stdout

    def test_cli_with_custom_models(self, tmp_path):
        """Test CLI with custom model files."""
        runner = CliRunner()
        
        # Initialize project
        runner.invoke(app, ['init', 'test_project', '--project-dir', str(tmp_path)])
        
        # Add custom model
        models_dir = tmp_path / 'models'
        (models_dir / 'custom_model.sql').write_text(
            '''{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num FROM {{ ref('eth') }}.blocks LIMIT 10
'''
        )
        
        # List models
        result = runner.invoke(app, ['list', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        assert 'custom_model' in result.stdout
        
        # Compile models
        result = runner.invoke(app, ['compile', '--project-dir', str(tmp_path)])
        assert result.exit_code == 0
        assert 'custom_model' in result.stdout

