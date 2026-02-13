"""CLI for Amp DBT."""

import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from amp.dbt.exceptions import AmpDbtError, DependencyError, ProjectNotFoundError
from amp.dbt.monitor import JobMonitor
from amp.dbt.project import AmpDbtProject
from amp.dbt.tracker import FreshnessMonitor, ModelTracker

app = typer.Typer(name='amp-dbt', help='Amp DBT - Query composition and orchestration framework')
console = Console()


@app.command()
def init(
    project_name: Optional[str] = typer.Argument(None, help='Project name (default: current directory name)'),
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
):
    """Initialize a new Amp DBT project."""
    if project_dir is None:
        project_dir = Path.cwd()
    else:
        project_dir = Path(project_dir).resolve()

    if project_name is None:
        project_name = project_dir.name

    console.print(f'[bold green]Initializing Amp DBT project:[/bold green] {project_name}')

    # Create directory structure
    models_dir = project_dir / 'models'
    macros_dir = project_dir / 'macros'
    tests_dir = project_dir / 'tests'
    docs_dir = project_dir / 'docs'

    # Create parent directories if they don't exist
    project_dir.mkdir(parents=True, exist_ok=True)
    
    models_dir.mkdir(parents=True, exist_ok=True)
    macros_dir.mkdir(parents=True, exist_ok=True)
    tests_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    # Create dbt_project.yml
    project_config = {
        'name': project_name,
        'version': '1.0.0',
        'models': {},
        'monitoring': {
            'alert_threshold_minutes': 30,
            'check_interval_seconds': 60,
        },
    }

    import yaml

    config_path = project_dir / 'dbt_project.yml'
    if not config_path.exists():
        with open(config_path, 'w') as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)
        console.print(f'  [green]✓[/green] Created {config_path}')
    else:
        console.print(f'  [yellow]⚠[/yellow] {config_path} already exists, skipping')

    # Create .gitignore
    gitignore_path = project_dir / '.gitignore'
    gitignore_content = """# Amp DBT
.amp-dbt/
"""
    if not gitignore_path.exists():
        gitignore_path.write_text(gitignore_content)
        console.print(f'  [green]✓[/green] Created {gitignore_path}')

    # Create example model
    example_model_path = models_dir / 'example_model.sql'
    if not example_model_path.exists():
        example_model = '''-- Example model
{{ config(
    dependencies={'eth': '_/eth_firehose@1.0.0'},
    description='Example model showing how to use ref()'
) }}

SELECT
    block_num,
    block_hash,
    timestamp
FROM {{ ref('eth') }}.blocks
LIMIT 10
'''
        example_model_path.write_text(example_model)
        console.print(f'  [green]✓[/green] Created example model: {example_model_path}')

    console.print(f'\n[bold green]✓ Project initialized successfully![/bold green]')
    console.print(f'\nNext steps:')
    console.print(f'  1. Edit models in {models_dir}/')
    console.print(f'  2. Run [bold]amp-dbt compile[/bold] to compile models')
    console.print(f'  3. Run [bold]amp-dbt run[/bold] to execute models')


@app.command()
def compile(
    select: Optional[str] = typer.Option(None, '--select', '-s', help='Glob pattern to select models (e.g., stg_*)'),
    show_sql: bool = typer.Option(False, '--show-sql', help='Show compiled SQL'),
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
):
    """Compile models."""
    try:
        project = AmpDbtProject(project_dir)

        console.print('[bold]Compiling models...[/bold]\n')

        compiled_models = project.compile_all(select)

        if not compiled_models:
            console.print('[yellow]No models found to compile[/yellow]')
            return

        # Create results table
        table = Table(title='Compilation Results')
        table.add_column('Model', style='cyan')
        table.add_column('Status', style='green')
        table.add_column('Dependencies', style='yellow')

        for name, compiled in compiled_models.items():
            # Separate internal and external dependencies
            internal_deps = [k for k, v in compiled.dependencies.items() if k == v]
            external_deps = [f'{k}: {v}' for k, v in compiled.dependencies.items() if k != v]
            
            deps_parts = []
            if internal_deps:
                deps_parts.append(f'Internal: {", ".join(internal_deps)}')
            if external_deps:
                deps_parts.append(f'External: {", ".join(external_deps)}')
            
            deps_str = ' | '.join(deps_parts) if deps_parts else 'None'
            table.add_row(name, '✓ Compiled', deps_str)

        console.print(table)
        
        # Show execution order if there are internal dependencies
        has_internal = any(
            any(k == v for k, v in compiled.dependencies.items())
            for compiled in compiled_models.values()
        )
        if has_internal:
            try:
                execution_order = project.get_execution_order(select)
                console.print(f'\n[bold]Execution order:[/bold] {" → ".join(execution_order)}')
            except Exception:
                pass  # Skip if we can't determine order

        if show_sql:
            console.print('\n[bold]Compiled SQL:[/bold]\n')
            for name, compiled in compiled_models.items():
                console.print(f'[bold cyan]{name}:[/bold cyan]')
                console.print(f'[dim]{compiled.sql[:500]}{"..." if len(compiled.sql) > 500 else ""}[/dim]\n')

    except ProjectNotFoundError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        console.print('Run [bold]amp-dbt init[/bold] to initialize a project')
        sys.exit(1)
    except AmpDbtError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        sys.exit(1)
    except Exception as e:
        console.print(f'[bold red]Unexpected error:[/bold red] {e}')
        sys.exit(1)


@app.command()
def list(
    select: Optional[str] = typer.Option(None, '--select', '-s', help='Glob pattern to filter models'),
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
):
    """List all models in the project."""
    try:
        project = AmpDbtProject(project_dir)

        models = project.find_models(select)

        if not models:
            console.print('[yellow]No models found[/yellow]')
            return

        table = Table(title='Models')
        table.add_column('Model', style='cyan')
        table.add_column('Path', style='dim')

        for model_path in models:
            relative_path = model_path.relative_to(project.project_root)
            table.add_row(model_path.stem, str(relative_path))

        console.print(table)

    except ProjectNotFoundError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        console.print('Run [bold]amp-dbt init[/bold] to initialize a project')
        sys.exit(1)
    except Exception as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        sys.exit(1)


@app.command()
def run(
    select: Optional[str] = typer.Option(None, '--select', '-s', help='Glob pattern to select models (e.g., stg_*)'),
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
    dry_run: bool = typer.Option(False, '--dry-run', help='Show what would be executed without running'),
):
    """Execute models in dependency order."""
    try:
        project = AmpDbtProject(project_dir)

        console.print('[bold]Compiling models...[/bold]\n')

        # Compile all models (with dependency resolution)
        compiled_models = project.compile_all(select)

        if not compiled_models:
            console.print('[yellow]No models found to execute[/yellow]')
            return

        # Get execution order
        execution_order = project.get_execution_order(select)

        if dry_run:
            console.print('[bold yellow]Dry run mode - showing execution plan[/bold yellow]\n')
            table = Table(title='Execution Plan')
            table.add_column('Order', style='cyan')
            table.add_column('Model', style='green')
            table.add_column('Dependencies', style='yellow')

            for idx, model_name in enumerate(execution_order, 1):
                compiled = compiled_models[model_name]
                deps = [d for d in compiled.dependencies.keys() if d in compiled_models]
                deps_str = ', '.join(deps) if deps else 'None'
                table.add_row(str(idx), model_name, deps_str)

            console.print(table)
            console.print(f'\n[dim]Would execute {len(execution_order)} models[/dim]')
            return

        # Execute models in order
        console.print(f'[bold]Executing {len(execution_order)} models...[/bold]\n')

        for idx, model_name in enumerate(execution_order, 1):
            compiled = compiled_models[model_name]
            console.print(f'[{idx}/{len(execution_order)}] [cyan]{model_name}[/cyan]')

            # In Phase 2, we just show the compiled SQL
            # Actual execution will be added in later phases
            console.print(f'  [dim]Compiled SQL ({len(compiled.sql)} chars)[/dim]')
            console.print(f'  [green]✓[/green] Ready to execute\n')

        console.print('[bold green]✓ All models compiled successfully![/bold green]')
        console.print('\n[dim]Note: Actual query execution will be implemented in Phase 3[/dim]')

        # Update state tracking (Phase 3)
        tracker = ModelTracker(project.project_root)
        for model_name in execution_order:
            # Mark as ready (actual execution would update with real job_id and blocks)
            tracker.update_progress(model_name, status='ready')

    except DependencyError as e:
        console.print(f'[bold red]Dependency Error:[/bold red] {e}')
        sys.exit(1)
    except ProjectNotFoundError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        console.print('Run [bold]amp-dbt init[/bold] to initialize a project')
        sys.exit(1)
    except AmpDbtError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        sys.exit(1)
    except Exception as e:
        console.print(f'[bold red]Unexpected error:[/bold red] {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)


@app.command()
def status(
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
    all: bool = typer.Option(False, '--all', help='Show status for all models'),
):
    """Check data freshness status."""
    try:
        project = AmpDbtProject(project_dir)
        tracker = ModelTracker(project.project_root)
        freshness_monitor = FreshnessMonitor(tracker)

        if all:
            # Check all models
            results = freshness_monitor.check_all_freshness()
        else:
            # Check models that have state
            states = tracker.get_all_states()
            results = {}
            for model_name in states.keys():
                results[model_name] = freshness_monitor.check_freshness(model_name)

        if not results:
            console.print('[yellow]No models with tracked state found[/yellow]')
            console.print('Run [bold]amp-dbt run[/bold] to start tracking models')
            return

        # Create status table
        table = Table(title='Data Freshness Status')
        table.add_column('Model', style='cyan')
        table.add_column('Status', style='green')
        table.add_column('Latest Block', style='yellow')
        table.add_column('Age', style='dim')

        for model_name, result in results.items():
            if result.stale:
                status_icon = '⚠️ Stale'
                status_style = 'red'
            elif result.latest_timestamp:
                status_icon = '✅ Fresh'
                status_style = 'green'
            else:
                status_icon = '❌ Error'
                status_style = 'red'

            block_str = str(result.latest_block) if result.latest_block else '-'
            age_str = str(result.age).split('.')[0] if result.age else '-'

            table.add_row(model_name, f'[{status_style}]{status_icon}[/{status_style}]', block_str, age_str)

        console.print(table)

    except ProjectNotFoundError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        console.print('Run [bold]amp-dbt init[/bold] to initialize a project')
        sys.exit(1)
    except Exception as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)


@app.command()
def monitor(
    project_dir: Optional[Path] = typer.Option(None, '--project-dir', help='Project directory (default: current directory)'),
    watch: bool = typer.Option(False, '--watch', help='Auto-refresh dashboard'),
    interval: int = typer.Option(5, '--interval', help='Refresh interval in seconds (default: 5)'),
):
    """Interactive job monitoring dashboard."""
    try:
        project = AmpDbtProject(project_dir)
        tracker = ModelTracker(project.project_root)

        # Load job mappings from .amp-dbt/jobs.json if it exists
        jobs_file = project.project_root / '.amp-dbt' / 'jobs.json'
        model_job_map = {}

        if jobs_file.exists():
            import json

            with open(jobs_file, 'r') as f:
                model_job_map = json.load(f)

        console.print('[bold]Job Monitor[/bold]\n')

        if watch:
            console.print(f'[dim]Refreshing every {interval} seconds. Press Ctrl+C to stop.[/dim]\n')

        import time

        try:
            while True:
                # Clear screen (works on most terminals)
                if watch:
                    console.print('\033[2J\033[H', end='')

                # Get all model states
                states = tracker.get_all_states()

                if not states:
                    console.print('[yellow]No tracked models found[/yellow]')
                    console.print('Run [bold]amp-dbt run[/bold] to start tracking models')
                    break

                # Create monitor table
                table = Table(title='Amp DBT Job Monitor')
                table.add_column('Model', style='cyan')
                table.add_column('Job ID', style='yellow')
                table.add_column('Status', style='green')
                table.add_column('Latest Block', style='dim')
                table.add_column('Last Updated', style='dim')

                for model_name, state in states.items():
                    job_id_str = str(state.job_id) if state.job_id else '-'
                    block_str = str(state.latest_block) if state.latest_block else '-'
                    updated_str = (
                        state.last_updated.strftime('%Y-%m-%d %H:%M:%S')
                        if state.last_updated
                        else '-'
                    )

                    # Status styling
                    if state.status == 'fresh':
                        status_icon = '✅ Fresh'
                        status_style = 'green'
                    elif state.status == 'stale':
                        status_icon = '⚠️ Stale'
                        status_style = 'yellow'
                    elif state.status == 'error':
                        status_icon = '❌ Error'
                        status_style = 'red'
                    elif state.status == 'running':
                        status_icon = '🔄 Running'
                        status_style = 'cyan'
                    else:
                        status_icon = '❓ Unknown'
                        status_style = 'dim'

                    table.add_row(
                        model_name,
                        job_id_str,
                        f'[{status_style}]{status_icon}[/{status_style}]',
                        block_str,
                        updated_str,
                    )

                console.print(table)

                if not watch:
                    break

                time.sleep(interval)

        except KeyboardInterrupt:
            console.print('\n[yellow]Monitoring stopped[/yellow]')

    except ProjectNotFoundError as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        console.print('Run [bold]amp-dbt init[/bold] to initialize a project')
        sys.exit(1)
    except Exception as e:
        console.print(f'[bold red]Error:[/bold red] {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Entry point for CLI."""
    app()


if __name__ == '__main__':
    main()

