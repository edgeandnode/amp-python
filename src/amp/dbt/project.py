"""Project management for Amp DBT."""

from pathlib import Path
from typing import Dict, List, Optional

from amp.dbt.compiler import Compiler
from amp.dbt.config import load_project_config, parse_config_block
from amp.dbt.dependencies import DependencyGraph, build_dependency_graph
from amp.dbt.exceptions import DependencyError, ProjectNotFoundError
from amp.dbt.models import CompiledModel, ModelConfig


class AmpDbtProject:
    """Main project class for Amp DBT."""

    def __init__(self, project_root: Optional[Path] = None):
        """Initialize project.

        Args:
            project_root: Root directory of the DBT project (default: current directory)

        Raises:
            ProjectNotFoundError: If project directory is invalid
        """
        if project_root is None:
            project_root = Path.cwd()

        project_root = Path(project_root).resolve()

        # Check for dbt_project.yml (optional for Phase 1)
        self.project_root = project_root
        self.models_dir = project_root / 'models'
        self.macros_dir = project_root / 'macros'

        # Initialize compiler
        self.compiler = Compiler(self.project_root, self.macros_dir)

        # Load project config
        self.config = load_project_config(self.project_root)

    def find_models(self, select: Optional[str] = None) -> List[Path]:
        """Find all model files in the project.

        Args:
            select: Optional glob pattern to filter models (e.g., 'stg_*')

        Returns:
            List of model file paths
        """
        if not self.models_dir.exists():
            return []

        pattern = select if select else '*.sql'
        models = list(self.models_dir.rglob(pattern))
        return sorted(models)

    def load_model(self, model_path: Path) -> tuple[str, ModelConfig]:
        """Load a model file and parse its config.

        Args:
            model_path: Path to model SQL file

        Returns:
            Tuple of (sql_content, ModelConfig)

        Raises:
            ProjectNotFoundError: If model file doesn't exist
        """
        if not model_path.exists():
            raise ProjectNotFoundError(f'Model file not found: {model_path}')

        sql = model_path.read_text()
        sql_without_config, config = parse_config_block(sql)

        return sql_without_config, config

    def compile_model(self, model_path: Path) -> CompiledModel:
        """Compile a single model.

        Args:
            model_path: Path to model SQL file

        Returns:
            CompiledModel with compiled SQL

        Raises:
            ProjectNotFoundError: If model file doesn't exist
        """
        # Load model and parse config (config block is removed from SQL)
        sql, config = self.load_model(model_path)

        # Get model name from file path (without extension)
        model_name = model_path.stem

        # Merge with project-level config if available
        if 'models' in self.config:
            # Apply project-level config overrides (future enhancement)
            pass

        return self.compiler.compile(sql, model_name, config)

    def compile_all(self, select: Optional[str] = None) -> Dict[str, CompiledModel]:
        """Compile all models in the project with dependency resolution.

        Args:
            select: Optional glob pattern to filter models

        Returns:
            Dictionary mapping model names to CompiledModel

        Raises:
            DependencyError: If circular dependencies are detected
        """
        # Find models matching select pattern
        selected_model_paths = self.find_models(select)
        if not selected_model_paths:
            return {}

        # To resolve dependencies properly, we need to compile ALL models first
        # to discover the dependency graph, then filter to selected + dependencies
        all_model_paths = self.find_models(None)  # Get all models

        # First pass: compile all models without CTEs to get dependencies
        # IMPORTANT: Add all model names to available_models FIRST so references can be resolved
        # even if dependencies haven't been compiled yet (we're just discovering dependencies here)
        available_models = {p.stem for p in all_model_paths}
        initial_compiled = {}

        for model_path in all_model_paths:
            try:
                sql, config = self.load_model(model_path)
                model_name = model_path.stem

                # Compile with all models available (for reference resolution)
                compiled = self.compiler.compile(sql, model_name, config, available_models)
                initial_compiled[model_name] = compiled
            except Exception as e:
                # Log error but continue with other models
                print(f'Warning: Failed to compile {model_path.name}: {e}')

        # Build dependency graph from all models
        graph = build_dependency_graph(initial_compiled)

        # Detect cycles
        cycles = graph.detect_cycles()
        if cycles:
            cycle_str = ' -> '.join(cycles[0])
            raise DependencyError(f'Circular dependency detected: {cycle_str}')

        # If select is specified, get execution order for selected models + dependencies
        if select is not None:
            selected_model_names = {p.stem for p in selected_model_paths}
            execution_order = graph.get_execution_order(selected_model_names)
        else:
            # No select - compile everything
            execution_order = graph.topological_sort()

        # Second pass: compile with CTE inlining in dependency order
        final_compiled = {}
        for model_name in execution_order:
            model_path = next(p for p in all_model_paths if p.stem == model_name)
            sql, config = self.load_model(model_path)

            # Get internal dependencies for this model
            internal_deps = {}
            for dep_name in graph.get_dependencies(model_name):
                if dep_name in final_compiled:
                    internal_deps[dep_name] = final_compiled[dep_name].sql

            # Compile with CTEs
            compiled = self.compiler.compile_with_ctes(
                sql, model_name, config, internal_deps, available_models
            )
            final_compiled[model_name] = compiled

        return final_compiled

    def get_execution_order(self, select: Optional[str] = None) -> List[str]:
        """Get execution order for models.

        Args:
            select: Optional glob pattern to filter models

        Returns:
            List of model names in execution order
        """
        model_paths = self.find_models(select)
        if not model_paths:
            return []

        # Compile to get dependencies
        compiled = {}
        for model_path in model_paths:
            try:
                sql, config = self.load_model(model_path)
                model_name = model_path.stem
                compiled[model_name] = self.compiler.compile(
                    sql, model_name, config, {p.stem for p in model_paths}
                )
            except Exception:
                pass  # Skip models that fail to compile

        graph = build_dependency_graph(compiled)
        return graph.topological_sort()

