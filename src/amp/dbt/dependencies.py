"""Dependency resolution and graph building for Amp DBT."""

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

from amp.dbt.exceptions import DependencyError
from amp.dbt.models import CompiledModel


class DependencyGraph:
    """Represents the dependency graph of models."""

    def __init__(self):
        """Initialize empty dependency graph."""
        # Maps model name -> set of dependencies (model names)
        self._dependencies: Dict[str, Set[str]] = defaultdict(set)
        # Maps model name -> set of dependents (models that depend on this one)
        self._dependents: Dict[str, Set[str]] = defaultdict(set)
        # All model names
        self._models: Set[str] = set()

    def add_model(self, model_name: str, dependencies: Set[str]):
        """Add a model and its dependencies to the graph.

        Args:
            model_name: Name of the model
            dependencies: Set of model names this model depends on
        """
        self._models.add(model_name)
        self._dependencies[model_name] = dependencies.copy()

        # Update reverse dependencies
        for dep in dependencies:
            self._dependents[dep].add(model_name)

    def get_dependencies(self, model_name: str) -> Set[str]:
        """Get dependencies of a model.

        Args:
            model_name: Name of the model

        Returns:
            Set of model names this model depends on
        """
        return self._dependencies.get(model_name, set()).copy()

    def get_dependents(self, model_name: str) -> Set[str]:
        """Get models that depend on this model.

        Args:
            model_name: Name of the model

        Returns:
            Set of model names that depend on this model
        """
        return self._dependents.get(model_name, set()).copy()

    def get_all_models(self) -> Set[str]:
        """Get all models in the graph.

        Returns:
            Set of all model names
        """
        return self._models.copy()

    def detect_cycles(self) -> List[List[str]]:
        """Detect cycles in the dependency graph.

        Returns:
            List of cycles, where each cycle is a list of model names
        """
        cycles = []
        visited = set()
        rec_stack = set()

        def dfs(node: str, path: List[str]) -> None:
            """Depth-first search to detect cycles."""
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return

            if node in visited:
                return

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            # Visit all dependencies
            for dep in self._dependencies.get(node, set()):
                if dep in self._models:  # Only check internal dependencies
                    dfs(dep, path)

            rec_stack.remove(node)
            path.pop()

        for model in self._models:
            if model not in visited:
                dfs(model, [])

        return cycles

    def topological_sort(self) -> List[str]:
        """Perform topological sort to get execution order.

        Returns:
            List of model names in execution order (dependencies first)

        Raises:
            DependencyError: If cycles are detected
        """
        cycles = self.detect_cycles()
        if cycles:
            cycle_str = ' -> '.join(cycles[0])
            raise DependencyError(f'Circular dependency detected: {cycle_str}')

        # Kahn's algorithm for topological sort
        in_degree = defaultdict(int)
        for model in self._models:
            in_degree[model] = 0

        # Count incoming edges (only for internal dependencies)
        for model in self._models:
            for dep in self._dependencies.get(model, set()):
                if dep in self._models:  # Only internal dependencies
                    in_degree[model] += 1

        # Queue of nodes with no incoming edges
        queue = deque([model for model in self._models if in_degree[model] == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Remove edges from this node
            for dependent in self._dependents.get(node, set()):
                if dependent in self._models:  # Only internal dependencies
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        # Check if all nodes were processed
        if len(result) != len(self._models):
            # This shouldn't happen if cycles are detected, but check anyway
            remaining = self._models - set(result)
            raise DependencyError(f'Could not resolve dependencies for: {remaining}')

        return result

    def get_execution_order(self, select: Optional[Set[str]] = None) -> List[str]:
        """Get execution order for selected models (including dependencies).

        Args:
            select: Optional set of model names to execute. If None, all models.

        Returns:
            List of model names in execution order
        """
        if select is None:
            return self.topological_sort()

        # Build subgraph with selected models and their dependencies
        to_execute = select.copy()
        queue = deque(select)

        while queue:
            model = queue.popleft()
            deps = self._dependencies.get(model, set())
            for dep in deps:
                if dep in self._models:  # Only internal dependencies
                    if dep not in to_execute:
                        to_execute.add(dep)
                        queue.append(dep)

        # Create subgraph
        subgraph = DependencyGraph()
        for model in to_execute:
            deps = self._dependencies.get(model, set())
            internal_deps = {d for d in deps if d in self._models}
            subgraph.add_model(model, internal_deps)

        return subgraph.topological_sort()


def build_dependency_graph(compiled_models: Dict[str, CompiledModel]) -> DependencyGraph:
    """Build dependency graph from compiled models.

    Args:
        compiled_models: Dictionary mapping model names to CompiledModel

    Returns:
        DependencyGraph with all dependencies
    """
    graph = DependencyGraph()

    for model_name, compiled in compiled_models.items():
        # Get internal dependencies (models referenced via ref())
        # External dependencies are in compiled.dependencies
        internal_deps = set()

        # Find ref() calls in raw SQL that reference other models
        # We need to check which refs are internal vs external
        for ref_name in compiled.dependencies.keys():
            # If ref_name is in compiled_models, it's an internal dependency
            if ref_name in compiled_models:
                internal_deps.add(ref_name)

        graph.add_model(model_name, internal_deps)

    return graph

