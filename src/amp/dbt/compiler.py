"""SQL compilation engine for Amp DBT."""

import re
from pathlib import Path
from typing import Dict, Optional, Set

from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from amp.dbt.exceptions import CompilationError, DependencyError
from amp.dbt.models import CompiledModel, ModelConfig


class Compiler:
    """Query compilation engine with Jinja templating support."""

    def __init__(self, project_root: Path, macros_dir: Optional[Path] = None):
        """Initialize compiler.

        Args:
            project_root: Root directory of the DBT project
            macros_dir: Optional directory containing macro files
        """
        self.project_root = project_root
        self.macros_dir = macros_dir or project_root / 'macros'

        # Set up Jinja environment
        self.env = Environment(
            loader=FileSystemLoader([str(project_root), str(self.macros_dir)]),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add custom functions to Jinja context
        self.env.globals['ref'] = self._ref_function

    def compile(
        self,
        sql: str,
        model_name: str,
        config: ModelConfig,
        available_models: Optional[Set[str]] = None,
    ) -> CompiledModel:
        """Compile a model SQL with Jinja templating.

        Args:
            sql: Raw SQL with Jinja templates
            model_name: Name of the model
            config: Model configuration
            available_models: Optional set of available internal model names

        Returns:
            CompiledModel with compiled SQL

        Raises:
            CompilationError: If compilation fails
        """
        try:
            # Parse config block if present
            template = self.env.from_string(sql)

            # Build context for Jinja rendering
            context = {
                'config': config,
                'model_name': model_name,
            }

            # Render template
            compiled_sql = template.render(**context)

            # Resolve dependencies (now supports both internal and external)
            dependencies = self._resolve_dependencies(compiled_sql, config, available_models)

            # Replace ref() placeholders with actual references
            final_sql = self._replace_refs(compiled_sql, dependencies)

            return CompiledModel(
                name=model_name,
                sql=final_sql,
                config=config,
                dependencies=dependencies,
                raw_sql=sql,
            )

        except Exception as e:
            raise CompilationError(f'Failed to compile model {model_name}: {e}') from e

    def _ref_function(self, name: str, *args, **kwargs) -> str:
        """Jinja function for resolving ref() calls.

        Args:
            name: Reference name (e.g., 'eth' or model name)
            *args: Additional arguments (not used for external datasets)
            **kwargs: Additional keyword arguments (not used)

        Returns:
            SQL fragment for the reference (dataset reference format)
        """
        # This will be resolved later based on config.dependencies
        # For now, return a placeholder that we'll replace
        # The placeholder includes the ref name so we can resolve it later
        return f'__REF__{name}__'

    def _resolve_dependencies(
        self, sql: str, config: ModelConfig, available_models: Optional[Set[str]] = None
    ) -> Dict[str, str]:
        """Resolve ref() calls in compiled SQL.

        Args:
            sql: Compiled SQL with ref() placeholders
            config: Model configuration with dependencies
            available_models: Optional set of available internal model names

        Returns:
            Dictionary mapping ref names to either dataset references (external) or model names (internal)
        """
        dependencies = {}
        available_models = available_models or set()

        # Find all ref() calls in the SQL
        ref_pattern = r'__REF__(\w+)__'
        matches = re.findall(ref_pattern, sql)

        for ref_name in matches:
            if ref_name in available_models:
                # Internal model reference - store as model name
                dependencies[ref_name] = ref_name
            elif ref_name in config.dependencies:
                # External dataset reference
                dataset_ref = config.dependencies[ref_name]
                dependencies[ref_name] = dataset_ref
            else:
                # Unknown reference
                raise DependencyError(
                    f'Unknown reference "{ref_name}". '
                    f'It must be either an internal model or defined in config.dependencies.'
                )

        return dependencies

    def _replace_refs(
        self, sql: str, dependencies: Dict[str, str], model_sql_map: Optional[Dict[str, str]] = None
    ) -> str:
        """Replace ref() placeholders with actual references.

        For external dependencies: replaces with dataset reference
        For internal dependencies: replaces with CTE name (if model_sql_map provided) or model name (first pass)

        Args:
            sql: SQL with ref() placeholders
            dependencies: Dictionary mapping ref names to references (dataset refs or model names)
            model_sql_map: Optional dictionary mapping model names to their compiled SQL (for CTE inlining)

        Returns:
            SQL with ref() calls replaced
        """
        result = sql
        model_sql_map = model_sql_map or {}

        for ref_name, ref_value in dependencies.items():
            placeholder = f'__REF__{ref_name}__'

            if ref_name == ref_value:
                # Internal model reference
                if ref_name in model_sql_map:
                    # Second pass: CTE will be inlined, just replace with model name
                    result = result.replace(placeholder, ref_name)
                else:
                    # First pass: just replace with model name (CTE will be added later)
                    # This allows dependency discovery without requiring CTE SQL
                    result = result.replace(placeholder, ref_name)
            else:
                # External dataset reference
                # Use alias format (e.g., 'arb_firehose') instead of full reference
                # The full reference will be tracked in dependencies for with_dependency()
                result = result.replace(placeholder, ref_name)

        return result

    def compile_with_ctes(
        self,
        sql: str,
        model_name: str,
        config: ModelConfig,
        internal_deps: Dict[str, str],
        available_models: Optional[Set[str]] = None,
    ) -> CompiledModel:
        """Compile a model with CTE inlining for internal dependencies.

        Args:
            sql: Raw SQL with Jinja templates
            model_name: Name of the model
            config: Model configuration
            internal_deps: Dictionary mapping internal model names to their compiled SQL
            available_models: Optional set of available internal model names

        Returns:
            CompiledModel with compiled SQL including CTEs

        Raises:
            CompilationError: If compilation fails
        """
        try:
            template = self.env.from_string(sql)
            context = {'config': config, 'model_name': model_name}
            compiled_sql = template.render(**context)

            # Resolve dependencies
            dependencies = self._resolve_dependencies(compiled_sql, config, available_models)

            # Replace ref() placeholders first (before extracting CTEs)
            final_sql = self._replace_refs(compiled_sql, dependencies, internal_deps)

            # Build CTE section for internal dependencies
            # Flatten all CTEs from dependencies into a single WITH clause
            # Also check if final_sql already has a WITH clause and merge it
            all_ctes = {}  # Map of CTE name -> SQL
            
            if internal_deps:
                for dep_name, dep_sql in internal_deps.items():
                    # Extract CTEs and the final SELECT from dependency SQL
                    ctes_from_dep, select_part = self._extract_ctes_and_select(dep_sql)
                    # Add any CTEs from this dependency
                    all_ctes.update(ctes_from_dep)
                    # Add this dependency as a CTE using just its SELECT part
                    all_ctes[dep_name] = select_part
            
            # Check if final_sql already has a WITH clause (from the model's own SQL)
            final_sql_upper = final_sql.upper().strip()
            if final_sql_upper.startswith('WITH '):
                # Extract CTEs from the model's own SQL
                model_ctes, model_select = self._extract_ctes_and_select(final_sql)
                # Merge with dependency CTEs
                all_ctes.update(model_ctes)
                # Use the model's SELECT as the final SQL
                final_sql = model_select
            
            # Build final CTE section if we have any CTEs
            if all_ctes:
                cte_parts = []
                for cte_name, cte_sql in all_ctes.items():
                    cte_parts.append(f'{cte_name} AS (\n{cte_sql}\n)')
                cte_section = 'WITH ' + ',\n'.join(cte_parts) + '\n'
                final_sql = cte_section + final_sql

            return CompiledModel(
                name=model_name,
                sql=final_sql,
                config=config,
                dependencies=dependencies,
                raw_sql=sql,
            )

        except Exception as e:
            raise CompilationError(f'Failed to compile model {model_name} with CTEs: {e}') from e

    def _extract_ctes_and_select(self, sql: str) -> tuple[Dict[str, str], str]:
        """Extract CTEs and final SELECT from SQL that may contain WITH clauses.
        
        Returns:
            Tuple of (dict of CTE name -> SQL, final SELECT statement)
        """
        import re
        
        sql_upper = sql.upper().strip()
        
        # If no WITH clause, return empty CTEs and the SQL as SELECT
        if 'WITH ' not in sql_upper:
            return {}, sql.strip()
        
        ctes = {}
        
        # Use regex to find all CTE definitions: name AS (content)
        # Pattern: word AS ( ... ) optionally followed by comma
        # We need to handle nested parentheses correctly
        with_match = re.search(r'^WITH\s+', sql_upper)
        if not with_match:
            return {}, sql.strip()
        
        after_with_start = with_match.end()
        after_with = sql[after_with_start:]
        
        # Find where WITH clause ends (the SELECT that's not inside parentheses)
        # Count parentheses to find the SELECT after all CTEs
        paren_count = 0
        in_string = False
        string_char = None
        i = 0
        
        while i < len(after_with):
            char = after_with[i]
            prev_char = after_with[i-1] if i > 0 else ''
            
            # Track string literals
            if char in ("'", '"') and prev_char != '\\':
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                i += 1
                continue
            
            if in_string:
                i += 1
                continue
            
            # Track parentheses
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
                # When we're back to 0 or negative, check if SELECT follows
                if paren_count <= 0:
                    remaining = after_with[i+1:].strip()
                    if remaining.upper().startswith('SELECT'):
                        # Found the final SELECT - extract CTEs from before this point
                        with_clause_part = after_with[:i+1]
                        ctes = self._parse_ctes_from_with('WITH ' + with_clause_part)
                        select_part = remaining
                        return ctes, select_part
            
            i += 1
        
        # Fallback: use regex-based extraction
        return self._simple_extract_ctes(sql)
    
    def _simple_extract_ctes(self, sql: str) -> tuple[Dict[str, str], str]:
        """Simple fallback to extract CTEs using regex."""
        import re
        
        # Find WHERE WITH ends and SELECT begins
        # Pattern: WITH ... ) SELECT (the SELECT after closing paren of last CTE)
        # We want to find the SELECT that comes after the WITH clause ends
        match = re.search(r'\)\s+(SELECT\s+)', sql, re.IGNORECASE | re.DOTALL)
        if match:
            # Find the position of SELECT (group 1 start)
            select_start = match.start(1)
            return {}, sql[select_start:].strip()
        
        # Alternative: look for SELECT after WITH
        match2 = re.search(r'WITH\s+.*?(SELECT\s+)', sql, re.IGNORECASE | re.DOTALL)
        if match2:
            select_start = match2.start(1)
            return {}, sql[select_start:].strip()
        
        return {}, sql.strip()
    
    def _parse_ctes_from_with(self, with_clause: str) -> Dict[str, str]:
        """Parse CTE definitions from a WITH clause.
        
        Args:
            with_clause: SQL starting with WITH and ending with closing paren
            
        Returns:
            Dictionary mapping CTE names to their SQL
        """
        import re
        ctes = {}
        
        # Remove the leading "WITH " 
        content = with_clause[5:].strip() if with_clause.upper().startswith('WITH ') else with_clause
        
        # Use regex to find CTE patterns: name AS (content)
        # Handle nested parentheses by matching balanced parens
        # Pattern: (\w+) AS \( ... \)
        i = 0
        while i < len(content):
            # Find next " AS ("
            as_match = re.search(r'\s+AS\s+\(', content[i:], re.IGNORECASE)
            if not as_match:
                break
            
            as_pos = i + as_match.start()
            
            # Find CTE name before " AS ("
            name_end = as_pos
            name_start = name_end
            while name_start > 0 and (content[name_start-1].isalnum() or content[name_start-1] == '_'):
                name_start -= 1
            cte_name = content[name_start:name_end].strip()
            
            if not cte_name:
                i = as_pos + as_match.end()
                continue
            
            # Find matching closing paren
            # as_match positions are relative to content[i:], so add i to get absolute position
            paren_start = i + as_match.end() - 1  # Position of (
            paren_count = 1
            in_string = False
            string_char = None
            j = paren_start + 1
            
            while j < len(content) and paren_count > 0:
                char = content[j]
                prev_char = content[j-1] if j > 0 else ''
                
                # Track string literals
                if char in ("'", '"') and prev_char != '\\':
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                    j += 1
                    continue
                
                if in_string:
                    j += 1
                    continue
                
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                
                j += 1
            
            if paren_count == 0:
                # Found matching closing paren
                cte_sql = content[paren_start + 1:j - 1].strip()
                ctes[cte_name] = cte_sql
                i = j  # Continue after this CTE
            else:
                break
        
        return ctes
    
    def _extract_select_from_sql(self, sql: str) -> str:
        """Extract the final SELECT statement from SQL that may contain CTEs.
        
        If the SQL has WITH clauses, returns just the SELECT part after all WITHs.
        Otherwise returns the SQL as-is.
        
        Args:
            sql: SQL that may contain WITH clauses
            
        Returns:
            SQL with only the final SELECT statement (CTEs removed)
        """
        sql_upper = sql.upper().strip()
        
        # If no WITH clause, return as-is
        if 'WITH ' not in sql_upper:
            return sql.strip()
        
        # Find the final SELECT that comes after all CTE definitions
        # Pattern: WITH cte1 AS (...), cte2 AS (...) SELECT ...
        # We need to find the SELECT that's not inside a CTE definition
        
        # Simple approach: find the last closing paren that's followed by SELECT
        # This indicates the end of the CTE list
        paren_count = 0
        in_string = False
        string_char = None
        
        for i in range(len(sql)):
            char = sql[i]
            prev_char = sql[i-1] if i > 0 else ''
            
            # Track string literals
            if char in ("'", '"') and prev_char != '\\':
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                continue
            
            if in_string:
                continue
            
            # Track parentheses
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
                # Check if this closes the last CTE (paren_count back to 0 or negative)
                if paren_count <= 0:
                    # Look ahead for SELECT (skip whitespace and newlines)
                    remaining = sql[i+1:].strip()
                    if remaining.upper().startswith('SELECT'):
                        return remaining
                    # Also check if there's a comma then more CTEs, then SELECT
                    if remaining.startswith(',') or remaining.startswith('\n'):
                        # Continue searching
                        continue
        
        # Fallback: if parsing failed, try regex to find SELECT after WITH
        import re
        # Match: WITH ... SELECT (capture the SELECT part)
        match = re.search(r'WITH\s+.*?\)\s+SELECT\s+', sql, re.IGNORECASE | re.DOTALL)
        if match:
            return sql[match.end() - len('SELECT '):].strip()
        
        # Last resort: return as-is (better than failing)
        return sql.strip()

