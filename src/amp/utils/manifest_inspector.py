"""Shared utilities for inspecting dataset manifests.

This module provides functions to parse and display dataset schemas
from manifest files in a human-readable format.
"""

from typing import Any, Dict, List, Union


def describe_manifest(manifest: dict) -> Dict[str, List[Dict[str, Union[str, bool]]]]:
    """Extract structured schema information from a manifest.

    Args:
        manifest: Dataset manifest dictionary

    Returns:
        dict: Mapping of table names to column information. Each column is a dict with:
            - name: Column name
            - type: Arrow type (simplified string representation)
            - nullable: Whether the column allows NULL values
    """
    tables = manifest.get('tables', {})

    result = {}
    for table_name, table_def in tables.items():
        schema = table_def.get('schema', {}).get('arrow', {})
        fields = schema.get('fields', [])

        columns = []
        for field in fields:
            col_type = format_arrow_type(field.get('type'))
            columns.append(
                {
                    'name': field.get('name', ''),
                    'type': col_type,
                    'nullable': field.get('nullable', True),
                }
            )

        result[table_name] = columns

    return result


def format_arrow_type(type_def: Any) -> str:
    """Format Arrow type definition into a readable string.

    Args:
        type_def: Arrow type definition (str or dict)

    Returns:
        str: Human-readable type string
    """
    if isinstance(type_def, str):
        return type_def
    elif isinstance(type_def, dict):
        # Handle complex types like Timestamp, FixedSizeBinary, Decimal128
        if 'Timestamp' in type_def:
            unit = type_def['Timestamp'][0] if type_def['Timestamp'] else 'Unknown'
            return f'Timestamp({unit})'
        elif 'FixedSizeBinary' in type_def:
            size = type_def['FixedSizeBinary']
            return f'FixedSizeBinary({size})'
        elif 'Decimal128' in type_def:
            precision, scale = type_def['Decimal128']
            return f'Decimal128({precision},{scale})'
        else:
            # Fallback for unknown complex types
            return str(type_def)
    else:
        return str(type_def)


def print_schema(schema: Dict[str, list[Dict[str, Any]]], header: str = None) -> None:
    """Pretty-print a schema dictionary.

    Args:
        schema: Schema dictionary from describe_manifest()
        header: Optional header text to print before the schema
    """
    if header:
        print(f'\n{header}')

    if not schema:
        print('\n  (No tables found in manifest)')
        return

    # Print each table
    for table_name, columns in schema.items():
        print(f'\n{table_name} ({len(columns)} columns)')
        for col in columns:
            nullable_str = 'NULL    ' if col['nullable'] else 'NOT NULL'
            # Pad column name for alignment
            col_name = col['name'].ljust(20)
            col_type = col['type'].ljust(20)
            print(f'  {col_name} {col_type} {nullable_str}')

    print()  # Empty line at end
