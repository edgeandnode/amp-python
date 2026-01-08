"""Schema client for Admin API.

This module provides the SchemaClient class for querying output schemas
of SQL queries without executing them.
"""

from typing import TYPE_CHECKING, Any, Optional

from . import models

if TYPE_CHECKING:
    from .client import AdminClient


class SchemaClient:
    """Client for schema operations.

    Provides methods for validating SQL queries and determining output schemas
    using DataFusion's query planner.

    Args:
        admin_client: Parent AdminClient instance

    Example:
        >>> client = AdminClient('http://localhost:8080')
        >>> response = client.schema.get_output_schema(
        ...     tables={'t1': 'SELECT * FROM eth.blocks'},
        ...     dependencies={'eth': '_/eth_firehose@0.0.0'}
        ... )
    """

    def __init__(self, admin_client: 'AdminClient'):
        """Initialize schema client.

        Args:
            admin_client: Parent AdminClient instance
        """
        self._admin = admin_client

    def get_output_schema(
        self,
        tables: Optional[dict[str, str]] = None,
        dependencies: Optional[dict[str, str]] = None,
        functions: Optional[dict[str, Any]] = None,
    ) -> models.SchemaResponse:
        """Get output schema for tables and functions.

        Validates the queries and returns the Arrow schemas that would be produced,
        without actually executing the queries.

        Args:
            tables: Optional map of table_name -> sql_query
            dependencies: Optional map of alias -> dataset reference
            functions: Optional map of function_name -> function_definition

        Returns:
            SchemaResponse containing schemas for all requested tables

        Raises:
            GetOutputSchemaError: If schema analysis fails
            DependencyValidationError: If query references invalid dependencies

        Example:
            >>> response = client.schema.get_output_schema(
            ...     tables={'my_table': 'SELECT block_num FROM eth.blocks'},
            ...     dependencies={'eth': '_/eth_firehose@0.0.0'}
            ... )
            >>> print(response.schemas['my_table'].schema)
        """
        request_data = models.SchemaRequest(
            tables=tables,
            dependencies=dependencies,
            functions=functions,
        )

        response = self._admin._request('POST', '/schema', json=request_data.model_dump(mode='json', exclude_none=True))

        return models.SchemaResponse.model_validate(response.json())
