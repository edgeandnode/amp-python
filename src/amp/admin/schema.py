"""Schema client for Admin API.

This module provides the SchemaClient class for querying output schemas
of SQL queries without executing them.
"""

from typing import TYPE_CHECKING, Optional

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
        >>> schema = client.schema.get_output_schema('SELECT * FROM eth.blocks', dependencies={'eth': '_/eth_firehose@0.0.0'})
    """

    def __init__(self, admin_client: 'AdminClient'):
        """Initialize schema client.

        Args:
            admin_client: Parent AdminClient instance
        """
        self._admin = admin_client

    def get_output_schema(
        self, sql_query: str, dependencies: Optional[dict[str, str]] = None
    ) -> models.TableSchemaWithNetworks:
        """Get output schema for a SQL query.

        Validates the query and returns the Arrow schema that would be produced,
        without actually executing the query.

        Args:
            sql_query: SQL query to analyze
            dependencies: Optional map of alias -> dataset reference

        Returns:
            TableSchemaWithNetworks with Arrow schema

        Raises:
            GetOutputSchemaError: If schema analysis fails
            DependencyValidationError: If query references invalid dependencies

        Example:
            >>> schema_resp = client.schema.get_output_schema(
            ...     'SELECT block_num, hash FROM eth.blocks WHERE block_num > 1000000',
            ...     dependencies={'eth': '_/eth_firehose@0.0.0'}
            ... )
            >>> print(schema_resp.schema)
        """
        # Wrap query in a temporary table for validation
        temp_table_name = 'query_analysis'

        request_data = models.SchemaRequest(
            tables={temp_table_name: sql_query},
            dependencies=dependencies or {},
            functions={},
        )

        response = self._admin._request('POST', '/schema', json=request_data.model_dump(mode='json', exclude_none=True))

        schema_response = models.SchemaResponse.model_validate(response.json())

        # Extract the schema for our temporary table
        if temp_table_name not in schema_response.schemas:
            # This should theoretically not happen if the server returns 200 OK
            raise KeyError(f"Server did not return schema for table '{temp_table_name}'")

        return schema_response.schemas[temp_table_name]
