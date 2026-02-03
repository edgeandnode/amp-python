"""Async schema client for Admin API.

This module provides the AsyncSchemaClient class for querying output schemas
of SQL queries without executing them using async/await.
"""

from typing import TYPE_CHECKING

from . import models

if TYPE_CHECKING:
    from .async_client import AsyncAdminClient


class AsyncSchemaClient:
    """Async client for schema operations.

    Provides async methods for validating SQL queries and determining output schemas
    using DataFusion's query planner.

    Args:
        admin_client: Parent AsyncAdminClient instance

    Example:
        >>> async with AsyncAdminClient('http://localhost:8080') as client:
        ...     schema = await client.schema.get_output_schema('SELECT * FROM eth.blocks', True)
    """

    def __init__(self, admin_client: 'AsyncAdminClient'):
        """Initialize async schema client.

        Args:
            admin_client: Parent AsyncAdminClient instance
        """
        self._admin = admin_client

    async def get_output_schema(self, sql_query: str, is_sql_dataset: bool = True) -> models.OutputSchemaResponse:
        """Get output schema for a SQL query.

        Validates the query and returns the Arrow schema that would be produced,
        without actually executing the query.

        Args:
            sql_query: SQL query to analyze
            is_sql_dataset: Whether this is for a SQL dataset (default: True)

        Returns:
            OutputSchemaResponse with Arrow schema

        Raises:
            GetOutputSchemaError: If schema analysis fails
            DependencyValidationError: If query references invalid dependencies

        Example:
            >>> async with AsyncAdminClient('http://localhost:8080') as client:
            ...     schema_resp = await client.schema.get_output_schema(
            ...         'SELECT block_num, hash FROM eth.blocks WHERE block_num > 1000000',
            ...         is_sql_dataset=True
            ...     )
            ...     print(schema_resp.schema)
        """
        request_data = models.OutputSchemaRequest(sql_query=sql_query, is_sql_dataset=is_sql_dataset)

        response = await self._admin._request('POST', '/schema', json=request_data.model_dump(mode='json'))
        return models.OutputSchemaResponse.model_validate(response.json())
