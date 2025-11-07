"""Deployment context for chainable dataset deployment operations.

This module provides the DeploymentContext class which enables
method chaining for dataset deployment workflows.
"""

from typing import Optional

from . import models


class DeploymentContext:
    """Chainable context for deploying registered datasets.

    Returned by QueryBuilder.register_as() to enable fluent deployment syntax.

    Args:
        client: Parent Client instance (with admin client)
        namespace: Dataset namespace
        name: Dataset name
        version: Dataset version

    Example:
        >>> context = DeploymentContext(client, '_', 'my_dataset', '1.0.0')
        >>> job = context.deploy(parallelism=4, wait=True)
    """

    def __init__(self, client, namespace: str, name: str, version: str):
        """Initialize deployment context.

        Args:
            client: Parent Client instance (with admin client)
            namespace: Dataset namespace
            name: Dataset name
            version: Dataset version
        """
        self._client = client
        self._namespace = namespace
        self._name = name
        self._version = version

    def deploy(
        self, end_block: Optional[str] = None, parallelism: Optional[int] = None, wait: bool = False
    ) -> models.JobInfo:
        """Deploy the registered dataset.

        Triggers data extraction and optionally waits for completion.

        Args:
            end_block: Optional end block ('latest', '-100', '1000000', or None)
            parallelism: Optional number of parallel workers
            wait: If True, blocks until deployment completes (default: False)

        Returns:
            JobInfo with deployment status

        Raises:
            DatasetNotFoundError: If dataset/version not found
            SchedulerError: If deployment fails

        Example:
            >>> # Deploy and return immediately
            >>> job = context.deploy(parallelism=4)
            >>> print(f'Job ID: {job.id}')
            >>>
            >>> # Deploy and wait for completion
            >>> job = context.deploy(parallelism=4, wait=True)
            >>> print(f'Final status: {job.status}')
        """
        # Trigger deployment
        deploy_response = self._client.datasets.deploy(
            self._namespace, self._name, self._version, end_block=end_block, parallelism=parallelism
        )

        # Wait for completion if requested
        if wait:
            return self._client.jobs.wait_for_completion(deploy_response.job_id)
        else:
            return self._client.jobs.get(deploy_response.job_id)

    @property
    def reference(self) -> str:
        """Get dataset reference string.

        Returns:
            Dataset reference in format '{namespace}/{name}@{version}'

        Example:
            >>> context.reference
            '_/my_dataset@1.0.0'
        """
        return f'{self._namespace}/{self._name}@{self._version}'
