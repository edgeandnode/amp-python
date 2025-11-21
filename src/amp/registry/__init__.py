"""Amp Registry API client.

The Registry provides dataset discovery, search, and publishing capabilities.

Example:
    >>> from amp.registry import RegistryClient
    >>>
    >>> # Read-only operations
    >>> client = RegistryClient()
    >>> results = client.datasets.search('ethereum blocks')
    >>> for dataset in results.datasets:
    ...     print(f"{dataset.namespace}/{dataset.name} - Score: {dataset.score}")
    >>>
    >>> # Get a specific dataset
    >>> dataset = client.datasets.get('edgeandnode', 'ethereum-mainnet')
    >>> manifest = client.datasets.get_manifest('edgeandnode', 'ethereum-mainnet', 'latest')
    >>>
    >>> # Authenticated operations
    >>> client = RegistryClient(auth_token='your-token')
    >>> client.datasets.publish(...)
"""

from . import errors, models
from .client import RegistryClient
from .datasets import RegistryDatasetsClient

__all__ = ['RegistryClient', 'RegistryDatasetsClient', 'errors', 'models']
