"""Amp - Flight SQL client with comprehensive data loading capabilities."""

import os

# Suppress PyArrow acero alignment warnings before any imports
# These warnings are informational and cannot be controlled via logging
# Valid values: ignore, warn, reallocate, error
os.environ.setdefault('ACERO_ALIGNMENT_HANDLING', 'ignore')

from amp.client import Client, QueryBuilder
from amp.registry import RegistryClient

__all__ = ['Client', 'QueryBuilder', 'RegistryClient']
