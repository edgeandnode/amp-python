"""
Label Manager for CSV-based label datasets.

This module provides functionality to register and manage CSV label datasets
that can be joined with streaming data during loading operations.
"""

import logging
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.csv as csv


class LabelManager:
    """
    Manages CSV label datasets for joining with streaming data.

    Labels are registered by name and loaded as PyArrow Tables for efficient
    joining operations. This allows reuse of label datasets across multiple
    queries and loaders.

    Example:
        >>> manager = LabelManager()
        >>> manager.add_label('token_labels', '/path/to/tokens.csv')
        >>> label_table = manager.get_label('token_labels')
    """

    def __init__(self):
        self._labels: Dict[str, pa.Table] = {}
        self.logger = logging.getLogger(__name__)

    def add_label(self, name: str, csv_path: str, binary_columns: Optional[List[str]] = None) -> None:
        """
        Load and register a CSV label dataset with automatic hex→binary conversion.

        Hex string columns (like Ethereum addresses) are automatically converted to
        binary format for efficient storage and joining. This reduces memory usage
        by ~50% and improves join performance.

        Args:
            name: Unique name for this label dataset
            csv_path: Path to the CSV file
            binary_columns: List of column names containing hex addresses to convert to binary.
                          If None, auto-detects columns with 'address' in the name.

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV cannot be parsed or name already exists
        """
        if name in self._labels:
            self.logger.warning(f"Label '{name}' already exists, replacing with new data")

        try:
            # Load CSV as PyArrow Table (initially as strings)
            temp_table = csv.read_csv(csv_path, read_options=csv.ReadOptions(autogenerate_column_names=False))

            # Force all columns to be strings initially
            column_types = {col_name: pa.string() for col_name in temp_table.column_names}
            convert_opts = csv.ConvertOptions(column_types=column_types)
            label_table = csv.read_csv(csv_path, convert_options=convert_opts)

            # Auto-detect or use specified binary columns
            if binary_columns is None:
                # Auto-detect columns with 'address' in name (case-insensitive)
                binary_columns = [col for col in label_table.column_names if 'address' in col.lower()]

            # Convert hex string columns to binary for efficiency
            converted_columns = []
            for col_name in binary_columns:
                if col_name not in label_table.column_names:
                    self.logger.warning(f"Binary column '{col_name}' not found in CSV, skipping")
                    continue

                hex_col = label_table.column(col_name)

                # Detect hex string format and convert to binary
                # Sample first non-null value to determine format
                sample_value = None
                for v in hex_col.to_pylist()[:100]:  # Check first 100 values
                    if v is not None:
                        sample_value = v
                        break

                if sample_value is None:
                    self.logger.warning(f"Column '{col_name}' has no non-null values, skipping conversion")
                    continue

                # Detect if it's a hex string (with or without 0x prefix)
                if isinstance(sample_value, str) and all(c in '0123456789abcdefABCDEFx' for c in sample_value):
                    # Determine binary length from hex string
                    hex_str = sample_value[2:] if sample_value.startswith('0x') else sample_value
                    binary_length = len(hex_str) // 2

                    # Convert all values to binary (fixed-size to match streaming data)
                    def hex_to_binary(v):
                        if v is None:
                            return None
                        hex_str = v[2:] if v.startswith('0x') else v
                        return bytes.fromhex(hex_str)

                    binary_values = pa.array(
                        [hex_to_binary(v) for v in hex_col.to_pylist()],
                        type=pa.binary(
                            binary_length
                        ),  # Fixed-size binary to match server data (e.g., 20 bytes for addresses)
                    )

                    # Replace the column
                    label_table = label_table.set_column(
                        label_table.schema.get_field_index(col_name), col_name, binary_values
                    )
                    converted_columns.append(f'{col_name} (hex→fixed_size_binary[{binary_length}])')
                    self.logger.info(f"Converted '{col_name}' from hex string to fixed_size_binary[{binary_length}]")

            self._labels[name] = label_table

            conversion_info = f', converted: {", ".join(converted_columns)}' if converted_columns else ''
            self.logger.info(
                f"Loaded label '{name}' from {csv_path}: "
                f'{label_table.num_rows:,} rows, {len(label_table.schema)} columns '
                f'({", ".join(label_table.schema.names)}){conversion_info}'
            )

        except FileNotFoundError:
            raise FileNotFoundError(f'Label CSV file not found: {csv_path}')
        except Exception as e:
            raise ValueError(f"Failed to load label CSV '{csv_path}': {e}") from e

    def get_label(self, name: str) -> Optional[pa.Table]:
        """
        Get label table by name.

        Args:
            name: Name of the label dataset

        Returns:
            PyArrow Table containing label data, or None if not found
        """
        return self._labels.get(name)

    def list_labels(self) -> List[str]:
        """
        List all registered label names.

        Returns:
            List of label names
        """
        return list(self._labels.keys())

    def remove_label(self, name: str) -> bool:
        """
        Remove a label dataset.

        Args:
            name: Name of the label to remove

        Returns:
            True if label was removed, False if it didn't exist
        """
        if name in self._labels:
            del self._labels[name]
            self.logger.info(f"Removed label '{name}'")
            return True
        return False

    def clear(self) -> None:
        """Remove all label datasets."""
        count = len(self._labels)
        self._labels.clear()
        self.logger.info(f'Cleared {count} label dataset(s)')
