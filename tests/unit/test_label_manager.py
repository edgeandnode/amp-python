"""Tests for LabelManager functionality"""

import tempfile
from pathlib import Path

import pytest

from amp.config.label_manager import LabelManager


class TestLabelManager:
    """Test LabelManager class"""

    def test_add_and_get_label(self):
        """Test adding and retrieving a label dataset"""
        # Create a temporary CSV file with valid 40-char Ethereum addresses
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('address,symbol,name\n')
            f.write('0x1234567890123456789012345678901234567890,ETH,Ethereum\n')
            f.write('0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,BTC,Bitcoin\n')
            csv_path = f.name

        try:
            manager = LabelManager()

            # Add label
            manager.add_label('tokens', csv_path)

            # Get label
            label_table = manager.get_label('tokens')

            assert label_table is not None
            assert label_table.num_rows == 2
            assert len(label_table.schema) == 3
            assert 'address' in label_table.schema.names
            assert 'symbol' in label_table.schema.names
            assert 'name' in label_table.schema.names

        finally:
            Path(csv_path).unlink()

    def test_get_nonexistent_label(self):
        """Test getting a label that doesn't exist"""
        manager = LabelManager()
        label_table = manager.get_label('nonexistent')
        assert label_table is None

    def test_list_labels(self):
        """Test listing all configured labels"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,value\n')
            f.write('1,a\n')
            csv_path = f.name

        try:
            manager = LabelManager()
            manager.add_label('test1', csv_path)
            manager.add_label('test2', csv_path)

            labels = manager.list_labels()
            assert 'test1' in labels
            assert 'test2' in labels
            assert len(labels) == 2

        finally:
            Path(csv_path).unlink()

    def test_replace_label(self):
        """Test replacing an existing label"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,value\n')
            f.write('1,a\n')
            f.write('2,b\n')
            csv_path1 = f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,value\n')
            f.write('1,x\n')
            csv_path2 = f.name

        try:
            manager = LabelManager()
            manager.add_label('test', csv_path1)

            # First version
            label1 = manager.get_label('test')
            assert label1.num_rows == 2

            # Replace with new version
            manager.add_label('test', csv_path2)
            label2 = manager.get_label('test')
            assert label2.num_rows == 1

        finally:
            Path(csv_path1).unlink()
            Path(csv_path2).unlink()

    def test_remove_label(self):
        """Test removing a label"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,value\n')
            f.write('1,a\n')
            csv_path = f.name

        try:
            manager = LabelManager()
            manager.add_label('test', csv_path)

            # Verify it exists
            assert manager.get_label('test') is not None

            # Remove it
            result = manager.remove_label('test')
            assert result is True

            # Verify it's gone
            assert manager.get_label('test') is None

            # Try to remove again
            result = manager.remove_label('test')
            assert result is False

        finally:
            Path(csv_path).unlink()

    def test_clear_labels(self):
        """Test clearing all labels"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,value\n')
            f.write('1,a\n')
            csv_path = f.name

        try:
            manager = LabelManager()
            manager.add_label('test1', csv_path)
            manager.add_label('test2', csv_path)

            assert len(manager.list_labels()) == 2

            manager.clear()

            assert len(manager.list_labels()) == 0

        finally:
            Path(csv_path).unlink()

    def test_invalid_csv_path(self):
        """Test adding a label with invalid CSV path"""
        manager = LabelManager()

        with pytest.raises(FileNotFoundError):
            manager.add_label('test', '/nonexistent/path.csv')
