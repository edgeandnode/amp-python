# tests/test_abi.py
import json
import tempfile
from pathlib import Path

from amp.utils.abi import Abi, Event


class TestEvent:
    """Tests for the Event class."""

    def test_event_signature_with_indexed_parameters(self):
        """Test event signature generation with mixed indexed/non-indexed parameters."""
        event_data = {
            'name': 'Transfer',
            'type': 'event',
            'inputs': [
                {'name': 'from', 'type': 'address', 'indexed': True},
                {'name': 'to', 'type': 'address', 'indexed': True},
                {'name': 'value', 'type': 'uint256', 'indexed': False},
            ],
        }
        event = Event(event_data)

        assert event.name == 'Transfer'
        assert event.signature() == 'Transfer(address indexed from,address indexed to,uint256 value)'
        assert event.names == ['from', 'to', 'value']

    def test_event_no_parameters(self):
        """Test event with no parameters (edge case)."""
        event_data = {
            'name': 'Paused',
            'type': 'event',
            'inputs': [],
        }
        event = Event(event_data)

        assert event.signature() == 'Paused()'
        assert event.names == []
        assert event.inputs == []


class TestAbi:
    """Tests for the Abi class."""

    def test_abi_loads_events_from_file(self):
        """Test loading ABI with multiple events from a JSON file."""
        abi_content = [
            {
                'name': 'Transfer',
                'type': 'event',
                'inputs': [
                    {'name': 'from', 'type': 'address', 'indexed': True},
                    {'name': 'to', 'type': 'address', 'indexed': True},
                    {'name': 'value', 'type': 'uint256', 'indexed': False},
                ],
            },
            {
                'name': 'Approval',
                'type': 'event',
                'inputs': [
                    {'name': 'owner', 'type': 'address', 'indexed': True},
                    {'name': 'spender', 'type': 'address', 'indexed': True},
                    {'name': 'value', 'type': 'uint256', 'indexed': False},
                ],
            },
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(abi_content, f)
            temp_path = f.name

        try:
            abi = Abi(temp_path)

            assert len(abi.events) == 2
            assert 'Transfer' in abi.events
            assert 'Approval' in abi.events
            assert isinstance(abi.events['Transfer'], Event)
            assert abi.events['Transfer'].signature() == 'Transfer(address indexed from,address indexed to,uint256 value)'
        finally:
            Path(temp_path).unlink()

    def test_abi_filters_only_events(self):
        """Test that ABI only loads event types, ignoring functions and constructors."""
        abi_content = [
            {
                'name': 'Transfer',
                'type': 'event',
                'inputs': [
                    {'name': 'from', 'type': 'address', 'indexed': True},
                    {'name': 'to', 'type': 'address', 'indexed': True},
                ],
            },
            {
                'name': 'transfer',
                'type': 'function',
                'inputs': [
                    {'name': 'to', 'type': 'address'},
                    {'name': 'amount', 'type': 'uint256'},
                ],
                'outputs': [{'name': '', 'type': 'bool'}],
            },
            {
                'type': 'constructor',
                'inputs': [{'name': 'initialSupply', 'type': 'uint256'}],
            },
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(abi_content, f)
            temp_path = f.name

        try:
            abi = Abi(temp_path)

            # Should only have the event, not the function or constructor
            assert len(abi.events) == 1
            assert 'Transfer' in abi.events
        finally:
            Path(temp_path).unlink()

    def test_abi_empty_file(self):
        """Test loading an empty ABI file (edge case)."""
        abi_content = []

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(abi_content, f)
            temp_path = f.name

        try:
            abi = Abi(temp_path)

            assert len(abi.events) == 0
            assert abi.events == {}
        finally:
            Path(temp_path).unlink()
