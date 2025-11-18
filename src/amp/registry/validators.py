"""Custom validators for Registry API models."""

from datetime import datetime, timezone
from typing import Any

from pydantic import field_validator


def parse_utc_datetime(value: Any) -> datetime:
    """Parse datetime strings from Registry API.

    The API returns timestamps in format: '2025-11-18 01:28:40.434357 UTC'
    Pydantic expects ISO 8601 format, so we need a custom parser.

    Args:
        value: DateTime value from API (string or datetime)

    Returns:
        datetime: Parsed timezone-aware datetime

    Raises:
        ValueError: If the format is not recognized
    """
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        # Try parsing the Registry API format: "YYYY-MM-DD HH:MM:SS.ffffff UTC"
        if value.endswith(' UTC'):
            # Remove the ' UTC' suffix and parse
            dt_str = value[:-4].strip()
            try:
                dt = datetime.fromisoformat(dt_str)
                # Ensure it's timezone-aware (UTC)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

        # Try standard ISO 8601 parsing
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    raise ValueError(f'Unable to parse datetime: {value}')


def create_datetime_validator():
    """Create a field validator for datetime fields.

    Returns:
        A Pydantic field_validator for parsing Registry API datetimes
    """
    return field_validator('created_at', 'updated_at', mode='before')(lambda v: parse_utc_datetime(v))
