"""
Custom JSON encoder for handling datetime objects in data serialization.

This module provides a custom JSON encoder that can handle datetime objects
and other non-standard JSON types that may be present in our data structures.
"""

import json
from datetime import datetime
from typing import Any


class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles datetime objects.

    Converts datetime objects to ISO format strings for JSON serialization.
    This allows us to preserve temporal information while maintaining
    JSON compatibility.
    """

    def default(self, obj: Any) -> Any:
        """
        Convert datetime objects to ISO format strings.

        Args:
            obj: Object to be serialized.

        Returns:
            ISO format string for datetime objects, or calls parent default
            for other object types.

        Raises:
            TypeError: If object cannot be serialized.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
