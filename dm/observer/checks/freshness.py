"""
Freshness Check

Detects stale tables by comparing the most recent timestamp column
(updated_at or created_at) against an expected update interval.
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def check_freshness(
    modern_conn: Any,
    table: str,
    expected_interval_hours: int = 24,
) -> dict:
    """Check whether a table has been updated within the expected interval.

    Attempts to find the most recent value of ``updated_at``; falls back to
    ``created_at`` if ``updated_at`` does not exist.

    Args:
        modern_conn: A BaseConnector instance connected to the modern DB.
        table: The table name to check.
        expected_interval_hours: Maximum acceptable hours since last update.

    Returns:
        Dict with keys:
            stale: bool — True if the table exceeds the expected interval.
            last_update: str | None — ISO timestamp of the most recent row.
            hours_since_update: float | None — Hours elapsed since last update.
    """
    # Determine which timestamp column to use
    timestamp_column = None
    try:
        schema = modern_conn.get_table_schema(table)
        column_names = [col["column_name"] for col in schema]

        if "updated_at" in column_names:
            timestamp_column = "updated_at"
        elif "created_at" in column_names:
            timestamp_column = "created_at"
    except Exception as e:
        logger.warning(f"Could not inspect schema for '{table}': {e}")

    if timestamp_column is None:
        logger.debug(
            f"No timestamp column found in '{table}'; skipping freshness check"
        )
        return {
            "stale": False,
            "last_update": None,
            "hours_since_update": None,
            "note": f"No updated_at or created_at column in '{table}'",
        }

    # Query the most recent timestamp
    try:
        last_update = modern_conn.execute_scalar(
            f"SELECT MAX({timestamp_column}) FROM {table}"
        )
    except Exception as e:
        logger.error(
            f"Could not query MAX({timestamp_column}) from '{table}': {e}"
        )
        return {
            "stale": False,
            "last_update": None,
            "hours_since_update": None,
            "error": str(e),
        }

    if last_update is None:
        return {
            "stale": True,
            "last_update": None,
            "hours_since_update": None,
            "note": f"Table '{table}' has no rows or all timestamps are NULL",
        }

    # Normalize to a timezone-aware datetime
    if isinstance(last_update, str):
        # Try common ISO formats
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                last_update = datetime.strptime(last_update, fmt)
                break
            except ValueError:
                continue

    if isinstance(last_update, datetime):
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        hours_since = (now - last_update).total_seconds() / 3600.0
    else:
        # Cannot determine elapsed time from unexpected type
        logger.warning(
            f"Unexpected timestamp type for '{table}': {type(last_update)}"
        )
        return {
            "stale": False,
            "last_update": str(last_update),
            "hours_since_update": None,
            "note": "Could not compute elapsed time",
        }

    stale = hours_since > expected_interval_hours

    if stale:
        logger.warning(
            f"Table '{table}' is stale: last update {hours_since:.1f}h ago "
            f"(threshold {expected_interval_hours}h)"
        )
    else:
        logger.debug(
            f"Table '{table}' is fresh: last update {hours_since:.1f}h ago"
        )

    return {
        "stale": stale,
        "last_update": last_update.isoformat() if isinstance(last_update, datetime) else str(last_update),
        "hours_since_update": round(hours_since, 2),
    }
