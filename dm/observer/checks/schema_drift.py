"""
Schema Drift Check

Compares the current table schema against a baseline snapshot to detect
added columns, dropped columns, and data type changes.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def check_schema_drift(modern_conn: Any, table: str, baseline: dict) -> dict:
    """Compare current schema against the baseline for a given table.

    Args:
        modern_conn: A BaseConnector instance connected to the modern DB.
        table: The table name to check.
        baseline: The full baseline dict (with 'tables' key).

    Returns:
        Dict with keys:
            drifted: bool — True if any schema changes were detected.
            changes: list[dict] — Each change has {type, column, detail}.
    """
    changes: List[Dict[str, str]] = []

    table_baseline = baseline.get("tables", {}).get(table)
    if table_baseline is None:
        return {
            "drifted": False,
            "changes": [],
            "note": f"No baseline entry for table '{table}'",
        }

    # Build lookup dicts from baseline schema
    baseline_schema = table_baseline.get("schema", [])
    baseline_columns: Dict[str, str] = {
        col["column_name"]: col["data_type"] for col in baseline_schema
    }

    # Fetch current schema
    try:
        current_schema = modern_conn.get_table_schema(table)
    except Exception as e:
        logger.error(f"Could not fetch current schema for {table}: {e}")
        return {
            "drifted": False,
            "changes": [],
            "error": str(e),
        }

    current_columns: Dict[str, str] = {
        col["column_name"]: col["data_type"] for col in current_schema
    }

    # Detect added columns (in current but not in baseline)
    for col_name in current_columns:
        if col_name not in baseline_columns:
            changes.append({
                "type": "added",
                "column": col_name,
                "detail": f"New column with type '{current_columns[col_name]}'",
            })

    # Detect dropped columns (in baseline but not in current)
    for col_name in baseline_columns:
        if col_name not in current_columns:
            changes.append({
                "type": "dropped",
                "column": col_name,
                "detail": f"Column removed (was type '{baseline_columns[col_name]}')",
            })

    # Detect type changes (column exists in both but type differs)
    for col_name in current_columns:
        if col_name in baseline_columns:
            current_type = current_columns[col_name]
            baseline_type = baseline_columns[col_name]
            if current_type != baseline_type:
                changes.append({
                    "type": "type_changed",
                    "column": col_name,
                    "detail": f"Type changed from '{baseline_type}' to '{current_type}'",
                })

    drifted = len(changes) > 0

    if drifted:
        logger.warning(
            f"Schema drift detected in '{table}': {len(changes)} change(s)"
        )
    else:
        logger.debug(f"No schema drift in '{table}'")

    return {
        "drifted": drifted,
        "changes": changes,
    }
