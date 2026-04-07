"""
Volume Anomaly Check

Compares the current row count against a baseline snapshot to detect
unexpected growth or shrinkage in table size.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def check_volume_anomaly(
    modern_conn: Any,
    table: str,
    baseline: dict,
    threshold: float = 0.3,
) -> dict:
    """Compare current row count against the baseline for a given table.

    Args:
        modern_conn: A BaseConnector instance connected to the modern DB.
        table: The table name to check.
        baseline: The full baseline dict (with 'tables' key).
        threshold: Maximum acceptable deviation as a fraction (default 0.3 = 30%).

    Returns:
        Dict with keys:
            anomaly: bool — True if deviation exceeds threshold.
            current_count: int — Current row count.
            baseline_count: int — Baseline row count.
            deviation_pct: float — Percentage deviation from baseline.
    """
    table_baseline = baseline.get("tables", {}).get(table)
    if table_baseline is None:
        return {
            "anomaly": False,
            "current_count": None,
            "baseline_count": None,
            "deviation_pct": 0.0,
            "note": f"No baseline entry for table '{table}'",
        }

    baseline_count = table_baseline.get("row_count")
    if baseline_count is None:
        return {
            "anomaly": False,
            "current_count": None,
            "baseline_count": None,
            "deviation_pct": 0.0,
            "note": f"Baseline row count not available for '{table}'",
        }

    try:
        current_count = modern_conn.get_row_count(table)
    except Exception as e:
        logger.error(f"Could not get current row count for {table}: {e}")
        return {
            "anomaly": False,
            "current_count": None,
            "baseline_count": baseline_count,
            "deviation_pct": 0.0,
            "error": str(e),
        }

    # Calculate deviation percentage
    if baseline_count == 0:
        deviation_pct = 100.0 if current_count > 0 else 0.0
    else:
        deviation_pct = abs(current_count - baseline_count) / baseline_count * 100.0

    anomaly = (deviation_pct / 100.0) > threshold

    if anomaly:
        logger.warning(
            f"Volume anomaly in '{table}': {baseline_count} -> {current_count} "
            f"({deviation_pct:.1f}% deviation, threshold {threshold * 100:.0f}%)"
        )
    else:
        logger.debug(
            f"Volume OK for '{table}': {current_count} rows "
            f"({deviation_pct:.1f}% deviation)"
        )

    return {
        "anomaly": anomaly,
        "current_count": current_count,
        "baseline_count": baseline_count,
        "deviation_pct": round(deviation_pct, 2),
    }
