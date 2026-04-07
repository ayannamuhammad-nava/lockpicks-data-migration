"""
Baseline Snapshot Management

Captures, saves, and loads baseline snapshots of the modern database state.
Used by the PipelineObserver to detect drift and volume anomalies.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class BaselineManager:
    """Manages baseline snapshots for pipeline monitoring."""

    def __init__(self, baseline_path: str):
        """Initialize with the path where baseline.json will be stored.

        Args:
            baseline_path: File path for the baseline JSON file.
        """
        self.baseline_path = Path(baseline_path)

    def capture(self, modern_conn: Any, tables: List[str]) -> dict:
        """Snapshot the current state of the modern database.

        For each table, captures: row count, schema (column names and types),
        null percentages per column, and distinct counts per column.

        Args:
            modern_conn: A BaseConnector instance connected to the modern DB.
            tables: List of table names to snapshot.

        Returns:
            Baseline dict with per-table metrics and a capture timestamp.
        """
        baseline: Dict[str, Any] = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "tables": {},
        }

        for table in tables:
            logger.info(f"Capturing baseline for table: {table}")
            table_snapshot: Dict[str, Any] = {}

            # Row count
            try:
                table_snapshot["row_count"] = modern_conn.get_row_count(table)
            except Exception as e:
                logger.warning(f"Could not get row count for {table}: {e}")
                table_snapshot["row_count"] = None

            # Schema: column names and types
            try:
                schema_info = modern_conn.get_table_schema(table)
                table_snapshot["schema"] = [
                    {
                        "column_name": col["column_name"],
                        "data_type": col["data_type"],
                    }
                    for col in schema_info
                ]
            except Exception as e:
                logger.warning(f"Could not get schema for {table}: {e}")
                table_snapshot["schema"] = []

            # Null percentages and distinct counts per column
            null_percentages: Dict[str, float] = {}
            distinct_counts: Dict[str, int] = {}

            for col_info in table_snapshot.get("schema", []):
                col_name = col_info["column_name"]

                try:
                    null_percentages[col_name] = modern_conn.get_null_percentage(
                        table, col_name
                    )
                except Exception as e:
                    logger.debug(f"Could not get null % for {table}.{col_name}: {e}")
                    null_percentages[col_name] = -1.0

                try:
                    # Distinct count via execute_scalar
                    distinct_count = modern_conn.execute_scalar(
                        f"SELECT COUNT(DISTINCT {col_name}) FROM {table}"
                    )
                    distinct_counts[col_name] = (
                        int(distinct_count) if distinct_count is not None else 0
                    )
                except Exception as e:
                    logger.debug(
                        f"Could not get distinct count for {table}.{col_name}: {e}"
                    )
                    distinct_counts[col_name] = -1

            table_snapshot["null_percentages"] = null_percentages
            table_snapshot["distinct_counts"] = distinct_counts

            baseline["tables"][table] = table_snapshot

        logger.info(f"Baseline captured for {len(tables)} table(s)")
        return baseline

    def save(self, baseline: dict) -> None:
        """Write baseline to baseline.json.

        Args:
            baseline: The baseline dict to persist.
        """
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.baseline_path, "w") as f:
            json.dump(baseline, f, indent=2, default=str)
        logger.info(f"Baseline saved to {self.baseline_path}")

    def load(self) -> dict:
        """Read baseline from baseline.json.

        Returns:
            The stored baseline dict.

        Raises:
            FileNotFoundError: If the baseline file does not exist.
        """
        if not self.exists():
            raise FileNotFoundError(
                f"No baseline found at {self.baseline_path}. "
                f"Run 'set_baseline' first."
            )
        with open(self.baseline_path, "r") as f:
            baseline = json.load(f)
        logger.info(f"Baseline loaded from {self.baseline_path}")
        return baseline

    def exists(self) -> bool:
        """Check whether a baseline file exists.

        Returns:
            True if the baseline file exists on disk.
        """
        return self.baseline_path.is_file()
