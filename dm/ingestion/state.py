"""
DM Migration State Tracker — L-Ingestor state management.

Tracks per-table migration status in a YAML file so that runs can be
resumed after failure without re-migrating already-completed tables.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Valid status values
STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

VALID_STATUSES = {STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_COMPLETED, STATUS_FAILED}


class MigrationState:
    """Track migration progress per table in a YAML state file.

    State is persisted at artifacts/migration_state.yaml. Each table has:
        status: pending | in_progress | completed | failed
        rows_migrated: int (populated on completion)
        error: str (populated on failure)
        started_at: ISO timestamp
        completed_at: ISO timestamp

    Args:
        state_path: Path to the YAML state file. Created if it does not exist.
    """

    def __init__(self, state_path: str) -> None:
        self.state_path = Path(state_path)
        self._state: Dict[str, Dict[str, Any]] = {}

    def load(self) -> Dict[str, Dict[str, Any]]:
        """Load state from the YAML file.

        Returns:
            The full state dict (table_name → status dict).
        """
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    data = yaml.safe_load(f)
                    self._state = data if isinstance(data, dict) else {}
            except Exception as e:
                logger.warning(f"Failed to load migration state: {e}")
                self._state = {}
        else:
            self._state = {}

        logger.debug(f"Loaded migration state: {len(self._state)} tables")
        return self._state

    def save(self) -> None:
        """Persist the current state to the YAML file."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.state_path, "w") as f:
            yaml.dump(
                self._state,
                f,
                default_flow_style=False,
                sort_keys=True,
                allow_unicode=True,
            )

        logger.debug(f"Saved migration state to {self.state_path}")

    def mark_in_progress(self, table: str) -> None:
        """Mark a table as currently being migrated.

        Args:
            table: Table name.
        """
        self._ensure_entry(table)
        self._state[table]["status"] = STATUS_IN_PROGRESS
        self._state[table]["started_at"] = _now_iso()
        self._state[table].pop("error", None)
        self.save()
        logger.info(f"Migration state: {table} → in_progress")

    def mark_completed(self, table: str, rows: int) -> None:
        """Mark a table as successfully migrated.

        Args:
            table: Table name.
            rows: Number of rows migrated.
        """
        self._ensure_entry(table)
        self._state[table]["status"] = STATUS_COMPLETED
        self._state[table]["rows_migrated"] = rows
        self._state[table]["completed_at"] = _now_iso()
        self._state[table].pop("error", None)
        self.save()
        logger.info(f"Migration state: {table} → completed ({rows} rows)")

    def mark_failed(self, table: str, error: str) -> None:
        """Mark a table as failed.

        Args:
            table: Table name.
            error: Error message or traceback.
        """
        self._ensure_entry(table)
        self._state[table]["status"] = STATUS_FAILED
        self._state[table]["error"] = error
        self._state[table]["failed_at"] = _now_iso()
        self.save()
        logger.error(f"Migration state: {table} → failed: {error}")

    def get_status(self, table: str) -> str:
        """Get the current status of a table.

        Args:
            table: Table name.

        Returns:
            Status string: pending | in_progress | completed | failed.
        """
        entry = self._state.get(table, {})
        return entry.get("status", STATUS_PENDING)

    def get_pending_tables(self) -> List[str]:
        """Return all tables that have not yet been completed.

        Includes tables with status: pending, in_progress, or failed.

        Returns:
            List of table names that still need migration.
        """
        return [
            table
            for table, info in self._state.items()
            if info.get("status") != STATUS_COMPLETED
        ]

    def get_summary(self) -> Dict[str, int]:
        """Return a summary count of tables by status.

        Returns:
            Dict mapping status → count.
        """
        summary: Dict[str, int] = {s: 0 for s in VALID_STATUSES}
        for info in self._state.values():
            status = info.get("status", STATUS_PENDING)
            summary[status] = summary.get(status, 0) + 1
        return summary

    def initialize_tables(self, tables: List[str]) -> None:
        """Ensure all tables have state entries.

        Tables already in state are not modified. New tables are set to pending.

        Args:
            tables: List of table names.
        """
        for table in tables:
            self._ensure_entry(table)
        self.save()

    def _ensure_entry(self, table: str) -> None:
        """Create a default state entry for a table if it does not exist."""
        if table not in self._state:
            self._state[table] = {
                "status": STATUS_PENDING,
                "rows_migrated": 0,
                "started_at": None,
                "completed_at": None,
            }


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
