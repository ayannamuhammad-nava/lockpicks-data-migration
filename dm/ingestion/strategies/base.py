"""
DM Migration Strategy — abstract base class.

All migration strategies implement this interface so the executor
can dispatch to the correct strategy at runtime.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseMigrationStrategy(ABC):
    """Abstract base for migration strategies.

    Each strategy encapsulates the logic for moving data from a legacy
    system to the modern target for one table.
    """

    @abstractmethod
    def execute(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        table: str,
        transform_sql: str,
    ) -> Dict:
        """Execute migration for one table.

        Args:
            legacy_conn: Database connector to the legacy/source system.
            modern_conn: Database connector to the modern/target system.
            table: Name of the table to migrate.
            transform_sql: SQL that transforms legacy data into the modern schema.

        Returns:
            Dict with keys:
                rows_migrated: int — number of rows transferred
                duration_seconds: float — wall-clock time
        """
