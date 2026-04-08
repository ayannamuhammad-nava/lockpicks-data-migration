"""
Abstract base class for database connectors.

All database-specific operations go through this interface so the toolkit
remains database-agnostic.  Built-in implementations: PostgresConnector.
Additional connectors can be registered via the dm_register_connectors hook.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import pandas as pd


class BaseConnector(ABC):
    """Abstract database connector."""

    def __init__(self, config: Dict):
        """Initialize with connection configuration.

        Args:
            config: Connection dict from project.yaml (host, port, database, user, password, …).
        """
        self.config = config
        self._conn: Any = None

    # ── Connection lifecycle ─────────────────────────────────────────

    @abstractmethod
    def connect(self) -> Any:
        """Open and return the underlying connection object."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""

    @property
    def connection(self) -> Any:
        """Return the raw connection, opening it lazily if needed."""
        if self._conn is None:
            self._conn = self.connect()
        return self._conn

    @contextmanager
    def managed(self):
        """Context-manager that guarantees the connection is closed."""
        try:
            yield self.connection
        finally:
            self.close()

    # ── Schema introspection ─────────────────────────────────────────

    @abstractmethod
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Return column metadata for *table_name*.

        Each dict must contain at least:
            column_name: str
            data_type: str
            is_nullable: str ('YES' / 'NO')
        """

    # ── Query execution ──────────────────────────────────────────────

    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SQL query and return a DataFrame."""

    @abstractmethod
    def execute_scalar(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return a single scalar value."""

    # ── Common validation helpers ────────────────────────────────────

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """Return total row count for *table_name*."""

    @abstractmethod
    def get_column_hash(self, table_name: str, column_name: str) -> str:
        """Compute an MD5 hash of all values in a column (for integrity checks)."""

    @abstractmethod
    def check_referential_integrity(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: Optional[str] = None,
    ) -> Dict:
        """Check FK integrity.  Returns {'orphan_count': int, 'orphan_sample': list}."""

    @abstractmethod
    def get_null_percentage(self, table_name: str, column_name: str) -> float:
        """Return the percentage of NULL values in a column."""

    @abstractmethod
    def get_duplicate_count(self, table_name: str, column_name: str) -> int:
        """Return the number of duplicate values in a column."""

    @abstractmethod
    def compute_checksum(self, table_name: str, columns: List[str]) -> str:
        """Compute a composite checksum over multiple columns."""
