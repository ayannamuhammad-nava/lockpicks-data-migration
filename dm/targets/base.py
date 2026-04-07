"""
Abstract base class for target platform adapters.

Target adapters encapsulate all platform-specific DDL generation, type mapping,
and SQL dialect translation.  Built-in: PostgreSQL.  Additional targets can be
registered via the dm_register_targets hook.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseTargetAdapter(ABC):
    """Abstract base for target platform code generation."""

    @abstractmethod
    def dialect_name(self) -> str:
        """Return target platform name (e.g., 'postgres', 'snowflake')."""

    @abstractmethod
    def map_type(self, source_type: str, profiling_stats: Optional[dict] = None) -> str:
        """Map a source data type to this platform's equivalent."""

    @abstractmethod
    def render_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_key: str,
        foreign_keys: List[Dict] = None,
        comment: str = "",
    ) -> str:
        """Render CREATE TABLE DDL in this platform's dialect.

        Args:
            table_name: Name of the table to create.
            columns: List of column dicts, each with at least
                     {name, data_type, nullable, constraints}.
            primary_key: Name of the primary key column.
            foreign_keys: Optional list of {column, references} dicts.
            comment: Optional table-level comment.

        Returns:
            A complete CREATE TABLE statement string.
        """

    @abstractmethod
    def render_insert_select(
        self,
        target_table: str,
        source_table: str,
        column_mappings: List[Dict],
    ) -> str:
        """Render INSERT INTO ... SELECT ... in this platform's dialect.

        Args:
            target_table: Destination table name.
            source_table: Source table name.
            column_mappings: List of {target_col, source_expr} dicts.

        Returns:
            A complete INSERT INTO ... SELECT statement string.
        """

    @abstractmethod
    def translate_function(self, func_name: str, args: list) -> str:
        """Translate a SQL function call to this platform's equivalent.

        Args:
            func_name: Original function name (e.g., 'NVL', 'SYSDATE').
            args: List of argument strings.

        Returns:
            Translated function call as a SQL string.
        """

    @abstractmethod
    def supports_serial(self) -> bool:
        """Whether platform supports SERIAL/auto-increment columns."""

    @abstractmethod
    def supports_check_constraints(self) -> bool:
        """Whether platform supports CHECK constraints."""
