"""
DM Full Load Strategy — truncate + reload.

The simplest migration strategy: truncate the target table, then execute
the transform SQL to load all rows from the legacy source.
"""

import logging
import time
from typing import Any, Dict

from dm.ingestion.strategies.base import BaseMigrationStrategy

logger = logging.getLogger(__name__)


class FullLoadStrategy(BaseMigrationStrategy):
    """Full load: truncate target table, then execute transform SQL.

    Suitable for small-to-medium tables where a complete reload is
    acceptable and simpler than incremental approaches.
    """

    def execute(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        table: str,
        transform_sql: str,
    ) -> Dict:
        """Truncate target and execute transform SQL.

        Args:
            legacy_conn: Connector to the legacy database (unused for SQL-based transforms).
            modern_conn: Connector to the modern/target database.
            table: Target table name.
            transform_sql: SQL to execute against the modern database.

        Returns:
            Dict with rows_migrated and duration_seconds.
        """
        start_time = time.time()

        # Step 1: Truncate target table
        logger.info(f"[FullLoad] Truncating target table: {table}")
        try:
            modern_conn.execute_query(f"TRUNCATE TABLE {table} CASCADE")
            modern_conn.connection.commit()
        except Exception as e:
            logger.warning(
                f"[FullLoad] Truncate failed for {table} "
                f"(table may not exist yet): {e}"
            )
            try:
                modern_conn.connection.rollback()
            except Exception:
                pass

        # Step 2: Execute transform SQL
        logger.info(f"[FullLoad] Executing transform SQL for: {table}")
        try:
            modern_conn.execute_query(transform_sql)
            modern_conn.connection.commit()
        except Exception as e:
            try:
                modern_conn.connection.rollback()
            except Exception:
                pass
            raise RuntimeError(
                f"[FullLoad] Transform SQL execution failed for {table}: {e}"
            ) from e

        # Step 3: Count rows
        try:
            rows = modern_conn.get_row_count(table)
        except Exception:
            rows = 0
            logger.warning(f"[FullLoad] Could not count rows in {table}")

        duration = round(time.time() - start_time, 2)

        logger.info(f"[FullLoad] {table}: {rows} rows loaded in {duration}s")

        return {
            "rows_migrated": rows,
            "duration_seconds": duration,
        }
