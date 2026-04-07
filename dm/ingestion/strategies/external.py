"""
DM External Strategy — no-op delegation to external tools.

Used for tables that should be migrated by an external tool such as
AWS DMS, Fivetran, Airbyte, or a custom ETL pipeline. DM logs the
expectation but does not perform any data movement.
"""

import logging
import time
from typing import Any, Dict

from dm.ingestion.strategies.base import BaseMigrationStrategy

logger = logging.getLogger(__name__)


class ExternalStrategy(BaseMigrationStrategy):
    """External tool delegation: no-op migration strategy.

    Logs that an external tool is expected to handle this table's
    migration and returns immediately with zero rows migrated.
    """

    def execute(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        table: str,
        transform_sql: str,
    ) -> Dict:
        """Log external delegation and return.

        Args:
            legacy_conn: Unused — external tool handles source access.
            modern_conn: Unused — external tool handles target writes.
            table: Table name.
            transform_sql: Unused — external tool has its own transform logic.

        Returns:
            Dict indicating external handling with zero rows migrated.
        """
        start_time = time.time()

        logger.info(
            f"[External] Table '{table}' is configured for external migration. "
            f"DM will not move data for this table. Ensure your external tool "
            f"(e.g., AWS DMS, Fivetran, Airbyte) is configured to handle it."
        )

        duration = round(time.time() - start_time, 2)

        return {
            "rows_migrated": 0,
            "duration_seconds": duration,
            "note": (
                f"Table '{table}' deferred to external migration tool. "
                f"No data was moved by DM."
            ),
        }
