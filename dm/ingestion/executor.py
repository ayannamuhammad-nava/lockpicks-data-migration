"""
DM Migration Executor — L-Ingestor execution engine.

Executes a MigrationPlan in dependency order, tracking state so that
failed runs can be resumed without re-migrating completed tables.
"""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dm.config import get_artifacts_path, get_connection_config
from dm.connectors.postgres import get_connector
from dm.ingestion.planner import MigrationPlan, TableMigrationStep
from dm.ingestion.state import MigrationState, STATUS_COMPLETED

logger = logging.getLogger(__name__)


class MigrationExecutor:
    """Execute a migration plan table-by-table in dependency order.

    For each table:
        1. Mark state as in_progress.
        2. Load transform SQL from artifacts/generated_schema/.
        3. Execute the migration strategy.
        4. Mark state as completed or failed.
        5. Call dm_post_ingest hook.

    Args:
        config: Project configuration dict (from project.yaml).
        plugin_manager: Optional pluggy PluginManager for dm_post_ingest hook.
    """

    def __init__(self, config: Dict, plugin_manager: Any = None) -> None:
        self.config = config
        self.pm = plugin_manager

        # State file lives in the artifacts directory
        artifacts_base = get_artifacts_path(config)
        self._state_path = str(Path(artifacts_base) / "migration_state.yaml")
        self.state = MigrationState(self._state_path)

    def execute(
        self,
        plan: MigrationPlan,
        resume: bool = False,
    ) -> Dict:
        """Execute the full migration plan.

        Args:
            plan: MigrationPlan from MigrationPlanner.generate_plan().
            resume: If True, skip tables already marked completed in state.

        Returns:
            Summary dict with counts and per-table results.
        """
        self.state.load()
        self.state.initialize_tables(plan.tables)

        results: Dict[str, Dict] = {}
        completed_count = 0
        failed_count = 0
        skipped_count = 0

        # Get modern connection for execution
        modern_conn = self._get_modern_connection()

        try:
            modern_conn.connect()

            for table in plan.tables:
                step = plan.strategies.get(table)
                if step is None:
                    logger.warning(f"No strategy found for {table} — skipping")
                    skipped_count += 1
                    continue

                # Resume: skip already-completed tables
                if resume and self.state.get_status(table) == STATUS_COMPLETED:
                    logger.info(f"Skipping already-completed table: {table}")
                    skipped_count += 1
                    results[table] = {
                        "status": "skipped",
                        "reason": "already completed",
                    }
                    continue

                # Execute this table
                logger.info(f"Migrating table: {table} (strategy={step.strategy})")
                self.state.mark_in_progress(table)

                try:
                    table_result = self.execute_table(
                        table=table,
                        strategy=step.strategy,
                        transform_path=step.transform_path,
                        modern_conn=modern_conn,
                    )

                    rows = table_result.get("rows_migrated", 0)
                    self.state.mark_completed(table, rows)
                    completed_count += 1
                    results[table] = {
                        "status": "completed",
                        **table_result,
                    }

                    # Call post-ingest hook
                    self._call_post_ingest_hook(table, table_result)

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Migration failed for {table}: {error_msg}")
                    self.state.mark_failed(table, error_msg)
                    failed_count += 1
                    results[table] = {
                        "status": "failed",
                        "error": error_msg,
                    }

        finally:
            modern_conn.close()

        summary = {
            "total_tables": len(plan.tables),
            "completed": completed_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "state_file": self._state_path,
            "tables": results,
        }

        logger.info(
            f"Migration complete: {completed_count} completed, "
            f"{failed_count} failed, {skipped_count} skipped"
        )

        return summary

    def execute_table(
        self,
        table: str,
        strategy: str,
        transform_path: Optional[str] = None,
        modern_conn: Any = None,
    ) -> Dict:
        """Execute migration for a single table using the given strategy.

        Args:
            table: Table name.
            strategy: Migration strategy name (full_load, external, etc.).
            transform_path: Path to the transform SQL file.
            modern_conn: Database connector for the modern/target system.

        Returns:
            Dict with keys: rows_migrated, duration_seconds.
        """
        start_time = time.time()

        if strategy == "full_load":
            result = self._execute_full_load(table, transform_path, modern_conn)
        elif strategy == "external":
            result = self._execute_external(table)
        else:
            logger.warning(
                f"Unknown strategy '{strategy}' for {table} — "
                f"falling back to full_load"
            )
            result = self._execute_full_load(table, transform_path, modern_conn)

        duration = round(time.time() - start_time, 2)
        result["duration_seconds"] = duration

        logger.info(
            f"Table {table}: {result.get('rows_migrated', 0)} rows "
            f"in {duration}s (strategy={strategy})"
        )

        return result

    def _execute_full_load(
        self,
        table: str,
        transform_path: Optional[str],
        modern_conn: Any,
    ) -> Dict:
        """Full load strategy: truncate target, then execute transform SQL.

        Args:
            table: Table name.
            transform_path: Path to the SQL transform file.
            modern_conn: Database connector.

        Returns:
            Dict with rows_migrated count.
        """
        if modern_conn is None:
            raise RuntimeError(
                f"No modern database connection for full_load of {table}"
            )

        if not transform_path:
            raise FileNotFoundError(
                f"No transform SQL file found for table '{table}'. "
                f"Expected at artifacts/generated_schema/{table}_transforms.sql"
            )

        transform_file = Path(transform_path)
        if not transform_file.exists():
            raise FileNotFoundError(
                f"Transform SQL file not found: {transform_path}"
            )

        transform_sql = transform_file.read_text(encoding="utf-8")

        # Truncate target table first
        logger.info(f"Truncating target table: {table}")
        try:
            modern_conn.execute_query(f"TRUNCATE TABLE {table} CASCADE")
            modern_conn.connection.commit()
        except Exception as e:
            logger.warning(
                f"Truncate failed for {table} (table may not exist yet): {e}"
            )
            modern_conn.connection.rollback()

        # Execute transform SQL
        logger.info(f"Executing transform SQL for: {table}")
        try:
            modern_conn.execute_query(transform_sql)
            modern_conn.connection.commit()
        except Exception as e:
            modern_conn.connection.rollback()
            raise RuntimeError(f"Transform SQL execution failed for {table}: {e}") from e

        # Get row count after load
        try:
            rows = modern_conn.get_row_count(table)
        except Exception:
            rows = 0
            logger.warning(f"Could not count rows in {table} after migration")

        return {"rows_migrated": rows}

    def _execute_external(self, table: str) -> Dict:
        """External strategy: no-op, log that an external tool should handle this.

        Args:
            table: Table name.

        Returns:
            Dict indicating external handling.
        """
        logger.info(
            f"Table '{table}' uses 'external' strategy — "
            f"skipping automated migration. An external tool (e.g., AWS DMS, "
            f"Fivetran, Airbyte) should handle this table."
        )
        return {
            "rows_migrated": 0,
            "note": "External tool migration — not handled by DM",
        }

    def _get_modern_connection(self) -> Any:
        """Instantiate the modern database connector from config."""
        conn_config = get_connection_config(self.config, "modern")

        # Collect plugin connectors if available
        plugin_connectors = {}
        if self.pm:
            try:
                results = self.pm.hook.dm_register_connectors()
                for result in results:
                    if result:
                        plugin_connectors.update(result)
            except Exception:
                pass

        return get_connector(conn_config, plugin_connectors)

    def _call_post_ingest_hook(self, table: str, result: Dict) -> None:
        """Call the dm_post_ingest plugin hook after successful table migration."""
        if not self.pm:
            return

        try:
            self.pm.hook.dm_post_ingest(dataset=table, result=result)
        except Exception as e:
            logger.warning(f"dm_post_ingest hook failed for {table}: {e}")
