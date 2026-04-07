"""
PostgreSQL connector — the default built-in connector.

Wraps psycopg2 behind the BaseConnector interface.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from dm.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """PostgreSQL database connector using psycopg2."""

    def connect(self) -> psycopg2.extensions.connection:
        try:
            conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config.get("port", 5432),
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            logger.info(f"Connected to PostgreSQL: {self.config['database']}")
            self._conn = conn
            return conn
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("PostgreSQL connection closed")

    # ── Schema introspection ─────────────────────────────────────────

    def get_table_schema(self, table_name: str) -> List[Dict]:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (table_name,))
            return [dict(row) for row in cur.fetchall()]

    # ── Query execution ──────────────────────────────────────────────

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        return pd.read_sql_query(query, self.connection, params=params)

    def execute_scalar(self, query: str, params: Optional[tuple] = None) -> Any:
        with self.connection.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row else None

    # ── Validation helpers ───────────────────────────────────────────

    def get_row_count(self, table_name: str) -> int:
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
        with self.connection.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def get_column_hash(self, table_name: str, column_name: str) -> str:
        q = sql.SQL(
            "SELECT MD5(STRING_AGG({col}::text, '' ORDER BY {col})) FROM {tbl}"
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
        with self.connection.cursor() as cur:
            cur.execute(q)
            result = cur.fetchone()
            return result[0] if result and result[0] else ""

    def check_referential_integrity(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: Optional[str] = None,
    ) -> Dict:
        pk_column = pk_column or fk_column
        q = sql.SQL(
            "SELECT {child}.{fk} "
            "FROM {child} "
            "LEFT JOIN {parent} ON {child}.{fk} = {parent}.{pk} "
            "WHERE {parent}.{pk} IS NULL "
            "LIMIT 100"
        ).format(
            child=sql.Identifier(child_table),
            parent=sql.Identifier(parent_table),
            fk=sql.Identifier(fk_column),
            pk=sql.Identifier(pk_column),
        )
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q)
            orphans = [row[fk_column] for row in cur.fetchall()]
        return {"orphan_count": len(orphans), "orphan_sample": orphans[:10]}

    def get_null_percentage(self, table_name: str, column_name: str) -> float:
        q = sql.SQL(
            "SELECT (COUNT(*) FILTER (WHERE {col} IS NULL) * 100.0 / NULLIF(COUNT(*), 0)) "
            "FROM {tbl}"
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
        with self.connection.cursor() as cur:
            cur.execute(q)
            result = cur.fetchone()
            return float(result[0]) if result and result[0] else 0.0

    def get_duplicate_count(self, table_name: str, column_name: str) -> int:
        q = sql.SQL(
            "SELECT COUNT(*) FROM ("
            "  SELECT {col} FROM {tbl} GROUP BY {col} HAVING COUNT(*) > 1"
            ") dups"
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
        with self.connection.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def compute_checksum(self, table_name: str, columns: List[str]) -> str:
        col_parts = sql.SQL(" || ").join([
            sql.SQL("COALESCE({col}::text, 'NULL')").format(col=sql.Identifier(c))
            for c in columns
        ])
        q = sql.SQL(
            "SELECT MD5(STRING_AGG({cols}, '' ORDER BY {order_col})) FROM {tbl}"
        ).format(
            cols=col_parts,
            order_col=sql.Identifier(columns[0]),
            tbl=sql.Identifier(table_name),
        )
        with self.connection.cursor() as cur:
            cur.execute(q)
            result = cur.fetchone()
            return result[0] if result and result[0] else ""


# ── Connector factory ────────────────────────────────────────────────

# Built-in registry; extended by dm_register_connectors hook.
BUILTIN_CONNECTORS = {
    "postgres": PostgresConnector,
    "postgresql": PostgresConnector,
}


def get_connector(config: Dict, plugin_connectors: Optional[Dict] = None) -> BaseConnector:
    """Instantiate the right connector for a connection config.

    Args:
        config: Connection dict with a 'type' key (defaults to 'postgres').
        plugin_connectors: Extra connectors from plugins.

    Returns:
        A BaseConnector instance (not yet connected).
    """
    conn_type = config.get("type", "postgres").lower()
    registry = {**BUILTIN_CONNECTORS}
    if plugin_connectors:
        registry.update(plugin_connectors)

    connector_cls = registry.get(conn_type)
    if connector_cls is None:
        available = ", ".join(sorted(registry.keys()))
        raise ValueError(
            f"Unknown connector type '{conn_type}'. Available: {available}"
        )

    return connector_cls(config)
