"""
Oracle Database connector — for reading from legacy Oracle systems.

Wraps oracledb (formerly cx_Oracle) behind the BaseConnector interface.
Requires: pip install oracledb

Many state government financial and provider systems run on Oracle.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from dm.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


def _quote_identifier(name: str) -> str:
    """Quote an Oracle identifier."""
    return f'"{name}"'


class OracleConnector(BaseConnector):
    """Oracle Database connector using oracledb (thin mode by default)."""

    def connect(self) -> Any:
        try:
            import oracledb

            host = self.config["host"]
            port = self.config.get("port", 1521)
            user = self.config["user"]
            password = self.config["password"]

            # Support both service_name and SID connection styles
            service_name = self.config.get("service_name")
            sid = self.config.get("sid", self.config.get("database"))

            if service_name:
                dsn = oracledb.makedsn(host, port, service_name=service_name)
            elif sid:
                dsn = oracledb.makedsn(host, port, sid=sid)
            else:
                raise ValueError(
                    "Oracle connection requires 'service_name' or 'sid' (or 'database') "
                    "in project.yaml"
                )

            # Use thin mode by default (no Oracle client required)
            conn = oracledb.connect(user=user, password=password, dsn=dsn)

            # Set default schema if provided
            schema = self.config.get("schema")
            if schema:
                cursor = conn.cursor()
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
                cursor.close()

            logger.info(f"Connected to Oracle: {dsn}")
            self._conn = conn
            return conn

        except Exception as e:
            logger.error(f"Oracle connection failed: {e}")
            raise

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
                logger.debug("Oracle connection closed")
            except Exception:
                pass

    # ── Schema introspection ─────────────────────────────────────────

    def get_table_schema(self, table_name: str) -> List[Dict]:
        owner = self.config.get("schema", "").upper()
        query = """
            SELECT COLUMN_NAME AS column_name,
                   DATA_TYPE AS data_type,
                   CASE WHEN NULLABLE = 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = :1
        """
        params = [table_name.upper()]
        if owner:
            query += " AND OWNER = :2"
            params.append(owner)
        query += " ORDER BY COLUMN_ID"

        cursor = self.connection.cursor()
        cursor.execute(query, params)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows

    # ── Query execution ──────────────────────────────────────────────

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        return pd.read_sql(query, self.connection, params=params)

    def execute_scalar(self, query: str, params: Optional[tuple] = None) -> Any:
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    # ── Validation helpers ───────────────────────────────────────────

    def get_row_count(self, table_name: str) -> int:
        tbl = _quote_identifier(table_name)
        return self.execute_scalar(f"SELECT COUNT(*) FROM {tbl}")

    def get_column_hash(self, table_name: str, column_name: str) -> str:
        """Compute MD5 hash of column values using DBMS_CRYPTO or ORA_HASH.

        Uses LISTAGG + STANDARD_HASH for Oracle 12c+.
        Falls back to ORA_HASH aggregate for older versions.
        """
        tbl = _quote_identifier(table_name)
        col = _quote_identifier(column_name)

        # Oracle 12c+ approach using STANDARD_HASH
        query = (
            f"SELECT LOWER(RAWTOHEX(STANDARD_HASH("
            f"  LISTAGG(CAST({col} AS VARCHAR2(4000)), '') "
            f"  WITHIN GROUP (ORDER BY {col}), "
            f"  'MD5'"
            f"))) FROM {tbl}"
        )
        try:
            result = self.execute_scalar(query)
            return result if result else ""
        except Exception:
            # Fallback: ORA_HASH aggregate
            logger.warning(
                f"STANDARD_HASH not available for {table_name}.{column_name}, using ORA_HASH"
            )
            fallback = (
                f"SELECT TO_CHAR(SUM(ORA_HASH(CAST({col} AS VARCHAR2(4000))))) "
                f"FROM {tbl}"
            )
            result = self.execute_scalar(fallback)
            return result if result else ""

    def check_referential_integrity(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: Optional[str] = None,
    ) -> Dict:
        pk_column = pk_column or fk_column
        child = _quote_identifier(child_table)
        parent = _quote_identifier(parent_table)
        fk = _quote_identifier(fk_column)
        pk = _quote_identifier(pk_column)

        query = (
            f"SELECT c.{fk} "
            f"FROM {child} c "
            f"LEFT JOIN {parent} p ON c.{fk} = p.{pk} "
            f"WHERE p.{pk} IS NULL "
            f"AND ROWNUM <= 100"
        )
        cursor = self.connection.cursor()
        cursor.execute(query)
        orphans = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return {"orphan_count": len(orphans), "orphan_sample": orphans[:10]}

    def get_null_percentage(self, table_name: str, column_name: str) -> float:
        tbl = _quote_identifier(table_name)
        col = _quote_identifier(column_name)
        query = (
            f"SELECT "
            f"(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 100.0 "
            f"/ NULLIF(COUNT(*), 0)) "
            f"FROM {tbl}"
        )
        result = self.execute_scalar(query)
        return float(result) if result else 0.0

    def get_duplicate_count(self, table_name: str, column_name: str) -> int:
        tbl = _quote_identifier(table_name)
        col = _quote_identifier(column_name)
        query = (
            f"SELECT COUNT(*) FROM ("
            f"  SELECT {col} FROM {tbl} GROUP BY {col} HAVING COUNT(*) > 1"
            f")"
        )
        return self.execute_scalar(query)

    def compute_checksum(self, table_name: str, columns: List[str]) -> str:
        tbl = _quote_identifier(table_name)
        col_parts = " || ".join(
            f"NVL(CAST({_quote_identifier(c)} AS VARCHAR2(4000)), 'NULL')"
            for c in columns
        )
        order_col = _quote_identifier(columns[0])

        query = (
            f"SELECT LOWER(RAWTOHEX(STANDARD_HASH("
            f"  LISTAGG({col_parts}, '') "
            f"  WITHIN GROUP (ORDER BY {order_col}), "
            f"  'MD5'"
            f"))) FROM {tbl}"
        )
        try:
            result = self.execute_scalar(query)
            return result if result else ""
        except Exception:
            logger.warning(f"STANDARD_HASH not available for checksum on {table_name}")
            fallback = (
                f"SELECT TO_CHAR(SUM(ORA_HASH({col_parts}))) "
                f"FROM {tbl}"
            )
            result = self.execute_scalar(fallback)
            return result if result else ""
