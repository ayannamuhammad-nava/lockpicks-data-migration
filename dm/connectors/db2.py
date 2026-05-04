"""
IBM DB2 connector — for reading from legacy DB2/mainframe systems.

Wraps ibm_db / ibm_db_dbi behind the BaseConnector interface.
Requires the ibm-db package: pip install ibm-db

DB2 is the most common database backing state Medicaid/SNAP/UI
legacy systems (COBOL + DB2 on z/OS).
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from dm.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


def _quote_identifier(name: str) -> str:
    """Quote a DB2 identifier to handle reserved words and case."""
    return f'"{name}"'


class DB2Connector(BaseConnector):
    """IBM DB2 database connector using ibm_db / ibm_db_dbi."""

    def connect(self) -> Any:
        try:
            import ibm_db
            import ibm_db_dbi

            host = self.config["host"]
            port = self.config.get("port", 50000)
            database = self.config["database"]
            user = self.config["user"]
            password = self.config["password"]
            schema = self.config.get("schema", "")

            conn_str = (
                f"DATABASE={database};"
                f"HOSTNAME={host};"
                f"PORT={port};"
                f"PROTOCOL=TCPIP;"
                f"UID={user};"
                f"PWD={password};"
            )

            # Optional: SSL and additional settings
            if self.config.get("ssl", False):
                conn_str += "SECURITY=SSL;"
            if self.config.get("connect_timeout"):
                conn_str += f"CONNECTTIMEOUT={self.config['connect_timeout']};"

            ibm_conn = ibm_db.connect(conn_str, "", "")
            conn = ibm_db_dbi.Connection(ibm_conn)

            # Set default schema if provided
            if schema:
                cursor = conn.cursor()
                cursor.execute(f"SET SCHEMA {schema}")
                cursor.close()

            logger.info(f"Connected to DB2: {database}@{host}:{port}")
            self._conn = conn
            return conn

        except Exception as e:
            logger.error(f"DB2 connection failed: {e}")
            raise

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
                logger.debug("DB2 connection closed")
            except Exception:
                pass

    # ── Schema introspection ─────────────────────────────────────────

    def get_table_schema(self, table_name: str) -> List[Dict]:
        schema = self.config.get("schema", "").upper()
        query = """
            SELECT COLNAME AS column_name,
                   TYPENAME AS data_type,
                   CASE WHEN NULLS = 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable
            FROM SYSCAT.COLUMNS
            WHERE TABNAME = ?
        """
        params = [table_name.upper()]
        if schema:
            query += " AND TABSCHEMA = ?"
            params.append(schema)
        query += " ORDER BY COLNO"

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
        """Compute MD5 hash of column values.

        DB2 supports HASH() with the MD5 algorithm (DB2 LUW 11.1+).
        For z/OS, falls back to a hex digest via GENERATE_UNIQUE.
        """
        tbl = _quote_identifier(table_name)
        col = _quote_identifier(column_name)
        # DB2 LUW approach — HASH(col, 2) gives MD5
        query = (
            f"SELECT HEX(HASH(LISTAGG(CAST({col} AS VARCHAR(4000)), '') "
            f"WITHIN GROUP (ORDER BY {col}), 2)) "
            f"FROM {tbl}"
        )
        try:
            result = self.execute_scalar(query)
            return result if result else ""
        except Exception:
            # Fallback: checksum via count + min + max concatenation
            logger.warning(f"HASH not available for {table_name}.{column_name}, using fallback")
            fallback = (
                f"SELECT CAST(COUNT({col}) AS VARCHAR(20)) || '_' || "
                f"MIN(CAST({col} AS VARCHAR(100))) || '_' || "
                f"MAX(CAST({col} AS VARCHAR(100))) "
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
            f"FETCH FIRST 100 ROWS ONLY"
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
            f"(CAST(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS DOUBLE) * 100.0 "
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
            f") dups"
        )
        return self.execute_scalar(query)

    def compute_checksum(self, table_name: str, columns: List[str]) -> str:
        tbl = _quote_identifier(table_name)
        col_parts = " || ".join(
            f"COALESCE(CAST({_quote_identifier(c)} AS VARCHAR(4000)), 'NULL')"
            for c in columns
        )
        order_col = _quote_identifier(columns[0])
        query = (
            f"SELECT HEX(HASH(LISTAGG({col_parts}, '') "
            f"WITHIN GROUP (ORDER BY {order_col}), 2)) "
            f"FROM {tbl}"
        )
        try:
            result = self.execute_scalar(query)
            return result if result else ""
        except Exception:
            logger.warning(f"HASH not available for checksum on {table_name}, using fallback")
            fallback = (
                f"SELECT CAST(COUNT(*) AS VARCHAR(20)) || '_' || "
                f"CAST(SUM(LENGTH({col_parts})) AS VARCHAR(20)) "
                f"FROM {tbl}"
            )
            result = self.execute_scalar(fallback)
            return result if result else ""
