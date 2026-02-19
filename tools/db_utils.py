"""
Database utility functions for PostgreSQL connections and operations.
Uses psycopg2.sql module for safe identifier quoting (prevents SQL injection).
"""
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pandas as pd
import hashlib
from contextlib import contextmanager
from typing import Dict, Optional, List, Any
import logging
import os
import re

logger = logging.getLogger(__name__)


def process_config_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process environment variable substitution in config.
    Replaces ${VAR_NAME:default_value} with environment variable or default.
    
    Args:
        config: Configuration dictionary (typically from YAML)
    
    Returns:
        Configuration dictionary with environment variables substituted
    """
    def replace_env_var(value: str) -> str:
        """Replace ${VAR:default} patterns with environment variables or defaults."""
        if not isinstance(value, str):
            return value
        
        pattern = r'\$\{([^:}]+):([^}]*)\}'
        
        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2)
            return os.environ.get(var_name, default_value)
        
        return re.sub(pattern, replacer, value)
    
    def process_value(value: Any) -> Any:
        """Recursively process values to handle env var substitution."""
        if isinstance(value, dict):
            return {k: process_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [process_value(v) for v in value]
        elif isinstance(value, str):
            return replace_env_var(value)
        else:
            return value
    
    return process_value(config)


def get_connection(db_config: Dict) -> psycopg2.extensions.connection:
    """
    Create a PostgreSQL database connection.

    Args:
        db_config: Dict with keys: host, port, database, user, password

    Returns:
        psycopg2 connection object
    """
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        logger.info(f"Connected to database: {db_config['database']}")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


@contextmanager
def get_managed_connection(db_config: Dict):
    """Context manager for database connections to prevent leaks."""
    conn = get_connection(db_config)
    try:
        yield conn
    finally:
        conn.close()


def get_row_count(conn: psycopg2.extensions.connection, table_name: str) -> int:
    """Get the total row count for a table."""
    query = sql.SQL("SELECT COUNT(*) as count FROM {}").format(
        sql.Identifier(table_name)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result['count'] if result else 0


def get_column_hash(conn: psycopg2.extensions.connection, table_name: str, column_name: str) -> str:
    """Compute a hash of all values in a column (for integrity checks)."""
    query = sql.SQL(
        "SELECT MD5(STRING_AGG({col}::text, '' ORDER BY {col})) as hash FROM {tbl}"
    ).format(
        col=sql.Identifier(column_name),
        tbl=sql.Identifier(table_name)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result['hash'] if result and result['hash'] else ''


def check_referential_integrity(
    conn: psycopg2.extensions.connection,
    child_table: str,
    parent_table: str,
    foreign_key_column: str,
    parent_key_column: str = None
) -> Dict:
    """
    Check referential integrity between two tables.

    Returns:
        Dict with orphan_count and orphan_sample (list of orphan IDs)
    """
    if parent_key_column is None:
        parent_key_column = foreign_key_column

    query = sql.SQL(
        "SELECT {child}.{fk} "
        "FROM {child} "
        "LEFT JOIN {parent} ON {child}.{fk} = {parent}.{pk} "
        "WHERE {parent}.{pk} IS NULL "
        "LIMIT 100"
    ).format(
        child=sql.Identifier(child_table),
        parent=sql.Identifier(parent_table),
        fk=sql.Identifier(foreign_key_column),
        pk=sql.Identifier(parent_key_column)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        orphans = [row[foreign_key_column] for row in cur.fetchall()]

    return {
        'orphan_count': len(orphans),
        'orphan_sample': orphans[:10]
    }


def execute_query(conn: psycopg2.extensions.connection, query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as DataFrame.
    Note: This accepts raw SQL strings. Only use with trusted internal queries.
    """
    return pd.read_sql_query(query, conn)


def get_table_schema(conn: psycopg2.extensions.connection, table_name: str) -> List[Dict]:
    """Get schema information for a table from information_schema."""
    query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (table_name,))
        return [dict(row) for row in cur.fetchall()]


def compute_checksum(conn: psycopg2.extensions.connection, table_name: str, columns: List[str]) -> str:
    """Compute checksum for multiple columns (for reconciliation)."""
    col_parts = sql.SQL(' || ').join([
        sql.SQL("COALESCE({col}::text, 'NULL')").format(col=sql.Identifier(c))
        for c in columns
    ])
    query = sql.SQL(
        "SELECT MD5(STRING_AGG({cols}, '' ORDER BY {order_col})) as checksum FROM {tbl}"
    ).format(
        cols=col_parts,
        order_col=sql.Identifier(columns[0]),
        tbl=sql.Identifier(table_name)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result['checksum'] if result and result['checksum'] else ''


def get_null_percentage(conn: psycopg2.extensions.connection, table_name: str, column_name: str) -> float:
    """Calculate percentage of null values in a column."""
    query = sql.SQL(
        "SELECT (COUNT(*) FILTER (WHERE {col} IS NULL) * 100.0 / NULLIF(COUNT(*), 0)) as null_pct FROM {tbl}"
    ).format(
        col=sql.Identifier(column_name),
        tbl=sql.Identifier(table_name)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        return float(result['null_pct']) if result and result['null_pct'] else 0.0


def get_duplicate_count(conn: psycopg2.extensions.connection, table_name: str, column_name: str) -> int:
    """Count duplicate values in a column (should be unique)."""
    query = sql.SQL(
        "SELECT COUNT(*) as dup_count FROM ("
        "  SELECT {col} FROM {tbl} GROUP BY {col} HAVING COUNT(*) > 1"
        ") dups"
    ).format(
        col=sql.Identifier(column_name),
        tbl=sql.Identifier(table_name)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result['dup_count'] if result else 0
