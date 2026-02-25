"""
Data sampling utilities for extracting representative samples from databases.
Uses psycopg2.sql module for safe identifier quoting (prevents SQL injection).
"""
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


def _execute_to_df(conn: psycopg2.extensions.connection, query_str: str) -> pd.DataFrame:
    """Execute a SQL string via psycopg2 cursor and return a DataFrame."""
    with conn.cursor() as cur:
        cur.execute(query_str)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def sample_table(
    conn: psycopg2.extensions.connection,
    table_name: str,
    sample_size: int = 1000,
    stratify_column: Optional[str] = None,
    random_seed: int = 42
) -> pd.DataFrame:
    """
    Extract a random sample from a database table.

    Args:
        conn: Database connection
        table_name: Name of the table to sample
        sample_size: Number of rows to sample
        stratify_column: Optional column name for stratified sampling
        random_seed: Random seed for reproducibility

    Returns:
        pandas DataFrame with sampled data
    """
    if stratify_column:
        query = sql.SQL(
            "WITH stratified AS ("
            "  SELECT *, "
            "    ROW_NUMBER() OVER (PARTITION BY {strat_col} ORDER BY RANDOM()) as rn, "
            "    COUNT(*) OVER (PARTITION BY {strat_col}) as group_size "
            "  FROM {tbl}"
            ") "
            "SELECT * FROM stratified "
            "WHERE rn <= CEIL({size}::float / (SELECT COUNT(DISTINCT {strat_col}) FROM {tbl})) "
            "LIMIT {size}"
        ).format(
            strat_col=sql.Identifier(stratify_column),
            tbl=sql.Identifier(table_name),
            size=sql.Literal(sample_size)
        )
    else:
        query = sql.SQL(
            "SELECT * FROM {tbl} ORDER BY RANDOM() LIMIT {size}"
        ).format(
            tbl=sql.Identifier(table_name),
            size=sql.Literal(sample_size)
        )

    logger.info(f"Sampling {sample_size} rows from {table_name}")
    df = _execute_to_df(conn, query.as_string(conn))
    logger.info(f"Sampled {len(df)} rows")

    return df


def sample_with_conditions(
    conn: psycopg2.extensions.connection,
    table_name: str,
    conditions: List[str],
    sample_size: int = 1000
) -> pd.DataFrame:
    """
    Sample data with specific WHERE conditions.
    Note: conditions are raw SQL strings - only use with trusted internal input.
    """
    where_clause = ' AND '.join(conditions) if conditions else '1=1'

    query = sql.SQL(
        "SELECT * FROM {tbl} WHERE " + where_clause + " ORDER BY RANDOM() LIMIT {size}"
    ).format(
        tbl=sql.Identifier(table_name),
        size=sql.Literal(sample_size)
    )

    logger.info(f"Sampling {sample_size} rows from {table_name} with conditions")
    df = _execute_to_df(conn, query.as_string(conn))

    return df


def get_column_sample(
    conn: psycopg2.extensions.connection,
    table_name: str,
    columns: List[str],
    sample_size: int = 1000
) -> pd.DataFrame:
    """Sample specific columns from a table (memory-efficient)."""
    col_list = sql.SQL(', ').join([sql.Identifier(c) for c in columns])

    query = sql.SQL(
        "SELECT {cols} FROM {tbl} ORDER BY RANDOM() LIMIT {size}"
    ).format(
        cols=col_list,
        tbl=sql.Identifier(table_name),
        size=sql.Literal(sample_size)
    )

    logger.info(f"Sampling {len(columns)} columns from {table_name}")
    df = _execute_to_df(conn, query.as_string(conn))

    return df


def sample_for_comparison(
    legacy_conn: psycopg2.extensions.connection,
    modern_conn: psycopg2.extensions.connection,
    table_name: str,
    key_column: str,
    sample_size: int = 100
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sample matching records from both legacy and modern systems for comparison.

    Returns:
        Tuple of (legacy_df, modern_df) with matching records
    """
    # Sample IDs from legacy
    id_query = sql.SQL(
        "SELECT {col} FROM {tbl} ORDER BY RANDOM() LIMIT {size}"
    ).format(
        col=sql.Identifier(key_column),
        tbl=sql.Identifier(table_name),
        size=sql.Literal(sample_size)
    )

    legacy_df = _execute_to_df(legacy_conn, id_query.as_string(legacy_conn))

    if legacy_df.empty:
        return legacy_df, pd.DataFrame()

    # Get the sampled IDs
    ids_list = legacy_df[key_column].tolist()

    # Fetch full records from legacy using parameterized query
    placeholders = ', '.join(['%s'] * len(ids_list))
    legacy_full_query = sql.SQL(
        "SELECT * FROM {tbl} WHERE {col} IN (" + placeholders + ")"
    ).format(
        tbl=sql.Identifier(table_name),
        col=sql.Identifier(key_column)
    )

    with legacy_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(legacy_full_query, ids_list)
        legacy_df = pd.DataFrame(cur.fetchall())

    # Fetch matching records from modern using parameterized query
    modern_full_query = sql.SQL(
        "SELECT * FROM {tbl} WHERE {col} IN (" + placeholders + ")"
    ).format(
        tbl=sql.Identifier(table_name),
        col=sql.Identifier(key_column)
    )

    with modern_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(modern_full_query, ids_list)
        result = cur.fetchall()
        modern_df = pd.DataFrame(result) if result else pd.DataFrame()

    logger.info(f"Sampled {len(legacy_df)} legacy / {len(modern_df)} modern records for comparison")

    return legacy_df, modern_df
