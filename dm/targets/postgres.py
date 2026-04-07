"""
PostgreSQL Target Adapter

Implements the BaseTargetAdapter interface for PostgreSQL, providing type
mapping, function translation, and DDL generation aligned with the existing
schema_gen.py TYPE_MAP and ABBREVIATION_MAP conventions.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dm.targets.base import BaseTargetAdapter

logger = logging.getLogger(__name__)


# ── Type mapping (mirrors schema_gen.TYPE_MAP) ───────────────────────

TYPE_MAP = {
    # Standard SQL types
    "integer": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "SMALLINT",
    "numeric": "NUMERIC",
    "decimal": "NUMERIC",
    "real": "REAL",
    "double precision": "DOUBLE PRECISION",
    "double": "DOUBLE PRECISION",
    "float": "DOUBLE PRECISION",
    # String types
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "varchar2": "VARCHAR",
    "nvarchar": "VARCHAR",
    "nvarchar2": "VARCHAR",
    "character": "CHAR",
    "char": "CHAR",
    "nchar": "CHAR",
    "text": "TEXT",
    "clob": "TEXT",
    "nclob": "TEXT",
    "long": "TEXT",
    # Boolean
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    # Date / Time
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPTZ",
    "timestamp": "TIMESTAMPTZ",
    "timestamptz": "TIMESTAMPTZ",
    "time": "TIME",
    "time with time zone": "TIMETZ",
    "interval": "INTERVAL",
    # Binary
    "bytea": "BYTEA",
    "blob": "BYTEA",
    "raw": "BYTEA",
    "long raw": "BYTEA",
    # UUID / JSON
    "uuid": "UUID",
    "json": "JSONB",
    "jsonb": "JSONB",
    # Oracle / mainframe legacy types
    "number": "NUMERIC",
    "string": "VARCHAR",
    "binary_float": "REAL",
    "binary_double": "DOUBLE PRECISION",
    "rowid": "VARCHAR(18)",
}

# ── Function translation (Oracle / legacy SQL → PostgreSQL) ──────────

FUNCTION_MAP = {
    "nvl":       lambda args: f"COALESCE({', '.join(args)})",
    "nvl2":      lambda args: (
        f"CASE WHEN {args[0]} IS NOT NULL THEN {args[1]} ELSE {args[2]} END"
        if len(args) >= 3 else f"COALESCE({', '.join(args)})"
    ),
    "sysdate":   lambda args: "NOW()",
    "systimestamp": lambda args: "NOW()",
    "getdate":   lambda args: "NOW()",
    "decode":    lambda _args: _decode_to_case(_args),
    "to_date":   lambda args: (
        f"TO_DATE({args[0]}, {args[1]})" if len(args) >= 2
        else f"{args[0]}::DATE"
    ),
    "to_char":   lambda args: f"TO_CHAR({', '.join(args)})",
    "to_number": lambda args: f"({args[0]})::NUMERIC" if args else "0",
    "instr":     lambda args: (
        f"POSITION({args[1]} IN {args[0]})" if len(args) >= 2
        else f"POSITION('' IN {args[0]})"
    ),
    "substr":    lambda args: f"SUBSTRING({', '.join(args)})",
    "length":    lambda args: f"LENGTH({args[0]})" if args else "LENGTH('')",
    "lengthb":   lambda args: f"OCTET_LENGTH({args[0]})" if args else "OCTET_LENGTH('')",
    "concat":    lambda args: " || ".join(args) if args else "''",
    "ifnull":    lambda args: f"COALESCE({', '.join(args)})",
    "isnull":    lambda args: f"COALESCE({', '.join(args)})",
    "charindex": lambda args: (
        f"POSITION({args[0]} IN {args[1]})" if len(args) >= 2
        else f"POSITION('' IN {args[0]})"
    ),
    "dateadd":   lambda args: (
        f"({args[2]} + INTERVAL '1 {args[0]}' * {args[1]})"
        if len(args) >= 3 else f"NOW()"
    ),
    "datediff":  lambda args: (
        f"EXTRACT(EPOCH FROM ({args[2]}::TIMESTAMP - {args[1]}::TIMESTAMP))"
        if len(args) >= 3 else f"0"
    ),
    "user":      lambda args: "CURRENT_USER",
    "rownum":    lambda args: "ROW_NUMBER() OVER ()",
}


def _decode_to_case(args: list) -> str:
    """Translate Oracle DECODE(expr, val1, result1, ..., default) to CASE."""
    if len(args) < 3:
        return f"/* DECODE with insufficient args: {', '.join(args)} */"

    expr = args[0]
    pairs = args[1:]
    parts = [f"CASE {expr}"]

    # pairs come as (search, result, search, result, ..., [default])
    i = 0
    while i < len(pairs) - 1:
        parts.append(f"    WHEN {pairs[i]} THEN {pairs[i + 1]}")
        i += 2

    # Odd remainder is the default
    if i < len(pairs):
        parts.append(f"    ELSE {pairs[i]}")

    parts.append("END")
    return "\n".join(parts)


class PostgresTargetAdapter(BaseTargetAdapter):
    """PostgreSQL target platform adapter."""

    def dialect_name(self) -> str:
        return "postgres"

    # ── Type Mapping ─────────────────────────────────────────────

    def map_type(self, source_type: str, profiling_stats: Optional[dict] = None) -> str:
        """Map a source data type to PostgreSQL equivalent.

        Uses profiling stats for intelligent type narrowing when available
        (e.g., NUMERIC with only integer values -> INTEGER).
        """
        stats = profiling_stats or {}

        # Extract base type and size qualifier
        raw = source_type.strip()
        base = raw.lower().split("(")[0].strip()
        size_match = re.search(r"\(([^)]+)\)", raw)
        size_qualifier = size_match.group(1) if size_match else None

        # Profiling-based narrowing: NUMERIC with integer-only data
        if base in ("numeric", "decimal", "number") and stats:
            min_val = stats.get("min_value")
            max_val = stats.get("max_value")
            if min_val is not None and max_val is not None:
                try:
                    fmin, fmax = float(min_val), float(max_val)
                    if fmin == int(fmin) and fmax == int(fmax):
                        if -2147483648 <= fmin and fmax <= 2147483647:
                            return "INTEGER"
                        return "BIGINT"
                except (ValueError, TypeError, OverflowError):
                    pass

        # Profiling-based narrowing: VARCHAR with boolean-like values
        if base in ("character varying", "varchar", "varchar2", "nvarchar",
                     "nvarchar2", "text", "string", "char") and stats:
            distinct_count = stats.get("distinct_count", 0)
            frequencies = stats.get("value_frequencies", [])
            if distinct_count == 2 and frequencies:
                vals = set()
                if isinstance(frequencies, list):
                    vals = {str(v.get("value", v)).upper() for v in frequencies}
                elif isinstance(frequencies, dict):
                    vals = {str(k).upper() for k in frequencies}
                bool_pairs = [
                    {"Y", "N"}, {"YES", "NO"}, {"T", "F"},
                    {"TRUE", "FALSE"}, {"1", "0"},
                ]
                if vals in bool_pairs:
                    return "BOOLEAN"

        # Look up in type map
        pg_type = TYPE_MAP.get(base)

        if pg_type is None:
            logger.warning(
                f"Unmapped source type '{source_type}', defaulting to VARCHAR"
            )
            pg_type = "VARCHAR"

        # Preserve size qualifier for types that use it
        if size_qualifier and pg_type in ("VARCHAR", "CHAR", "NUMERIC"):
            return f"{pg_type}({size_qualifier})"

        return pg_type

    # ── DDL Rendering ────────────────────────────────────────────

    def render_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_key: str,
        foreign_keys: List[Dict] = None,
        comment: str = "",
    ) -> str:
        """Render PostgreSQL CREATE TABLE DDL.

        Column dicts are expected to have:
            name: str
            data_type: str
            nullable: bool
            constraints: list[str]  (e.g., ["NOT NULL", "UNIQUE", "DEFAULT NOW()"])
        """
        foreign_keys = foreign_keys or []

        lines = [
            f"-- Generated by DM Target Adapter (postgres)",
            f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
            "",
            f"CREATE TABLE {table_name} (",
        ]

        col_defs = []
        for col in columns:
            name = col["name"]
            data_type = col["data_type"]
            nullable = col.get("nullable", True)
            constraints = col.get("constraints", [])

            parts = [f"    {name:<25} {data_type}"]

            # Inline constraints (skip CHECK and REFERENCES for separate lines)
            for c in constraints:
                if not c.startswith("CHECK") and not c.startswith("REFERENCES"):
                    parts.append(f" {c}")

            if not nullable and "NOT NULL" not in constraints and "PRIMARY KEY" not in constraints:
                parts.append(" NOT NULL")

            col_comment = col.get("comment", "")
            if col_comment:
                parts.append(f"  -- {col_comment}")

            col_defs.append("".join(parts))

        # Foreign key constraints
        for fk in foreign_keys:
            col_defs.append(
                f"    CONSTRAINT fk_{table_name}_{fk['column']} "
                f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
            )

        # CHECK constraints
        for col in columns:
            for c in col.get("constraints", []):
                if c.startswith("CHECK"):
                    col_defs.append(f"    {c}")

        lines.append(",\n".join(col_defs))
        lines.append(");")

        # Indexes on FK columns
        for fk in foreign_keys:
            lines.append(
                f"\nCREATE INDEX idx_{table_name}_{fk['column']} "
                f"ON {table_name}({fk['column']});"
            )

        # Table comment via COMMENT ON
        if comment:
            escaped = comment.replace("'", "''")
            lines.append(f"\nCOMMENT ON TABLE {table_name} IS '{escaped}';")

        # Column comments via COMMENT ON
        for col in columns:
            col_comment = col.get("comment", "")
            if col_comment:
                escaped = col_comment.replace("'", "''")
                lines.append(
                    f"COMMENT ON COLUMN {table_name}.{col['name']} IS '{escaped}';"
                )

        lines.append("")
        return "\n".join(lines)

    # ── INSERT ... SELECT Rendering ──────────────────────────────

    def render_insert_select(
        self,
        target_table: str,
        source_table: str,
        column_mappings: List[Dict],
    ) -> str:
        """Render PostgreSQL INSERT INTO ... SELECT.

        column_mappings: list of {target_col, source_expr} dicts.
        """
        if not column_mappings:
            return f"-- No column mappings for {target_table}\n"

        target_cols = [m["target_col"] for m in column_mappings]
        source_exprs = [m["source_expr"] for m in column_mappings]

        cols_str = ",\n    ".join(target_cols)
        exprs_str = ",\n    ".join(source_exprs)

        lines = [
            f"-- Migration: {source_table} -> {target_table}",
            f"-- Review and customize before execution",
            "",
            f"INSERT INTO {target_table} (",
            f"    {cols_str}",
            f")",
            f"SELECT",
            f"    {exprs_str}",
            f"FROM {source_table};",
            "",
        ]
        return "\n".join(lines)

    # ── Function Translation ─────────────────────────────────────

    def translate_function(self, func_name: str, args: list) -> str:
        """Translate a SQL function call to PostgreSQL equivalent."""
        key = func_name.lower().strip()
        translator = FUNCTION_MAP.get(key)

        if translator:
            return translator(args)

        # No translation needed — pass through as-is
        args_str = ", ".join(args) if args else ""
        return f"{func_name}({args_str})"

    # ── Feature Support ──────────────────────────────────────────

    def supports_serial(self) -> bool:
        return True

    def supports_check_constraints(self) -> bool:
        return True


# ── Built-in Target Registry ─────────────────────────────────────────

BUILTIN_TARGETS = {
    "postgres": PostgresTargetAdapter,
    "postgresql": PostgresTargetAdapter,
}


def get_target_adapter(
    target_name: str,
    plugin_targets: dict = None,
) -> BaseTargetAdapter:
    """Instantiate the right target adapter by name.

    Args:
        target_name: Platform name (e.g., 'postgres', 'snowflake').
        plugin_targets: Extra adapters from plugins (dm_register_targets hook).

    Returns:
        A BaseTargetAdapter instance.

    Raises:
        ValueError: If the target name is not registered.
    """
    registry = {**BUILTIN_TARGETS}
    if plugin_targets:
        registry.update(plugin_targets)

    adapter_cls = registry.get(target_name.lower())
    if not adapter_cls:
        available = ", ".join(sorted(registry.keys()))
        raise ValueError(
            f"Unknown target '{target_name}'. Available: {available}"
        )
    return adapter_cls()
