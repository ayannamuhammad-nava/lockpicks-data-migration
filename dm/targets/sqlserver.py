"""
Microsoft SQL Server / Azure SQL Target Adapter

Implements the BaseTargetAdapter interface for SQL Server and Azure SQL Database,
providing type mapping, function translation, and DDL generation.

Covers both on-premise SQL Server and Azure SQL Database — same T-SQL dialect.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dm.targets.base import BaseTargetAdapter

logger = logging.getLogger(__name__)

TYPE_MAP = {
    # Integer types
    "integer": "INT",
    "int": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "TINYINT",
    "serial": "INT IDENTITY(1,1)",
    # Numeric types
    "numeric": "DECIMAL",
    "decimal": "DECIMAL",
    "real": "REAL",
    "double precision": "FLOAT",
    "double": "FLOAT",
    "float": "FLOAT",
    # String types
    "character varying": "NVARCHAR",
    "varchar": "NVARCHAR",
    "varchar2": "NVARCHAR",
    "nvarchar": "NVARCHAR",
    "nvarchar2": "NVARCHAR",
    "character": "NCHAR",
    "char": "NCHAR",
    "nchar": "NCHAR",
    "text": "NVARCHAR(MAX)",
    "clob": "NVARCHAR(MAX)",
    "nclob": "NVARCHAR(MAX)",
    "long": "NVARCHAR(MAX)",
    # Boolean — SQL Server uses BIT
    "boolean": "BIT",
    "bool": "BIT",
    # Date / Time
    "date": "DATE",
    "timestamp without time zone": "DATETIME2",
    "timestamp with time zone": "DATETIMEOFFSET",
    "timestamp": "DATETIME2",
    "timestamptz": "DATETIMEOFFSET",
    "time": "TIME",
    "time with time zone": "TIME",
    "interval": "NVARCHAR(50)",
    # Binary
    "bytea": "VARBINARY(MAX)",
    "blob": "VARBINARY(MAX)",
    "raw": "VARBINARY(MAX)",
    "long raw": "VARBINARY(MAX)",
    # UUID / JSON
    "uuid": "UNIQUEIDENTIFIER",
    "json": "NVARCHAR(MAX)",
    "jsonb": "NVARCHAR(MAX)",
    # Oracle / mainframe legacy types
    "number": "DECIMAL",
    "string": "NVARCHAR",
    "binary_float": "REAL",
    "binary_double": "FLOAT",
    "rowid": "NVARCHAR(18)",
}

FUNCTION_MAP = {
    "nvl":       lambda args: f"ISNULL({', '.join(args)})",
    "nvl2":      lambda args: f"IIF({args[0]} IS NOT NULL, {args[1]}, {args[2]})" if len(args) >= 3 else f"ISNULL({', '.join(args)})",
    "coalesce":  lambda args: f"COALESCE({', '.join(args)})",
    "sysdate":   lambda args: "GETDATE()",
    "systimestamp": lambda args: "SYSDATETIMEOFFSET()",
    "getdate":   lambda args: "GETDATE()",
    "now":       lambda args: "GETDATE()",
    "decode":    lambda args: _decode_to_case(args),
    "to_date":   lambda args: f"CAST({args[0]} AS DATE)" if args else "GETDATE()",
    "to_char":   lambda args: f"FORMAT({', '.join(args)})" if args else "''",
    "to_number": lambda args: f"CAST({args[0]} AS DECIMAL)" if args else "0",
    "instr":     lambda args: f"CHARINDEX({args[1]}, {args[0]})" if len(args) >= 2 else "0",
    "substr":    lambda args: f"SUBSTRING({', '.join(args)})",
    "length":    lambda args: f"LEN({args[0]})" if args else "LEN('')",
    "concat":    lambda args: " + ".join(args) if args else "''",
    "ifnull":    lambda args: f"ISNULL({', '.join(args)})",
    "isnull":    lambda args: f"ISNULL({', '.join(args)})",
    "charindex": lambda args: f"CHARINDEX({', '.join(args)})",
    "dateadd":   lambda args: f"DATEADD({', '.join(args)})" if len(args) >= 3 else "GETDATE()",
    "datediff":  lambda args: f"DATEDIFF({', '.join(args)})" if len(args) >= 3 else "0",
    "user":      lambda args: "SUSER_SNAME()",
    "rownum":    lambda args: "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))",
}


def _decode_to_case(args: list) -> str:
    if len(args) < 3:
        return f"/* DECODE with insufficient args: {', '.join(args)} */"
    expr = args[0]
    pairs = args[1:]
    parts = [f"CASE {expr}"]
    i = 0
    while i < len(pairs) - 1:
        parts.append(f"    WHEN {pairs[i]} THEN {pairs[i + 1]}")
        i += 2
    if i < len(pairs):
        parts.append(f"    ELSE {pairs[i]}")
    parts.append("END")
    return "\n".join(parts)


class SqlServerTargetAdapter(BaseTargetAdapter):
    """Microsoft SQL Server / Azure SQL Database target platform adapter."""

    def __init__(self, is_azure: bool = False):
        self._is_azure = is_azure

    def dialect_name(self) -> str:
        return "azuresql" if self._is_azure else "sqlserver"

    def map_type(self, source_type: str, profiling_stats: Optional[dict] = None) -> str:
        raw = source_type.strip()
        raw = re.split(r'\s+(?:PRIMARY|NOT|DEFAULT|UNIQUE|CHECK|REFERENCES|GENERATED)\b', raw, flags=re.IGNORECASE)[0].strip()
        base = raw.lower().split("(")[0].strip()
        size_match = re.search(r"\(([^)]+)\)", raw)
        size_qualifier = size_match.group(1) if size_match else None

        ss_type = TYPE_MAP.get(base)
        if ss_type is None:
            logger.warning(f"Unmapped source type '{source_type}', defaulting to NVARCHAR")
            ss_type = "NVARCHAR"

        if size_qualifier and ss_type in ("NVARCHAR", "NCHAR", "DECIMAL", "VARBINARY"):
            return f"{ss_type}({size_qualifier})"

        # SQL Server NVARCHAR requires a size
        if ss_type == "NVARCHAR" and not size_qualifier:
            return "NVARCHAR(4000)"

        return ss_type

    def render_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_key: str,
        foreign_keys: List[Dict] = None,
        comment: str = "",
    ) -> str:
        foreign_keys = foreign_keys or []
        _target = "Azure SQL" if self._is_azure else "SQL Server"
        lines = [
            f"-- Generated by DM Target Adapter ({_target})",
            f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
            "",
            f"CREATE TABLE [{table_name}] (",
        ]

        col_defs = []
        for col in columns:
            name = col["name"]
            data_type = col["data_type"]
            nullable = col.get("nullable", True)
            constraints = col.get("constraints", [])

            parts = [f"    [{name}] {data_type}"]
            for c in constraints:
                if not c.startswith("REFERENCES"):
                    parts.append(f" {c}")
            if not nullable and "NOT NULL" not in constraints and "PRIMARY KEY" not in constraints:
                parts.append(" NOT NULL")

            col_comment = col.get("comment", "")
            if col_comment:
                parts.append(f"  -- {col_comment}")
            col_defs.append("".join(parts))

        # FK constraints
        for fk in foreign_keys:
            col_defs.append(
                f"    CONSTRAINT [fk_{table_name}_{fk['column']}] "
                f"FOREIGN KEY ([{fk['column']}]) REFERENCES {fk['references']}"
            )

        lines.append(",\n".join(col_defs))
        lines.append(");")

        # Indexes on FK columns
        for fk in foreign_keys:
            lines.append(
                f"\nCREATE INDEX [idx_{table_name}_{fk['column']}] "
                f"ON [{table_name}]([{fk['column']}]);"
            )

        # Comments via sp_addextendedproperty
        if comment:
            escaped = comment.replace("'", "''")
            lines.append(
                f"\nEXEC sp_addextendedproperty 'MS_Description', '{escaped}', "
                f"'SCHEMA', 'dbo', 'TABLE', '{table_name}';"
            )

        lines.append("")
        return "\n".join(lines)

    def render_insert_select(
        self,
        target_table: str,
        source_table: str,
        column_mappings: List[Dict],
    ) -> str:
        if not column_mappings:
            return f"-- No column mappings for {target_table}\n"
        target_cols = [m["target_col"] for m in column_mappings]
        source_exprs = [m["source_expr"] for m in column_mappings]
        cols_str = ",\n    ".join(f"[{c}]" for c in target_cols)
        exprs_str = ",\n    ".join(source_exprs)
        lines = [
            f"-- Migration: {source_table} -> {target_table}",
            "",
            f"INSERT INTO [{target_table}] (",
            f"    {cols_str}",
            f")",
            f"SELECT",
            f"    {exprs_str}",
            f"FROM [{source_table}];",
            "",
        ]
        return "\n".join(lines)

    def translate_function(self, func_name: str, args: list) -> str:
        key = func_name.lower().strip()
        translator = FUNCTION_MAP.get(key)
        if translator:
            return translator(args)
        args_str = ", ".join(args) if args else ""
        return f"{func_name}({args_str})"

    def supports_serial(self) -> bool:
        return True  # IDENTITY columns

    def supports_check_constraints(self) -> bool:
        return True
