"""
SQL Object Parser — Stored Procedures, Views, and Triggers

Parses SQL files to extract:
  - Tables referenced (FROM, JOIN, INSERT, UPDATE, DELETE)
  - Fields/columns used (SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY)
  - Input parameters (procedure arguments)
  - Business logic (WHERE conditions, CASE statements)
  - Dependencies between objects

Uses sqlglot for dialect-aware parsing with regex fallback.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class SqlObject:
    """A parsed SQL object (stored procedure, view, or trigger)."""
    name: str
    object_type: str  # procedure, view, trigger, script
    source_file: str
    source_sql: str
    tables_referenced: List[str] = field(default_factory=list)
    columns_referenced: List[str] = field(default_factory=list)
    parameters: List[Dict] = field(default_factory=list)  # [{name, type, direction}]
    joins: List[Dict] = field(default_factory=list)  # [{left_table, right_table, condition}]
    conditions: List[str] = field(default_factory=list)  # WHERE/HAVING clauses
    output_columns: List[str] = field(default_factory=list)  # SELECT list
    dependencies: List[str] = field(default_factory=list)  # other objects called

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "object_type": self.object_type,
            "source_file": self.source_file,
            "tables_referenced": self.tables_referenced,
            "columns_referenced": sorted(set(self.columns_referenced)),
            "parameters": self.parameters,
            "joins": self.joins,
            "conditions": self.conditions,
            "output_columns": self.output_columns,
            "dependencies": self.dependencies,
            "summary": {
                "table_count": len(self.tables_referenced),
                "column_count": len(set(self.columns_referenced)),
                "join_count": len(self.joins),
                "condition_count": len(self.conditions),
                "param_count": len(self.parameters),
            },
        }


def parse_sql_file(source: str, name: Optional[str] = None) -> List[SqlObject]:
    """Parse a SQL file and extract all objects (procedures, views, scripts).

    Args:
        source: File path to a .sql file.
        name: Optional name override.

    Returns:
        List of SqlObject instances found in the file.
    """
    source_path = Path(source)
    if not source_path.exists():
        return []

    text = source_path.read_text(encoding="utf-8", errors="replace")
    file_name = name or source_path.stem.upper()

    objects = []

    # Try to detect object type from content
    upper = text.upper()

    if "CREATE PROCEDURE" in upper or "CREATE OR REPLACE PROCEDURE" in upper:
        obj = _parse_procedure(text, file_name, str(source_path))
        if obj:
            objects.append(obj)
    elif "CREATE VIEW" in upper or "CREATE OR REPLACE VIEW" in upper:
        obj = _parse_view(text, file_name, str(source_path))
        if obj:
            objects.append(obj)
    elif "CREATE TRIGGER" in upper or "CREATE OR REPLACE TRIGGER" in upper:
        obj = _parse_trigger(text, file_name, str(source_path))
        if obj:
            objects.append(obj)
    else:
        # Generic SQL script — extract tables and columns
        obj = _parse_script(text, file_name, str(source_path))
        if obj:
            objects.append(obj)

    return objects


def _parse_procedure(text: str, name: str, source_file: str) -> Optional[SqlObject]:
    """Parse a stored procedure."""
    obj = SqlObject(name=name, object_type="procedure", source_file=source_file, source_sql=text)

    # Extract procedure name
    proc_match = re.search(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([\w.]+)',
        text, re.IGNORECASE
    )
    if proc_match:
        obj.name = proc_match.group(1).split(".")[-1].upper()

    # Extract parameters
    param_section = re.search(r'\((.*?)\)\s*(?:AS|IS|BEGIN|LANGUAGE)', text, re.IGNORECASE | re.DOTALL)
    if param_section:
        _extract_parameters(param_section.group(1), obj)

    _extract_sql_elements(text, obj)
    return obj


def _parse_view(text: str, name: str, source_file: str) -> Optional[SqlObject]:
    """Parse a view definition."""
    obj = SqlObject(name=name, object_type="view", source_file=source_file, source_sql=text)

    # Extract view name
    view_match = re.search(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w.]+)',
        text, re.IGNORECASE
    )
    if view_match:
        obj.name = view_match.group(1).split(".")[-1].upper()

    _extract_sql_elements(text, obj)
    return obj


def _parse_trigger(text: str, name: str, source_file: str) -> Optional[SqlObject]:
    """Parse a trigger definition."""
    obj = SqlObject(name=name, object_type="trigger", source_file=source_file, source_sql=text)

    trigger_match = re.search(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+([\w.]+)',
        text, re.IGNORECASE
    )
    if trigger_match:
        obj.name = trigger_match.group(1).split(".")[-1].upper()

    _extract_sql_elements(text, obj)
    return obj


def _parse_script(text: str, name: str, source_file: str) -> Optional[SqlObject]:
    """Parse a generic SQL script."""
    obj = SqlObject(name=name, object_type="script", source_file=source_file, source_sql=text)
    _extract_sql_elements(text, obj)

    if not obj.tables_referenced and not obj.columns_referenced:
        return None
    return obj


def _extract_parameters(param_text: str, obj: SqlObject):
    """Extract procedure parameters from the parameter list."""
    for param in param_text.split(","):
        param = param.strip()
        if not param:
            continue
        parts = param.split()
        if len(parts) >= 2:
            direction = "IN"
            name_idx = 0
            if parts[0].upper() in ("IN", "OUT", "INOUT", "IN OUT"):
                direction = parts[0].upper()
                name_idx = 1
            if name_idx < len(parts):
                p_name = parts[name_idx].strip("@")
                p_type = " ".join(parts[name_idx + 1:]) if name_idx + 1 < len(parts) else "unknown"
                obj.parameters.append({
                    "name": p_name,
                    "type": p_type.rstrip(",").strip(),
                    "direction": direction,
                })


def _extract_sql_elements(text: str, obj: SqlObject):
    """Extract tables, columns, joins, and conditions from SQL text."""

    # Try sqlglot first
    try:
        import sqlglot
        _extract_with_sqlglot(text, obj)
        return
    except Exception:
        pass

    # Fallback: regex extraction
    _extract_with_regex(text, obj)


def _extract_with_sqlglot(text: str, obj: SqlObject):
    """Use sqlglot AST to extract SQL elements."""
    import sqlglot
    from sqlglot import exp

    tables = set()
    columns = set()
    output_cols = []

    for stmt in sqlglot.parse(text, error_level=sqlglot.ErrorLevel.IGNORE):
        if stmt is None:
            continue

        # Extract tables
        for table in stmt.find_all(exp.Table):
            tname = table.name
            if tname:
                tables.add(tname.upper())

        # Extract columns
        for col in stmt.find_all(exp.Column):
            cname = col.name
            if cname:
                columns.add(cname.upper())
                tbl = col.table
                if tbl:
                    columns.add(f"{tbl.upper()}.{cname.upper()}")

        # Extract SELECT output columns
        if isinstance(stmt, exp.Select):
            for sel_col in stmt.expressions:
                if isinstance(sel_col, exp.Column):
                    output_cols.append(sel_col.name.upper() if sel_col.name else str(sel_col))
                elif hasattr(sel_col, "alias"):
                    output_cols.append(sel_col.alias if sel_col.alias else str(sel_col))

        # Extract WHERE conditions
        where = stmt.find(exp.Where)
        if where:
            obj.conditions.append(str(where.this))

        # Extract JOINs
        for join in stmt.find_all(exp.Join):
            _jtable = join.find(exp.Table)
            _jon = join.find(exp.On)
            if _jtable:
                obj.joins.append({
                    "table": _jtable.name.upper() if _jtable.name else "",
                    "condition": str(_jon.this) if _jon else "",
                })

    obj.tables_referenced = sorted(tables)
    obj.columns_referenced = sorted(columns)
    obj.output_columns = output_cols


def _extract_with_regex(text: str, obj: SqlObject):
    """Fallback regex extraction when sqlglot can't parse."""
    upper = text.upper()

    # Tables: FROM, JOIN, INTO, UPDATE, DELETE FROM
    table_patterns = [
        r'FROM\s+([\w.]+)',
        r'JOIN\s+([\w.]+)',
        r'INTO\s+([\w.]+)',
        r'UPDATE\s+([\w.]+)',
        r'DELETE\s+FROM\s+([\w.]+)',
    ]
    tables = set()
    for pattern in table_patterns:
        for match in re.finditer(pattern, upper):
            tname = match.group(1).split(".")[-1]
            if tname not in ("SELECT", "SET", "VALUES", "WHERE", "AND", "OR", "ON"):
                tables.add(tname)

    # Columns: harder with regex, get what we can from SELECT and WHERE
    col_pattern = r'(?:SELECT|WHERE|AND|OR|ON|BY|SET)\s+([\w.]+(?:\s*,\s*[\w.]+)*)'
    columns = set()
    for match in re.finditer(col_pattern, upper):
        for col in match.group(1).split(","):
            col = col.strip().split(".")[-1]
            if col and col not in ("FROM", "WHERE", "AND", "OR", "SELECT", "*"):
                columns.add(col)

    # WHERE conditions
    where_matches = re.findall(r'WHERE\s+(.+?)(?:ORDER|GROUP|HAVING|LIMIT|;|\Z)', upper, re.DOTALL)
    for w in where_matches:
        obj.conditions.append(w.strip()[:200])

    obj.tables_referenced = sorted(tables)
    obj.columns_referenced = sorted(columns)


def rewrite_sql(
    original_sql: str,
    column_mappings: Dict[str, str],
    table_mappings: Dict[str, str],
    normalization_plan: Dict,
    target_dialect: str = "postgres",
) -> Dict:
    """Rewrite SQL using column/table mappings and normalization plan.

    Args:
        original_sql: The original SQL text.
        column_mappings: {old_col: new_col} mapping.
        table_mappings: {old_table: new_table} mapping.
        normalization_plan: The normalization plan dict.
        target_dialect: Target SQL dialect.

    Returns:
        Dict with 'rewritten_sql', 'changes', 'notes'.
    """
    rewritten = original_sql
    changes = []
    notes = []

    # Step 1: Replace table names
    for old_table, new_table in table_mappings.items():
        pattern = re.compile(r'\b' + re.escape(old_table) + r'\b', re.IGNORECASE)
        if pattern.search(rewritten):
            rewritten = pattern.sub(new_table, rewritten)
            changes.append(f"Table: {old_table} → {new_table}")

    # Step 2: Replace column names
    for old_col, new_col in column_mappings.items():
        pattern = re.compile(r'\b' + re.escape(old_col) + r'\b', re.IGNORECASE)
        if pattern.search(rewritten):
            rewritten = pattern.sub(new_col, rewritten)
            changes.append(f"Column: {old_col} → {new_col}")

    # Step 3: Check if normalization requires JOINs
    for source_table, plan_data in normalization_plan.items():
        if not isinstance(plan_data, dict):
            continue
        entities = plan_data.get("entities", [])
        children = [e for e in entities if e.get("role") == "child"]

        if children:
            # Check if the rewritten SQL references columns from child tables
            for child in children:
                child_cols = child.get("columns", [])
                for col in child_cols:
                    if re.search(r'\b' + re.escape(col) + r'\b', rewritten, re.IGNORECASE):
                        child_name = child["name"]
                        notes.append(
                            f"Column `{col}` was normalized into `{child_name}`. "
                            f"You may need to add: JOIN {child_name} ON {child_name}.{source_table}_id = {source_table}.{source_table}_id"
                        )
                        break

    # Step 4: Dialect-specific adjustments
    if target_dialect in ("sqlserver", "azuresql"):
        # Bracket quoting
        notes.append("SQL Server uses [bracket] quoting for identifiers with special characters")
        # ISNULL instead of COALESCE for simple cases
        rewritten = re.sub(r'\bNVL\b', 'ISNULL', rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r'\bSYSDATE\b', 'GETDATE()', rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r'\bNOW\(\)', 'GETDATE()', rewritten, flags=re.IGNORECASE)
    elif target_dialect == "oracle":
        rewritten = re.sub(r'\bCOALESCE\b', 'NVL', rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r'\bNOW\(\)', 'SYSDATE', rewritten, flags=re.IGNORECASE)
    elif target_dialect == "snowflake":
        rewritten = re.sub(r'\bNOW\(\)', 'CURRENT_TIMESTAMP()', rewritten, flags=re.IGNORECASE)
        notes.append("Snowflake is case-insensitive by default but preserves case in double-quoted identifiers")

    # Step 5: Note potential differences
    if "GROUP BY" in original_sql.upper():
        notes.append("GROUP BY behavior may differ — verify aggregate results match between old and new")
    if "ORDER BY" in original_sql.upper():
        notes.append("ORDER BY with NULL handling may differ between platforms (NULLS FIRST/LAST)")
    if "DISTINCT" in original_sql.upper():
        notes.append("DISTINCT on normalized tables may produce different row counts if denormalized data had duplicates")
    if any(kw in original_sql.upper() for kw in ["ROWNUM", "TOP ", "LIMIT "]):
        notes.append("Row limiting syntax differs: ROWNUM (Oracle), TOP (SQL Server), LIMIT (PostgreSQL/Snowflake)")

    return {
        "rewritten_sql": rewritten,
        "changes": changes,
        "notes": notes,
        "change_count": len(changes),
    }


def scan_sql_files(repo_path: str) -> List[SqlObject]:
    """Scan a directory for SQL files and parse all of them.

    Args:
        repo_path: Path to scan for .sql, .prc, .sp, .vw, .trg files.

    Returns:
        List of SqlObject instances.
    """
    repo = Path(repo_path)
    objects = []

    for f in sorted(repo.rglob("*")):
        if f.suffix.lower() in (".sql", ".prc", ".sp", ".vw", ".trg") and f.is_file():
            try:
                parsed = parse_sql_file(str(f))
                objects.extend(parsed)
            except Exception as e:
                logger.warning(f"Failed to parse {f.name}: {e}")

    logger.info(f"Scanned {repo_path}: {len(objects)} SQL objects")
    return objects
