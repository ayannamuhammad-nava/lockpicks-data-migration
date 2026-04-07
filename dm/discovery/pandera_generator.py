"""
Pandera schema auto-generation and loading.

Generates Pandera DataFrameSchema classes from database table schemas
and loads them dynamically for validation.
"""

import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


SQL_TO_PANDERA = {
    "integer": "int", "bigint": "int", "smallint": "int",
    "numeric": "float", "decimal": "float", "double precision": "float", "real": "float",
    "character varying": "str", "varchar": "str", "character": "str", "char": "str", "text": "str",
    "boolean": "bool",
    "timestamp without time zone": "'datetime64[ns]'",
    "timestamp with time zone": "'datetime64[ns]'",
    "date": "'datetime64[ns]'",
    "time": "str", "json": "str", "jsonb": "str", "uuid": "str",
}


def sql_type_to_pandera_type(sql_type: str) -> str:
    return SQL_TO_PANDERA.get(sql_type.lower(), "str")


def generate_pandera_schema(
    conn: Any,
    table_name: str,
    system_name: str = "generated",
    output_path: Optional[str] = None,
) -> str:
    """Auto-generate a Pandera schema from a database table.

    Args:
        conn: BaseConnector instance.
        table_name: Table to introspect.
        system_name: 'legacy' or 'modern'.
        output_path: If provided, write generated code to this file.

    Returns:
        Generated Python source code as a string.
    """
    schema_info = conn.get_table_schema(table_name)
    if not schema_info:
        raise ValueError(f"Table '{table_name}' not found or has no columns")

    # Detect unique ID column
    unique_column = None
    for col in schema_info:
        if col["column_name"].endswith("_id"):
            unique_column = col["column_name"]
            break

    code = f'"""\nAuto-generated Pandera schema for {table_name}\nGenerated from: {system_name}\n"""\n'
    code += "import pandera as pa\nfrom pandera import Column, DataFrameSchema\n\n\n"

    class_name = f"{table_name.capitalize()}Schema"
    code += f"{class_name} = DataFrameSchema({{\n"

    for col_info in schema_info:
        col_name = col_info["column_name"]
        pandera_type = sql_type_to_pandera_type(col_info["data_type"])
        is_nullable = col_info["is_nullable"] == "YES"

        col_def = f'    "{col_name}": Column({pandera_type}, nullable={is_nullable}'
        if col_name == unique_column:
            col_def += ", unique=True"
        col_def += ", coerce=True),"
        code += col_def + "\n"

    code += "}, strict=False, coerce=True)\n"

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(code)
        logger.info(f"Generated Pandera schema: {output_file}")

    return code


def load_pandera_schema(system: str, table_name: str, config: Dict) -> Any:
    """Dynamically load a Pandera schema from the project's schemas directory.

    Args:
        system: 'legacy' or 'modern'.
        table_name: Table name (e.g. 'claimants').
        config: Project config (used to resolve project_dir).

    Returns:
        Pandera DataFrameSchema or None.
    """
    import sys

    project_dir = config.get("_project_dir", ".")
    schemas_dir = Path(project_dir) / "schemas"

    if str(schemas_dir) not in sys.path:
        sys.path.insert(0, str(schemas_dir))

    module_path = f"{system}.{table_name}"
    try:
        module = importlib.import_module(module_path)
        schema_class_name = f"{table_name.capitalize()}Schema"
        schema = getattr(module, schema_class_name)
        logger.info(f"Loaded Pandera schema: {system}.{table_name}")
        return schema
    except (ImportError, AttributeError) as e:
        logger.debug(f"No Pandera schema found for {module_path}: {e}")
        return None


def ensure_schemas_exist(legacy_conn, modern_conn, dataset: str, config: Dict) -> None:
    """Auto-generate Pandera schemas if they don't exist."""
    project_dir = config.get("_project_dir", ".")
    schema_dir = Path(project_dir) / "schemas"

    for system, conn in [("legacy", legacy_conn), ("modern", modern_conn)]:
        schema_file = schema_dir / system / f"{dataset}.py"
        if not schema_file.exists():
            logger.info(f"Generating schema for {system}.{dataset}...")
            try:
                generate_pandera_schema(conn, dataset, system, output_path=str(schema_file))
                # Create __init__.py
                init_file = schema_dir / system / "__init__.py"
                if not init_file.exists():
                    init_file.parent.mkdir(parents=True, exist_ok=True)
                    init_file.write_text(f'"""Pandera schemas for {system} system"""\n')
            except Exception as e:
                logger.warning(f"Could not generate {system} schema: {e}")
