"""
Schema loading and comparison utilities for Pandera schemas and database introspection.
"""
import importlib
import psycopg2
from typing import Dict, List, Tuple
import logging
from tools.db_utils import get_table_schema

logger = logging.getLogger(__name__)


def load_pandera_schema(system: str, table_name: str):
    """
    Dynamically load a Pandera schema class.

    Args:
        system: 'legacy' or 'modern'
        table_name: Name of the table (e.g., 'claimants', 'employers')

    Returns:
        Pandera DataFrameSchema object
    """
    module_path = f"schemas.{system}.{table_name}"
    try:
        module = importlib.import_module(module_path)
        schema_class_name = f"{table_name.capitalize()}Schema"
        schema = getattr(module, schema_class_name)
        logger.info(f"Loaded Pandera schema: {system}.{table_name}")
        return schema
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to load schema {module_path}: {e}")
        return None


def introspect_database_schema(conn: psycopg2.extensions.connection, table_name: str) -> Dict:
    """
    Introspect database schema using information_schema.

    Args:
        conn: Database connection
        table_name: Name of the table

    Returns:
        Dict mapping column_name -> data_type
    """
    schema_info = get_table_schema(conn, table_name)
    schema_dict = {row['column_name']: row['data_type'] for row in schema_info}
    logger.info(f"Introspected schema for {table_name}: {len(schema_dict)} columns")
    return schema_dict


def compare_schemas(
    legacy_schema: Dict,
    modern_schema: Dict
) -> Dict[str, List]:
    """
    Compare two schemas and identify differences.

    Args:
        legacy_schema: Dict of column_name -> data_type for legacy system
        modern_schema: Dict of column_name -> data_type for modern system

    Returns:
        Dict with keys:
            - missing_in_modern: Columns in legacy but not in modern
            - missing_in_legacy: Columns in modern but not in legacy
            - type_mismatches: Columns with different data types
            - common_columns: Columns present in both
    """
    legacy_cols = set(legacy_schema.keys())
    modern_cols = set(modern_schema.keys())

    missing_in_modern = list(legacy_cols - modern_cols)
    missing_in_legacy = list(modern_cols - legacy_cols)
    common_columns = list(legacy_cols & modern_cols)

    type_mismatches = []
    for col in common_columns:
        if legacy_schema[col] != modern_schema[col]:
            type_mismatches.append({
                'column': col,
                'legacy_type': legacy_schema[col],
                'modern_type': modern_schema[col]
            })

    result = {
        'missing_in_modern': missing_in_modern,
        'missing_in_legacy': missing_in_legacy,
        'type_mismatches': type_mismatches,
        'common_columns': common_columns
    }

    logger.info(f"Schema comparison: {len(missing_in_modern)} missing in modern, "
                f"{len(type_mismatches)} type mismatches")

    return result


def generate_schema_diff_report(
    legacy_schema: Dict,
    modern_schema: Dict,
    table_name: str
) -> str:
    """
    Generate a markdown report of schema differences.

    Args:
        legacy_schema: Legacy schema dict
        modern_schema: Modern schema dict
        table_name: Name of the table

    Returns:
        Markdown formatted string
    """
    diff = compare_schemas(legacy_schema, modern_schema)

    report = f"# Schema Diff Report: {table_name}\n\n"

    # Missing columns
    if diff['missing_in_modern']:
        report += "## Columns Missing in Modern System\n\n"
        for col in diff['missing_in_modern']:
            report += f"- **{col}** ({legacy_schema[col]})\n"
        report += "\n"

    if diff['missing_in_legacy']:
        report += "## New Columns in Modern System\n\n"
        for col in diff['missing_in_legacy']:
            report += f"- **{col}** ({modern_schema[col]})\n"
        report += "\n"

    # Type mismatches
    if diff['type_mismatches']:
        report += "## Type Mismatches\n\n"
        report += "| Column | Legacy Type | Modern Type |\n"
        report += "|--------|-------------|-------------|\n"
        for mismatch in diff['type_mismatches']:
            report += f"| {mismatch['column']} | {mismatch['legacy_type']} | {mismatch['modern_type']} |\n"
        report += "\n"

    # Summary
    report += "## Summary\n\n"
    report += f"- Common columns: {len(diff['common_columns'])}\n"
    report += f"- Missing in modern: {len(diff['missing_in_modern'])}\n"
    report += f"- New in modern: {len(diff['missing_in_legacy'])}\n"
    report += f"- Type mismatches: {len(diff['type_mismatches'])}\n"

    return report


def extract_pandera_schema_info(schema) -> Dict:
    """
    Extract schema information from a Pandera DataFrameSchema object.

    Args:
        schema: Pandera DataFrameSchema

    Returns:
        Dict mapping column_name -> type info
    """
    schema_info = {}

    if hasattr(schema, 'columns'):
        for col_name, col_schema in schema.columns.items():
            dtype = str(col_schema.dtype) if hasattr(col_schema, 'dtype') else 'unknown'
            nullable = col_schema.nullable if hasattr(col_schema, 'nullable') else True

            schema_info[col_name] = {
                'dtype': dtype,
                'nullable': nullable
            }

    return schema_info


def map_pandera_to_sql_type(pandera_type: str) -> str:
    """
    Map Pandera data types to SQL data types.

    Args:
        pandera_type: Pandera type string (e.g., 'int64', 'object')

    Returns:
        Approximate SQL type
    """
    type_mapping = {
        'int64': 'bigint',
        'int32': 'integer',
        'float64': 'double precision',
        'float32': 'real',
        'object': 'text',
        'string': 'text',
        'bool': 'boolean',
        'datetime64[ns]': 'timestamp'
    }

    return type_mapping.get(pandera_type, 'unknown')


def sql_type_to_pandera_type(sql_type: str) -> str:
    """
    Map SQL data types to Pandera Column types.

    Args:
        sql_type: SQL data type from information_schema

    Returns:
        Pandera type string
    """
    type_mapping = {
        'integer': 'int',
        'bigint': 'int',
        'smallint': 'int',
        'numeric': 'float',
        'decimal': 'float',
        'double precision': 'float',
        'real': 'float',
        'character varying': 'str',
        'varchar': 'str',
        'character': 'str',
        'char': 'str',
        'text': 'str',
        'boolean': 'bool',
        'timestamp without time zone': "'datetime64[ns]'",
        'timestamp with time zone': "'datetime64[ns]'",
        'date': "'datetime64[ns]'",
        'time': 'str',
        'json': 'str',
        'jsonb': 'str',
        'uuid': 'str'
    }

    return type_mapping.get(sql_type.lower(), 'str')


def generate_pandera_schema(
    conn: psycopg2.extensions.connection,
    table_name: str,
    system_name: str = 'generated',
    output_path: str = None,
    assume_unique_id: bool = True,
    id_column: str = None
) -> str:
    """
    Auto-generate a Pandera schema class from database table schema.

    Args:
        conn: Database connection
        table_name: Name of the table to introspect
        system_name: System name (legacy/modern) for the schema file
        output_path: Optional path to save the generated schema file
        assume_unique_id: If True, assumes first column ending in '_id' is unique
        id_column: Explicit ID column name (overrides assume_unique_id)

    Returns:
        Generated Python code as string

    Example:
        >>> conn = get_connection('legacy')
        >>> schema_code = generate_pandera_schema(conn, 'claimants', 'legacy')
        >>> print(schema_code)
        # Auto-generated Pandera schema
        import pandera as pa
        from pandera import Column, DataFrameSchema

        ClaimantsSchema = DataFrameSchema({
            "cl_recid": Column(int, nullable=True, unique=True, coerce=True),
            "cl_fnam": Column(str, nullable=True),
            ...
        })
    """
    from pathlib import Path

    # Get schema info from database
    schema_info = get_table_schema(conn, table_name)

    if not schema_info:
        raise ValueError(f"Table '{table_name}' not found or has no columns")

    # Detect unique ID column
    unique_column = id_column
    if not unique_column and assume_unique_id:
        # Find first column ending with _id
        for col in schema_info:
            if col['column_name'].endswith('_id'):
                unique_column = col['column_name']
                break

    # Generate imports
    code = '"""\n'
    code += f'Auto-generated Pandera schema for {table_name}\n'
    code += f'Generated from database: {system_name}\n'
    code += '"""\n'
    code += 'import pandera as pa\n'
    code += 'from pandera import Column, DataFrameSchema\n\n\n'

    # Generate schema class
    class_name = f"{table_name.capitalize()}Schema"
    code += f'{class_name} = DataFrameSchema({{\n'

    # Generate column definitions
    for col_info in schema_info:
        col_name = col_info['column_name']
        sql_type = col_info['data_type']
        is_nullable = col_info['is_nullable'] == 'YES'

        # Map SQL type to Pandera type
        pandera_type = sql_type_to_pandera_type(sql_type)

        # Build column definition
        col_def = f'    "{col_name}": Column({pandera_type}'
        col_def += f', nullable={is_nullable}'

        # Add unique constraint for ID column
        if col_name == unique_column:
            col_def += ', unique=True'

        # Add coerce for type conversion
        col_def += ', coerce=True'

        col_def += '),'
        code += col_def + '\n'

    code += '}, strict=False, coerce=True)\n'

    # Save to file if output path provided
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(code)
        logger.info(f"Generated Pandera schema saved to: {output_file}")

    return code


def generate_schemas_for_database(
    conn: psycopg2.extensions.connection,
    system_name: str,
    table_names: List[str],
    output_dir: str = 'schemas'
) -> Dict[str, str]:
    """
    Generate Pandera schemas for multiple tables in a database.

    Args:
        conn: Database connection
        system_name: System name (e.g., 'legacy', 'modern')
        table_names: List of table names to generate schemas for
        output_dir: Base directory for schemas (default: 'schemas')

    Returns:
        Dict mapping table_name -> generated code

    Example:
        >>> conn = get_connection('legacy')
        >>> schemas = generate_schemas_for_database(
        ...     conn,
        ...     'legacy',
        ...     ['claimants', 'employers', 'claims']
        ... )
        >>> # Generates:
        >>> # schemas/legacy/claimants.py
        >>> # schemas/legacy/employers.py
        >>> # schemas/legacy/products.py
    """
    from pathlib import Path

    results = {}

    for table_name in table_names:
        try:
            # Generate schema code
            output_path = Path(output_dir) / system_name / f"{table_name}.py"
            code = generate_pandera_schema(
                conn,
                table_name,
                system_name,
                output_path=str(output_path)
            )
            results[table_name] = code
            logger.info(f"Generated schema for {system_name}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to generate schema for {table_name}: {e}")
            results[table_name] = None

    # Create __init__.py for the package
    init_file = Path(output_dir) / system_name / '__init__.py'
    init_file.parent.mkdir(parents=True, exist_ok=True)
    with open(init_file, 'w') as f:
        f.write(f'"""\nPandera schemas for {system_name} system\n"""\n')

    return results
