"""
Schema introspection and comparison utilities.

Works with any BaseConnector — database-agnostic.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def introspect_schema(conn: Any, table_name: str) -> Dict[str, str]:
    """Introspect a table schema via the connector.

    Args:
        conn: A BaseConnector instance.
        table_name: Table to introspect.

    Returns:
        Dict mapping column_name -> data_type.
    """
    schema_info = conn.get_table_schema(table_name)
    schema_dict = {row["column_name"]: row["data_type"] for row in schema_info}
    logger.info(f"Introspected schema for {table_name}: {len(schema_dict)} columns")
    return schema_dict


def compare_schemas(legacy_schema: Dict, modern_schema: Dict) -> Dict[str, List]:
    """Compare two schemas and identify differences.

    Returns:
        Dict with keys: missing_in_modern, missing_in_legacy,
        type_mismatches, common_columns.
    """
    legacy_cols = set(legacy_schema.keys())
    modern_cols = set(modern_schema.keys())

    missing_in_modern = sorted(legacy_cols - modern_cols)
    missing_in_legacy = sorted(modern_cols - legacy_cols)
    common_columns = sorted(legacy_cols & modern_cols)

    type_mismatches = []
    for col in common_columns:
        if legacy_schema[col] != modern_schema[col]:
            type_mismatches.append({
                "column": col,
                "legacy_type": legacy_schema[col],
                "modern_type": modern_schema[col],
            })

    return {
        "missing_in_modern": missing_in_modern,
        "missing_in_legacy": missing_in_legacy,
        "type_mismatches": type_mismatches,
        "common_columns": common_columns,
    }


def generate_schema_diff_report(
    legacy_schema: Dict, modern_schema: Dict, table_name: str
) -> str:
    """Generate a markdown report of schema differences."""
    diff = compare_schemas(legacy_schema, modern_schema)

    report = f"# Schema Diff Report: {table_name}\n\n"

    if diff["missing_in_modern"]:
        report += "## Columns Missing in Modern System\n\n"
        for col in diff["missing_in_modern"]:
            report += f"- **{col}** ({legacy_schema[col]})\n"
        report += "\n"

    if diff["missing_in_legacy"]:
        report += "## New Columns in Modern System\n\n"
        for col in diff["missing_in_legacy"]:
            report += f"- **{col}** ({modern_schema[col]})\n"
        report += "\n"

    if diff["type_mismatches"]:
        report += "## Type Mismatches\n\n"
        report += "| Column | Legacy Type | Modern Type |\n"
        report += "|--------|-------------|-------------|\n"
        for m in diff["type_mismatches"]:
            report += f"| {m['column']} | {m['legacy_type']} | {m['modern_type']} |\n"
        report += "\n"

    report += "## Summary\n\n"
    report += f"- Common columns: {len(diff['common_columns'])}\n"
    report += f"- Missing in modern: {len(diff['missing_in_modern'])}\n"
    report += f"- New in modern: {len(diff['missing_in_legacy'])}\n"
    report += f"- Type mismatches: {len(diff['type_mismatches'])}\n"

    return report
