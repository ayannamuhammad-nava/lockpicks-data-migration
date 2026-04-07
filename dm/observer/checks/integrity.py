"""
Foreign Key Integrity Check

Re-runs referential integrity checks from project configuration
to detect newly introduced orphan records.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def check_fk_integrity(modern_conn: Any, table: str, config: dict) -> dict:
    """Re-run referential integrity checks for a table using config-defined relationships.

    Supports two configuration formats:
        Format 1: {child_table, parent_table, fk_column, pk_column}
        Format 2: {child: "table.col", parent: "table.col"}

    Args:
        modern_conn: A BaseConnector instance connected to the modern DB.
        table: The table name to check (used to filter relevant FK rules).
        config: Full project configuration dict containing
                validation.referential_integrity rules.

    Returns:
        Dict with keys:
            violations: int — Total orphan count across all FK checks.
            details: list[dict] — Per-check results with relationship info.
    """
    # Extract referential integrity config
    ri_config = config.get("validation", config).get("referential_integrity", {})

    if isinstance(ri_config, dict):
        fk_checks = ri_config.get(table, [])
    elif isinstance(ri_config, list):
        fk_checks = ri_config
    else:
        fk_checks = []

    # Filter to checks relevant to this table (as child or parent)
    relevant_checks: List[Dict] = []
    for fk in fk_checks:
        if "child" in fk and "." in str(fk.get("child", "")):
            child_table = fk["child"].split(".")[0]
            parent_table = fk["parent"].split(".")[0]
        else:
            child_table = fk.get("child_table", "")
            parent_table = fk.get("parent_table", "")

        if table in (child_table, parent_table):
            relevant_checks.append(fk)

    if not relevant_checks:
        return {
            "violations": 0,
            "details": [],
            "note": f"No FK checks configured for '{table}'",
        }

    total_violations = 0
    details: List[Dict] = []

    for fk in relevant_checks:
        # Parse the FK definition (same dual-format as referential validator)
        if "child" in fk and "." in str(fk.get("child", "")):
            child_parts = fk["child"].split(".")
            parent_parts = fk["parent"].split(".")
            child_table, fk_column = child_parts[0], child_parts[1]
            parent_table, pk_column = parent_parts[0], parent_parts[1]
        else:
            child_table = fk.get("child_table", "")
            parent_table = fk.get("parent_table", "")
            fk_column = fk.get("fk_column", "")
            pk_column = fk.get("pk_column", fk_column)

        check_label = f"{child_table}.{fk_column} -> {parent_table}.{pk_column}"

        try:
            result = modern_conn.check_referential_integrity(
                child_table=child_table,
                parent_table=parent_table,
                fk_column=fk_column,
                pk_column=pk_column,
            )
            orphan_count = result.get("orphan_count", 0)
            total_violations += orphan_count

            details.append({
                "check": check_label,
                "orphan_count": orphan_count,
                "orphan_sample": result.get("orphan_sample", []),
            })

            if orphan_count > 0:
                logger.warning(
                    f"FK integrity violation: {check_label} "
                    f"({orphan_count} orphan(s))"
                )
        except Exception as e:
            logger.error(f"FK integrity check failed for {check_label}: {e}")
            details.append({
                "check": check_label,
                "orphan_count": -1,
                "error": str(e),
            })

    return {
        "violations": total_violations,
        "details": details,
    }
