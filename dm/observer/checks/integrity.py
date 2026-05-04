"""
Foreign Key Integrity Check

Re-runs referential integrity checks from project configuration
to detect newly introduced orphan records.

Supports cross-source checks where child and parent tables live
in different databases (e.g. claims_db and eligibility_db).
"""

import logging
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def _parse_fk_def(fk: Dict) -> Dict:
    """Normalize a FK check config into a standard dict."""
    if "child" in fk and "." in str(fk.get("child", "")):
        child_parts = fk["child"].split(".")
        parent_parts = fk["parent"].split(".")
        return {
            "child_table": child_parts[0],
            "fk_column": child_parts[1],
            "child_source": fk.get("child_source"),
            "parent_table": parent_parts[0],
            "pk_column": parent_parts[1],
            "parent_source": fk.get("parent_source"),
        }
    return {
        "child_table": fk.get("child_table", ""),
        "fk_column": fk.get("fk_column", ""),
        "child_source": fk.get("child_source"),
        "parent_table": fk.get("parent_table", ""),
        "pk_column": fk.get("pk_column", fk.get("fk_column", "")),
        "parent_source": fk.get("parent_source"),
    }


def _cross_source_check(
    child_conn: Any,
    parent_conn: Any,
    child_table: str,
    parent_table: str,
    fk_column: str,
    pk_column: str,
) -> Dict:
    """Check referential integrity across two different database connections."""
    child_df = child_conn.execute_query(
        f"SELECT DISTINCT {fk_column} FROM {child_table} WHERE {fk_column} IS NOT NULL"
    )
    child_fk_values: Set = set(child_df[fk_column].tolist()) if not child_df.empty else set()

    parent_df = parent_conn.execute_query(
        f"SELECT DISTINCT {pk_column} FROM {parent_table}"
    )
    parent_pk_values: Set = set(parent_df[pk_column].tolist()) if not parent_df.empty else set()

    orphans = child_fk_values - parent_pk_values
    return {
        "orphan_count": len(orphans),
        "orphan_sample": sorted(list(orphans))[:10],
        "cross_source": True,
    }


def check_fk_integrity(modern_conn: Any, table: str, config: dict) -> dict:
    """Re-run referential integrity checks for a table.

    Supports three configuration formats:
        Format 1: {child_table, parent_table, fk_column, pk_column}
        Format 2: {child: "table.col", parent: "table.col"}
        Format 3: Format 1 or 2 with child_source / parent_source for cross-DB checks

    Args:
        modern_conn: A BaseConnector instance connected to the modern DB.
        table: The table name to check (used to filter relevant FK rules).
        config: Full project configuration dict.

    Returns:
        Dict with violations count and per-check details.
    """
    ri_config = config.get("validation", config).get("referential_integrity", {})

    if isinstance(ri_config, dict):
        fk_checks = ri_config.get(table, [])
    elif isinstance(ri_config, list):
        fk_checks = ri_config
    else:
        fk_checks = []

    # Filter to checks relevant to this table
    relevant_checks: List[Dict] = []
    for fk in fk_checks:
        parsed = _parse_fk_def(fk)
        if table in (parsed["child_table"], parsed["parent_table"]):
            relevant_checks.append(fk)

    if not relevant_checks:
        return {
            "violations": 0,
            "details": [],
            "note": f"No FK checks configured for '{table}'",
        }

    total_violations = 0
    details: List[Dict] = []
    opened_conns: List[Any] = []

    for fk in relevant_checks:
        parsed = _parse_fk_def(fk)
        child_table = parsed["child_table"]
        parent_table = parsed["parent_table"]
        fk_column = parsed["fk_column"]
        pk_column = parsed["pk_column"]
        child_source = parsed["child_source"]
        parent_source = parsed["parent_source"]

        is_cross_source = (
            child_source and parent_source
            and child_source != parent_source
        )

        check_label = f"{child_table}.{fk_column} -> {parent_table}.{pk_column}"
        if is_cross_source:
            check_label += f" ({child_source} -> {parent_source})"

        try:
            if is_cross_source:
                from dm.config import get_connection_config
                from dm.connectors.postgres import get_connector

                child_conn = get_connector(get_connection_config(config, child_source))
                parent_conn = get_connector(get_connection_config(config, parent_source))
                child_conn.connect()
                parent_conn.connect()
                opened_conns.extend([child_conn, parent_conn])

                logger.info(f"Cross-source FK check: {check_label}")
                result = _cross_source_check(
                    child_conn, parent_conn,
                    child_table, parent_table,
                    fk_column, pk_column,
                )
            else:
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
                "cross_source": result.get("cross_source", False),
            })

            if orphan_count > 0:
                logger.warning(f"FK integrity violation: {check_label} ({orphan_count} orphan(s))")

        except Exception as e:
            logger.error(f"FK integrity check failed for {check_label}: {e}")
            details.append({
                "check": check_label,
                "orphan_count": -1,
                "error": str(e),
            })

    # Close connections opened for cross-source checks
    for conn in opened_conns:
        try:
            conn.close()
        except Exception:
            pass

    return {
        "violations": total_violations,
        "details": details,
    }
