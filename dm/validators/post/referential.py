"""Referential Integrity Validator — check FK relationships in modern system.

Supports cross-source referential integrity: when the child and parent
tables live in different databases (e.g., claims in claims_db references
claimants in eligibility_db), the validator opens connections to both
sources and checks for orphans in Python.

Config format for cross-source FK checks:

    referential_integrity:
      claims:
        - child_table: claims
          child_source: claims_db        # optional — which connection
          parent_table: claimants
          parent_source: eligibility_db   # optional — different connection
          fk_column: claimant_id
          pk_column: claimant_id

When child_source and parent_source are omitted or identical, the check
runs as a single-connection JOIN (original behavior).
"""

import logging
from typing import Any, Dict, Optional, Set

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


def _parse_fk_check(fk: Dict) -> Dict:
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


def _check_cross_source(
    child_conn: Any,
    parent_conn: Any,
    child_table: str,
    parent_table: str,
    fk_column: str,
    pk_column: str,
) -> Dict:
    """Check referential integrity across two different database connections.

    Pulls FK values from the child and PK values from the parent, then
    compares in Python to find orphans.
    """
    # Get child FK values
    child_df = child_conn.execute_query(
        f"SELECT DISTINCT {fk_column} FROM {child_table} WHERE {fk_column} IS NOT NULL"
    )
    child_fk_values: Set = set(child_df[fk_column].tolist()) if not child_df.empty else set()

    # Get parent PK values
    parent_df = parent_conn.execute_query(
        f"SELECT DISTINCT {pk_column} FROM {parent_table}"
    )
    parent_pk_values: Set = set(parent_df[pk_column].tolist()) if not parent_df.empty else set()

    # Orphans = child FK values not in parent PK values
    orphans = child_fk_values - parent_pk_values
    orphan_sample = sorted(list(orphans))[:10]

    return {
        "orphan_count": len(orphans),
        "orphan_sample": orphan_sample,
        "cross_source": True,
        "child_fk_count": len(child_fk_values),
        "parent_pk_count": len(parent_pk_values),
    }


class ReferentialIntegrityValidator(PostValidator):
    """Check foreign key integrity using config-defined relationships.

    Supports both same-database and cross-database FK checks.
    """

    name = "referential_integrity"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        from dm.discovery.dataset_resolver import DatasetResolver

        # Support both flat list and per-dataset dict formats
        ri_config = config.get("validation", config).get("referential_integrity", {})

        if isinstance(ri_config, dict):
            fk_checks = ri_config.get(dataset, [])
        elif isinstance(ri_config, list):
            fk_checks = ri_config
        else:
            fk_checks = []

        # Auto-generate FK checks from normalization plan if none configured
        if not fk_checks:
            resolver = DatasetResolver(config)
            if resolver.is_normalized(dataset):
                primary_table = resolver.get_primary_table(dataset)
                pk = resolver.get_primary_key(dataset)
                for child in resolver.get_child_tables(dataset):
                    fk_checks.append({
                        "child_table": child["table"],
                        "parent_table": primary_table,
                        "fk_column": child.get("fk", pk),
                        "pk_column": pk,
                    })

        if not fk_checks:
            return ValidatorResult(
                name=self.name,
                status="SKIP",
                score_penalty=0,
                details={"reason": f"No FK checks configured for {dataset}"},
            )

        results = {}
        total_penalty = 0.0
        # Track connections opened for cross-source checks so we can close them
        _opened_conns = []

        for fk in fk_checks:
            parsed = _parse_fk_check(fk)
            child_table = parsed["child_table"]
            parent_table = parsed["parent_table"]
            fk_column = parsed["fk_column"]
            pk_column = parsed["pk_column"]
            child_source = parsed["child_source"]
            parent_source = parsed["parent_source"]

            check_name = f"{child_table}_{parent_table}_fk"
            is_cross_source = (
                child_source and parent_source
                and child_source != parent_source
            )

            try:
                if is_cross_source:
                    # Cross-source: open separate connections
                    fk_result = self._run_cross_source_check(
                        config, child_source, parent_source,
                        child_table, parent_table, fk_column, pk_column,
                        _opened_conns,
                    )
                else:
                    # Same-database: use the single modern_conn JOIN
                    fk_result = modern_conn.check_referential_integrity(
                        child_table=child_table,
                        parent_table=parent_table,
                        fk_column=fk_column,
                        pk_column=pk_column,
                    )

                results[check_name] = fk_result
                orphan_count = fk_result.get("orphan_count", 0)
                if orphan_count > 0:
                    total_penalty += min(orphan_count, 20)

            except Exception as e:
                logger.error(f"FK check '{check_name}' failed: {e}")
                results[check_name] = {"orphan_count": -1, "error": str(e)}

        # Close any connections we opened for cross-source checks
        for conn in _opened_conns:
            try:
                conn.close()
            except Exception:
                pass

        has_orphans = any(
            r.get("orphan_count", 0) > 0
            for r in results.values()
            if isinstance(r, dict)
        )

        return ValidatorResult(
            name=self.name,
            status="FAIL" if has_orphans else "PASS",
            score_penalty=total_penalty,
            details=results,
            severity="HIGH" if has_orphans else "INFO",
        )

    def _run_cross_source_check(
        self,
        config: Dict,
        child_source: str,
        parent_source: str,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: str,
        opened_conns: list,
    ) -> Dict:
        """Open connections to two different sources and check FK integrity."""
        from dm.config import get_connection_config
        from dm.connectors.postgres import get_connector

        child_conn_cfg = get_connection_config(config, child_source)
        parent_conn_cfg = get_connection_config(config, parent_source)

        child_conn = get_connector(child_conn_cfg)
        parent_conn = get_connector(parent_conn_cfg)
        child_conn.connect()
        parent_conn.connect()
        opened_conns.extend([child_conn, parent_conn])

        logger.info(
            f"Cross-source FK check: {child_table}.{fk_column} ({child_source}) "
            f"-> {parent_table}.{pk_column} ({parent_source})"
        )

        return _check_cross_source(
            child_conn, parent_conn,
            child_table, parent_table,
            fk_column, pk_column,
        )
