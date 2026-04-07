"""
Normalization Integrity Validator — Post-migration verification for
flat→normalized decomposition.

Ensures no data was lost when a flat legacy table was decomposed into
multiple normalized modern tables.
"""

import logging
from typing import Any, Dict

from dm.discovery.dataset_resolver import DatasetResolver
from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


class NormalizationIntegrityValidator(PostValidator):
    """Verify no data lost during flat→normalized decomposition."""

    name = "normalization_integrity"

    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        config: Dict,
    ) -> ValidatorResult:
        """Run normalization integrity checks."""
        resolver = DatasetResolver(config)

        if not resolver.is_normalized(dataset):
            return ValidatorResult(
                name=self.name, status="SKIP", score_penalty=0,
                details={"reason": "Not a normalized dataset"},
                severity="INFO",
            )

        issues = []
        modern_tables = resolver.get_modern_tables(dataset)
        primary_table = resolver.get_primary_table(dataset)

        # Check 1: Primary table row count matches legacy
        legacy_count = legacy_conn.get_row_count(dataset)
        try:
            primary_count = modern_conn.get_row_count(primary_table)
        except Exception as e:
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=10,
                details={"error": f"Cannot access primary table {primary_table}: {e}"},
                severity="HIGH",
            )

        if legacy_count != primary_count:
            diff = abs(legacy_count - primary_count)
            diff_pct = (diff / legacy_count * 100) if legacy_count > 0 else 0
            issues.append({
                "check": "primary_row_count",
                "severity": "HIGH" if diff_pct > 1 else "MEDIUM",
                "detail": (
                    f"Legacy {dataset}: {legacy_count} rows, "
                    f"Modern {primary_table}: {primary_count} rows "
                    f"(diff: {diff}, {diff_pct:.1f}%)"
                ),
            })

        # Check 2: Child tables have reasonable row counts
        for table_info in modern_tables:
            if table_info.get("role") not in ("child",):
                continue
            child_table = table_info["table"]
            try:
                child_count = modern_conn.get_row_count(child_table)
            except Exception:
                issues.append({
                    "check": "child_table_access",
                    "severity": "HIGH",
                    "detail": f"Cannot access child table: {child_table}",
                })
                continue

            if child_count == 0 and legacy_count > 0:
                issues.append({
                    "check": "empty_child_table",
                    "severity": "HIGH",
                    "detail": f"Child table {child_table} is empty but legacy has {legacy_count} rows",
                })

        # Check 3: FK integrity within normalized tables
        for table_info in modern_tables:
            if table_info.get("role") != "child":
                continue
            fk_col = table_info.get("fk")
            pk_col = resolver.get_primary_key(dataset)
            if not fk_col or not pk_col:
                continue

            try:
                result = modern_conn.check_referential_integrity(
                    child_table=table_info["table"],
                    parent_table=primary_table,
                    fk_column=fk_col,
                    pk_column=pk_col,
                )
                orphan_count = result.get("orphan_count", 0)
                if orphan_count > 0:
                    issues.append({
                        "check": "fk_integrity",
                        "severity": "HIGH",
                        "detail": (
                            f"{orphan_count} orphaned records in "
                            f"{table_info['table']}.{fk_col} "
                            f"(missing parent in {primary_table}.{pk_col})"
                        ),
                    })
            except Exception as e:
                logger.warning(f"FK check failed for {table_info['table']}: {e}")

        # Check 4: Reconstruction — can we rebuild the flat view?
        recon_query = resolver.build_reconstruction_query(dataset)
        if recon_query:
            try:
                recon_df = modern_conn.execute_query(f"{recon_query} LIMIT 1")
                if recon_df.empty and legacy_count > 0:
                    issues.append({
                        "check": "reconstruction",
                        "severity": "HIGH",
                        "detail": "Reconstruction query returned empty results",
                    })
            except Exception as e:
                issues.append({
                    "check": "reconstruction",
                    "severity": "MEDIUM",
                    "detail": f"Reconstruction query failed: {e}",
                })

        # Calculate penalty
        penalty = sum(
            5 if i["severity"] == "HIGH" else 2 if i["severity"] == "MEDIUM" else 0
            for i in issues
        )
        penalty = min(penalty, 30)

        status = "PASS" if not issues else "FAIL" if penalty >= 10 else "WARN"
        severity = "INFO" if not issues else "HIGH" if penalty >= 10 else "MEDIUM"

        return ValidatorResult(
            name=self.name,
            status=status,
            score_penalty=penalty,
            details={
                "issue_count": len(issues),
                "issues": issues,
                "modern_tables": [t["table"] for t in modern_tables],
                "legacy_row_count": legacy_count,
                "primary_table": primary_table,
            },
            severity=severity,
        )
