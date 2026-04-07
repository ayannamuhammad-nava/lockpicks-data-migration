"""Referential Integrity Validator — check FK relationships in modern system."""

import logging
from typing import Any, Dict

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


class ReferentialIntegrityValidator(PostValidator):
    """Check foreign key integrity in the modern system using config-defined relationships."""

    name = "referential_integrity"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        from dm.discovery.dataset_resolver import DatasetResolver

        # Support both flat list and per-dataset dict formats
        ri_config = config.get("validation", config).get("referential_integrity", {})

        if isinstance(ri_config, dict):
            fk_checks = ri_config.get(dataset, [])
        elif isinstance(ri_config, list):
            # Flat list format: [{child: "t.col", parent: "t.col"}, ...]
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

        for fk in fk_checks:
            # Support both formats:
            # Format 1: {child_table, parent_table, fk_column, pk_column}
            # Format 2: {child: "table.col", parent: "table.col"}
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

            check_name = f"{child_table}_{parent_table}_fk"

            try:
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
