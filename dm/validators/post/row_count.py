"""Row Count Validator — verify legacy and modern row counts match."""

import logging
from typing import Any, Dict

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


class RowCountValidator(PostValidator):
    """Verify row counts match between legacy and modern systems."""

    name = "row_count"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        from dm.discovery.dataset_resolver import DatasetResolver

        resolver = DatasetResolver(config)
        legacy_count = legacy_conn.get_row_count(dataset)

        if resolver.is_normalized(dataset):
            # Normalized: compare against primary entity table
            primary_table = resolver.get_primary_table(dataset)
            modern_count = modern_conn.get_row_count(primary_table)

            # Also check child tables aren't empty
            child_details = {}
            for child in resolver.get_child_tables(dataset):
                try:
                    child_count = modern_conn.get_row_count(child["table"])
                    child_details[child["table"]] = child_count
                except Exception:
                    child_details[child["table"]] = "ERROR"
        else:
            modern_count = modern_conn.get_row_count(dataset)
            child_details = {}

        match = legacy_count == modern_count
        difference = abs(legacy_count - modern_count)

        penalty = 0.0
        if not match and legacy_count > 0:
            diff_pct = difference / legacy_count * 100
            penalty = min(diff_pct, 30)

        details = {
            "legacy_count": legacy_count,
            "modern_count": modern_count,
            "match": match,
            "difference": difference,
        }
        if child_details:
            details["child_table_counts"] = child_details
            details["primary_table"] = resolver.get_primary_table(dataset)

        return ValidatorResult(
            name=self.name,
            status="PASS" if match else "FAIL",
            score_penalty=penalty,
            details=details,
            severity="INFO" if match else "HIGH",
        )
