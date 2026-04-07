"""
Compliance Validators

ArchivedLeakageValidator — ensure archived fields haven't leaked into modern schema.
UnmappedColumnsValidator — detect ungoverned columns added outside ETL spec.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


def _load_mappings(metadata_path: Path) -> list:
    mappings_file = metadata_path / "mappings.json"
    if not mappings_file.exists():
        return []
    try:
        return json.loads(mappings_file.read_text()).get("mappings", [])
    except Exception:
        return []


class ArchivedLeakageValidator(PostValidator):
    """Check that fields marked 'archived' in mappings.json are NOT in modern schema."""

    name = "archived_leakage"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        metadata_path = Path(config.get("_project_dir", ".")) / config.get("metadata", {}).get("path", "./metadata")
        mappings = _load_mappings(metadata_path)

        archived_cols = {
            m["source"].lower(): m
            for m in mappings
            if m.get("table", "").lower() == dataset.lower() and m.get("type") == "archived"
        }

        if not archived_cols:
            return ValidatorResult(
                name=self.name, status="PASS", score_penalty=0,
                details={"violations": [], "violation_count": 0},
            )

        try:
            modern_schema = modern_conn.get_table_schema(dataset)
            modern_cols = {col["column_name"].lower() for col in modern_schema}
        except Exception as e:
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=0,
                details={"error": str(e)},
            )

        violations = []
        for col_lower, mapping in archived_cols.items():
            if col_lower in modern_cols:
                violations.append({
                    "column": mapping["source"],
                    "table": dataset,
                    "rationale": mapping.get("rationale", ""),
                    "severity": "CRITICAL",
                })

        penalty = min(len(violations) * 20, 40)

        return ValidatorResult(
            name=self.name,
            status="FAIL" if violations else "PASS",
            score_penalty=penalty,
            details={"violations": violations, "violation_count": len(violations)},
            severity="CRITICAL" if violations else "INFO",
        )


class UnmappedColumnsValidator(PostValidator):
    """Detect modern columns with no source mapping — ungoverned columns."""

    name = "unmapped_columns"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        metadata_path = Path(config.get("_project_dir", ".")) / config.get("metadata", {}).get("path", "./metadata")
        mappings = _load_mappings(metadata_path)

        mapped_targets = {
            m["target"].lower()
            for m in mappings
            if m.get("table", "").lower() == dataset.lower() and m.get("target")
        }

        archived_sources = {
            m["source"].lower()
            for m in mappings
            if m.get("table", "").lower() == dataset.lower() and m.get("type") == "archived"
        }

        try:
            modern_schema = modern_conn.get_table_schema(dataset)
            modern_cols = {col["column_name"].lower() for col in modern_schema}
        except Exception as e:
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=0,
                details={"error": str(e)},
            )

        ungoverned = sorted((modern_cols - mapped_targets) - archived_sources)
        penalty = min(len(ungoverned) * 5, 15)

        return ValidatorResult(
            name=self.name,
            status="WARN" if ungoverned else "PASS",
            score_penalty=penalty,
            details={
                "ungoverned_columns": ungoverned,
                "count": len(ungoverned),
            },
            severity="MEDIUM" if ungoverned else "INFO",
        )
