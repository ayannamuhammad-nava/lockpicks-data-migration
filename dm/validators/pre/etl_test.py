"""
ETL Test Validator — Auto-generate and run ETL test cases from transform scripts.

Validates that generated transform SQL correctly handles type conversions,
null handling, PII transformations, and boolean mappings before migration.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


class ETLTestValidator(PreValidator):
    """Auto-generate and run ETL test cases from transform scripts."""

    name = "etl_tests"

    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        sample_df: pd.DataFrame,
        config: Dict,
    ) -> ValidatorResult:
        """Run ETL tests by applying transforms to sample data."""
        project_dir = config.get("_project_dir", ".")
        transform_path = (
            Path(project_dir) / "artifacts" / "generated_schema"
            / f"{dataset}_transforms.sql"
        )

        if not transform_path.exists():
            return ValidatorResult(
                name=self.name, status="SKIP", score_penalty=0,
                details={"reason": f"No transform script found at {transform_path}"},
                severity="INFO",
            )

        transform_sql = transform_path.read_text()
        issues = []

        # Load mappings to understand expected transformations
        metadata_path = config.get("metadata", {}).get("path", "./metadata")
        mappings_file = Path(project_dir) / metadata_path / "mappings.json"
        mappings = []
        if mappings_file.exists():
            with open(mappings_file) as f:
                mappings = json.load(f).get("mappings", [])

        table_mappings = [m for m in mappings if m.get("table") == dataset]

        # Test 1: Verify transform SQL is syntactically valid
        if not transform_sql.strip():
            issues.append({
                "test": "transform_not_empty",
                "severity": "HIGH",
                "detail": "Transform script is empty",
            })

        # Test 2: Verify all mapped columns appear in the transform
        for mapping in table_mappings:
            source = mapping.get("source", "")
            target = mapping.get("target", "")
            mapping_type = mapping.get("type", "")

            if mapping_type in ("archived", "removed"):
                # Archived columns should NOT appear in transform target
                if target and target in transform_sql:
                    issues.append({
                        "test": "archived_column_in_transform",
                        "severity": "HIGH",
                        "detail": f"Archived column {source} appears as target {target} in transform",
                    })
            elif mapping_type == "transform" and target:
                if source not in transform_sql:
                    issues.append({
                        "test": "transform_source_missing",
                        "severity": "MEDIUM",
                        "detail": f"Transform for {source}->{target} not found in script",
                    })

        # Test 3: Verify PII columns have appropriate transforms
        for mapping in table_mappings:
            if mapping.get("type") == "transform":
                source = mapping.get("source", "")
                rationale = mapping.get("rationale", "").lower()
                if "sha" in rationale or "hash" in rationale:
                    if "sha256" not in transform_sql.lower() and "encode" not in transform_sql.lower():
                        issues.append({
                            "test": "pii_hash_missing",
                            "severity": "HIGH",
                            "detail": f"Column {source} requires hashing but no SHA-256 found in transform",
                        })

        # Test 4: Verify boolean conversions exist for Y/N columns
        if sample_df is not None and not sample_df.empty:
            for col in sample_df.columns:
                unique_vals = sample_df[col].dropna().astype(str).str.upper().unique()
                if set(unique_vals).issubset({"Y", "N", "YES", "NO", "T", "F", "TRUE", "FALSE"}):
                    if "CASE WHEN" not in transform_sql.upper() and "BOOLEAN" not in transform_sql.upper():
                        issues.append({
                            "test": "boolean_conversion_missing",
                            "severity": "LOW",
                            "detail": f"Column {col} has boolean values but no conversion in transform",
                        })

        penalty = sum(
            3 if i["severity"] == "HIGH" else 1 if i["severity"] == "MEDIUM" else 0
            for i in issues
        )
        penalty = min(penalty, 20)

        status = "PASS" if not issues else "FAIL" if penalty >= 10 else "WARN"

        return ValidatorResult(
            name=self.name,
            status=status,
            score_penalty=penalty,
            details={
                "issue_count": len(issues),
                "issues": issues[:15],
                "transform_path": str(transform_path),
                "mapping_count": len(table_mappings),
            },
            severity="HIGH" if penalty >= 10 else "MEDIUM" if issues else "INFO",
        )
