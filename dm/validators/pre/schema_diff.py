"""
Schema Diff Validator

Compares legacy and modern schemas, computes structure score using
knowledge-base mapping types, and generates schema diff report.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


def _load_column_mapping_types(metadata_path: Path, dataset: str) -> Dict[str, str]:
    """Load mappings.json and return {source_col: mapping_type} for the given table."""
    mappings_file = metadata_path / "mappings.json"
    if not mappings_file.exists():
        return {}
    try:
        data = json.loads(mappings_file.read_text())
        return {
            m["source"].lower(): m.get("type", "removed")
            for m in data.get("mappings", [])
            if m.get("table", "").lower() == dataset.lower()
        }
    except Exception:
        return {}


class SchemaDiffValidator(PreValidator):
    """Compare legacy and modern schemas and score structural readiness."""

    name = "schema_diff"

    def run(self, legacy_conn, modern_conn, dataset, sample_df, config) -> ValidatorResult:
        from dm.discovery.schema_introspector import introspect_schema, compare_schemas

        legacy_schema = introspect_schema(legacy_conn, dataset)
        modern_schema = introspect_schema(modern_conn, dataset)
        schema_diff = compare_schemas(legacy_schema, modern_schema)

        # Calculate penalty using knowledge-base mapping types
        metadata_path = Path(config.get("_project_dir", ".")) / config.get("metadata", {}).get("path", "./metadata")
        mapping_types = _load_column_mapping_types(metadata_path, dataset)

        PENALTY = {
            "rename": 0,
            "transform": 0,
            "archived": 1,
            "removed": 4,
        }

        penalties = 0.0
        for col in schema_diff.get("missing_in_modern", []):
            col_type = mapping_types.get(col.lower(), "removed")
            penalties += PENALTY.get(col_type, 4)

        type_mismatches = len(schema_diff.get("type_mismatches", []))
        penalties += type_mismatches * 5

        status = "PASS" if penalties == 0 else ("WARN" if penalties < 20 else "FAIL")

        return ValidatorResult(
            name=self.name,
            status=status,
            score_penalty=penalties,
            details={
                "schema_diff": schema_diff,
                "legacy_schema": legacy_schema,
                "modern_schema": modern_schema,
                "mapping_types": mapping_types,
            },
            severity="LOW" if penalties < 10 else "MEDIUM" if penalties < 30 else "HIGH",
        )
