"""
Sample Compare Validator

Draws a random sample from legacy, matches records in modern,
and compares field values with format-aware tolerance.
"""

import json
import logging
import re
from datetime import date as date_type, datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)

_DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
]

_BOOL_MAP = {
    "y": True, "n": False, "true": True, "false": False,
    "1": True, "0": False,
}


def _parse_date_safe(val):
    if isinstance(val, (datetime, date_type)):
        return val.date() if isinstance(val, datetime) else val
    s = str(val).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _values_equivalent(val1, val2) -> bool:
    """Compare values with format-aware tolerance."""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False

    s1 = str(val1).strip().lower()
    s2 = str(val2).strip().lower()

    # Boolean Y/N normalization
    b1 = val1 if isinstance(val1, bool) else _BOOL_MAP.get(s1)
    b2 = val2 if isinstance(val2, bool) else _BOOL_MAP.get(s2)
    if b1 is not None and b2 is not None:
        return b1 == b2

    # Date normalization
    d1 = _parse_date_safe(val1)
    d2 = _parse_date_safe(val2)
    if d1 is not None and d2 is not None:
        return d1 == d2

    # Phone/digit normalization
    digits1 = re.sub(r"\D", "", str(val1))
    digits2 = re.sub(r"\D", "", str(val2))
    if digits1 and digits2 and len(digits1) >= 7 and digits1 == digits2:
        return True

    # Numeric comparison
    try:
        return abs(float(val1) - float(val2)) < 0.001
    except (ValueError, TypeError):
        pass

    return s1 == s2


def _build_column_pairs(legacy_cols, modern_cols, mappings, table):
    SKIP_TYPES = {"removed", "transform", "archived"}
    pairs = []
    all_mapped_sources = {m["source"] for m in mappings if m.get("table") == table}

    for m in mappings:
        if (m.get("table") == table
                and m.get("target")
                and m.get("type") not in SKIP_TYPES):
            if m["source"] in legacy_cols and m["target"] in modern_cols:
                pairs.append((m["source"], m["target"]))

    for col in legacy_cols:
        if col not in all_mapped_sources and col in modern_cols:
            pairs.append((col, col))

    return pairs


class SampleCompareValidator(PostValidator):
    """Compare random sample records field-by-field between legacy and modern."""

    name = "sample_compare"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        from dm.discovery.dataset_resolver import DatasetResolver

        sample_size = config.get("validation", {}).get("sample_size", 100)
        metadata_path = Path(config.get("_project_dir", ".")) / config.get("metadata", {}).get("path", "./metadata")
        resolver = DatasetResolver(config)

        mappings_file = metadata_path / "mappings.json"
        mappings = []
        if mappings_file.exists():
            try:
                mappings = json.loads(mappings_file.read_text()).get("mappings", [])
            except Exception:
                pass

        try:
            legacy_schema = legacy_conn.get_table_schema(dataset)
            if not legacy_schema:
                return ValidatorResult(
                    name=self.name, status="SKIP", score_penalty=0,
                    details={"error": f"No schema found for {dataset}"},
                )

            # Determine modern table and key
            if resolver.is_normalized(dataset):
                modern_table = resolver.get_primary_table(dataset)
            else:
                modern_table = dataset

            modern_schema = modern_conn.get_table_schema(modern_table)
            if not modern_schema:
                return ValidatorResult(
                    name=self.name, status="SKIP", score_penalty=0,
                    details={"error": f"No schema found for {modern_table}"},
                )

            legacy_key = legacy_schema[0]["column_name"]
            modern_cols_list = [col["column_name"] for col in modern_schema]

            # Resolve modern key via mappings
            modern_key = legacy_key
            for m in mappings:
                if m.get("table") == dataset and m.get("source") == legacy_key and m.get("target"):
                    modern_key = m["target"]
                    break
            if modern_key not in modern_cols_list and legacy_key in modern_cols_list:
                modern_key = legacy_key

            # Sample from legacy
            query = f"SELECT * FROM {dataset} ORDER BY RANDOM() LIMIT {sample_size}"
            legacy_sample = legacy_conn.execute_query(query)
            if legacy_sample.empty:
                return ValidatorResult(
                    name=self.name, status="SKIP", score_penalty=0,
                    details={"note": "No records in legacy"},
                )

            sampled_ids = legacy_sample[legacy_key].tolist()

            # Fetch matching modern records
            # For normalized datasets, use reconstruction query if available
            recon_query = resolver.build_reconstruction_query(dataset) if resolver.is_normalized(dataset) else None

            placeholders = ", ".join(["%s"] * len(sampled_ids))
            if recon_query:
                modern_query = f"{recon_query} WHERE {modern_table}.{modern_key} IN ({placeholders})"
            else:
                modern_query = f"SELECT * FROM {modern_table} WHERE {modern_key} IN ({placeholders})"
            modern_df = modern_conn.execute_query(modern_query, tuple(sampled_ids))

            if modern_df.empty:
                return ValidatorResult(
                    name=self.name, status="WARN", score_penalty=30,
                    details={"note": "No matching records in modern"},
                    severity="HIGH",
                )

            col_pairs = _build_column_pairs(
                legacy_sample.columns.tolist(), modern_df.columns.tolist(),
                mappings, dataset,
            )

            modern_indexed = {row[modern_key]: row for _, row in modern_df.iterrows()}
            exact_matches = 0
            discrepancies = []

            for _, legacy_row in legacy_sample.iterrows():
                record_id = legacy_row[legacy_key]
                if record_id not in modern_indexed:
                    continue
                modern_row = modern_indexed[record_id]
                row_diffs = []
                for legacy_col, modern_col in col_pairs:
                    if legacy_col == legacy_key and modern_col == modern_key:
                        continue
                    if not _values_equivalent(legacy_row.get(legacy_col), modern_row.get(modern_col)):
                        row_diffs.append({
                            "legacy_column": legacy_col,
                            "modern_column": modern_col,
                            "legacy_value": str(legacy_row.get(legacy_col))[:50],
                            "modern_value": str(modern_row.get(modern_col))[:50],
                        })
                if not row_diffs:
                    exact_matches += 1
                else:
                    discrepancies.append({"id": record_id, "differences": row_diffs[:5]})

            total = len(modern_indexed)
            match_rate = (exact_matches / total * 100) if total else 0
            penalty = (100 - match_rate) * 0.3

            return ValidatorResult(
                name=self.name,
                status="PASS" if match_rate == 100 else ("WARN" if match_rate >= 90 else "FAIL"),
                score_penalty=penalty,
                details={
                    "sample_size": total,
                    "exact_matches": exact_matches,
                    "discrepancies": len(discrepancies),
                    "match_rate": match_rate,
                    "sample_discrepancies": discrepancies[:10],
                },
                severity="INFO" if match_rate == 100 else "MEDIUM",
            )

        except Exception as e:
            logger.error(f"Sample comparison failed: {e}")
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=0,
                details={"error": str(e)},
            )
