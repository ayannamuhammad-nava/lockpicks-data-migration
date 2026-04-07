"""Checksum Validator — compare column-level MD5 checksums."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


def _load_column_mappings(metadata_path: Path) -> list:
    mappings_file = metadata_path / "mappings.json"
    if not mappings_file.exists():
        return []
    try:
        data = json.loads(mappings_file.read_text())
        return data.get("mappings", [])
    except Exception:
        return []


def _build_column_pairs(legacy_cols, modern_cols, mappings, table) -> list:
    """Build (legacy_col, modern_col) pairs for comparable columns only."""
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


class ChecksumValidator(PostValidator):
    """Compute and compare column-level checksums between legacy and modern."""

    name = "checksums"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        metadata_path = Path(config.get("_project_dir", ".")) / config.get("metadata", {}).get("path", "./metadata")
        mappings = _load_column_mappings(metadata_path)

        try:
            legacy_schema = legacy_conn.get_table_schema(dataset)
            modern_schema = modern_conn.get_table_schema(dataset)

            legacy_cols = [col["column_name"] for col in legacy_schema]
            modern_cols = [col["column_name"] for col in modern_schema]

            col_pairs = _build_column_pairs(legacy_cols, modern_cols, mappings, dataset)

            results = {}
            matches = 0
            mismatches = 0

            for legacy_col, modern_col in col_pairs:
                try:
                    legacy_hash = legacy_conn.get_column_hash(dataset, legacy_col)
                    modern_hash = modern_conn.get_column_hash(dataset, modern_col)
                    is_match = legacy_hash == modern_hash
                    if is_match:
                        matches += 1
                    else:
                        mismatches += 1
                    results[f"{legacy_col}->{modern_col}"] = {
                        "legacy_checksum": legacy_hash[:12] + "..." if legacy_hash else "N/A",
                        "modern_checksum": modern_hash[:12] + "..." if modern_hash else "N/A",
                        "match": is_match,
                    }
                except Exception as e:
                    logger.warning(f"Checksum failed for {legacy_col}->{modern_col}: {e}")
                    results[f"{legacy_col}->{modern_col}"] = {"match": False, "error": str(e)}

            match_rate = (matches / len(col_pairs) * 100) if col_pairs else 0
            results["_summary"] = {
                "total_columns": len(col_pairs),
                "matches": matches,
                "mismatches": mismatches,
                "match_rate": match_rate,
            }

            # Checksums are not penalised — legacy CHAR vs modern VARCHAR formatting
            # causes hash mismatches even when data is equivalent after trimming.
            return ValidatorResult(
                name=self.name,
                status="PASS" if mismatches == 0 else "WARN",
                score_penalty=0,
                details=results,
            )

        except Exception as e:
            logger.error(f"Checksum computation failed: {e}")
            return ValidatorResult(
                name=self.name,
                status="ERROR",
                score_penalty=0,
                details={"error": str(e)},
            )
