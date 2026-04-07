"""
Encoding Validator — Verify character encoding survived migration.

Checks for mojibake (garbled text from encoding mismatches), common
EBCDIC-to-UTF8 conversion issues, and non-ASCII character preservation.
"""

import logging
import re
from typing import Any, Dict

import pandas as pd

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)

# Common mojibake patterns (UTF-8 bytes misinterpreted as Latin-1)
MOJIBAKE_PATTERNS = [
    r'Ã[\x80-\xBF]',       # UTF-8 two-byte sequences read as Latin-1
    r'â\x80[\x90-\xBF]',   # UTF-8 three-byte sequences
    r'\xef\xbf\xbd',       # Unicode replacement character U+FFFD
    r'\x00',               # Null bytes (EBCDIC padding not cleaned)
]


class EncodingValidator(PostValidator):
    """Verify character encoding survived migration (EBCDIC to UTF-8)."""

    name = "encoding"

    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        config: Dict,
    ) -> ValidatorResult:
        """Check for encoding issues in migrated text columns."""
        from dm.discovery.dataset_resolver import DatasetResolver

        resolver = DatasetResolver(config)
        modern_table = resolver.get_primary_table(dataset) if resolver.is_normalized(dataset) else dataset

        try:
            modern_schema = modern_conn.get_table_schema(modern_table)
        except Exception as e:
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=0,
                details={"error": str(e)}, severity="MEDIUM",
            )

        # Find text/varchar columns
        text_columns = [
            col["column_name"] for col in modern_schema
            if col.get("data_type", "").lower() in (
                "character varying", "varchar", "text", "char", "character",
            )
        ]

        if not text_columns:
            return ValidatorResult(
                name=self.name, status="SKIP", score_penalty=0,
                details={"reason": "No text columns found"},
                severity="INFO",
            )

        issues = []
        sample_size = min(config.get("validation", {}).get("sample_size", 1000), 5000)

        # Sample text data from modern table
        cols_sql = ", ".join(text_columns[:20])  # Limit columns to check
        query = f"SELECT {cols_sql} FROM {modern_table} LIMIT {sample_size}"

        try:
            sample_df = modern_conn.execute_query(query)
        except Exception as e:
            return ValidatorResult(
                name=self.name, status="ERROR", score_penalty=0,
                details={"error": f"Failed to sample: {e}"},
            )

        for col in sample_df.columns:
            col_values = sample_df[col].dropna().astype(str)
            if col_values.empty:
                continue

            # Check for mojibake patterns
            for pattern in MOJIBAKE_PATTERNS:
                matches = col_values[col_values.str.contains(pattern, regex=True, na=False)]
                if not matches.empty:
                    issues.append({
                        "column": col,
                        "issue": "mojibake",
                        "severity": "HIGH",
                        "count": len(matches),
                        "sample": str(matches.iloc[0])[:100],
                        "detail": f"{col}: {len(matches)} rows with encoding artifacts",
                    })
                    break

            # Check for null byte contamination
            null_matches = col_values[col_values.str.contains('\x00', na=False)]
            if not null_matches.empty:
                issues.append({
                    "column": col,
                    "issue": "null_bytes",
                    "severity": "MEDIUM",
                    "count": len(null_matches),
                    "detail": f"{col}: {len(null_matches)} rows with embedded null bytes",
                })

            # Check for replacement characters
            replacement = col_values[col_values.str.contains('\ufffd', na=False)]
            if not replacement.empty:
                issues.append({
                    "column": col,
                    "issue": "replacement_characters",
                    "severity": "HIGH",
                    "count": len(replacement),
                    "detail": f"{col}: {len(replacement)} rows with Unicode replacement chars (U+FFFD)",
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
                "columns_checked": len(text_columns[:20]),
                "sample_size": sample_size,
                "issue_count": len(issues),
                "issues": issues[:15],
            },
            severity="HIGH" if penalty >= 10 else "MEDIUM" if issues else "INFO",
        )
