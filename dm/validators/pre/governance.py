"""
Governance Validator

Checks PII detection, naming conventions, null thresholds, and required fields.
Fully config-driven — no domain-specific logic.
"""

import logging
import re
from typing import Any, Dict, List

import pandas as pd

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


def detect_pii_columns(columns: List[str], pii_keywords: List[str]) -> List[str]:
    """Return column names that match PII keywords."""
    pii_cols = []
    for col in columns:
        col_lower = col.lower()
        for keyword in pii_keywords:
            if keyword in col_lower:
                pii_cols.append(col)
                break
    return pii_cols


def check_naming_conventions(columns: List[str], naming_regex: str) -> List[str]:
    """Return column names that violate naming convention."""
    pattern = re.compile(naming_regex)
    return [col for col in columns if not pattern.match(col)]


def check_required_fields(columns: List[str], required: List[str]) -> List[str]:
    """Return required fields that are missing."""
    col_set = {c.lower() for c in columns}
    return [r for r in required if r.lower() not in col_set]


def check_null_thresholds(df: pd.DataFrame, max_null_pct: float) -> Dict[str, float]:
    """Return columns exceeding the null percentage threshold."""
    violations = {}
    for col in df.columns:
        null_pct = df[col].isna().sum() / len(df) * 100 if len(df) > 0 else 0
        if null_pct > max_null_pct:
            violations[col] = round(null_pct, 2)
    return violations


def calculate_governance_score(gov_results: Dict) -> float:
    """Calculate governance score (0-100) from check results."""
    penalties = 0.0
    pii_count = len(gov_results.get("pii_columns", []))
    naming_violations = len(gov_results.get("naming_violations", []))
    missing_required = len(gov_results.get("missing_required", []))
    null_violations = len(gov_results.get("null_violations", {}))

    penalties += min(pii_count * 5, 30)
    penalties += min(naming_violations * 2, 10)
    penalties += min(missing_required * 10, 30)
    penalties += min(null_violations * 3, 15)

    return max(0, 100 - penalties)


class GovernanceValidator(PreValidator):
    """Run governance checks: PII, naming, nulls, required fields."""

    name = "governance"

    def run(self, legacy_conn, modern_conn, dataset, sample_df, config) -> ValidatorResult:
        gov_config = config.get("validation", {}).get("governance", {})

        pii_keywords = gov_config.get("pii_keywords", [])
        naming_regex = gov_config.get("naming_regex", r"^[a-z0-9_]+$")
        max_null_pct = gov_config.get("max_null_percent", 10)
        required_fields = gov_config.get("required_fields", {}).get(dataset, [])

        columns = sample_df.columns.tolist()

        pii_cols = detect_pii_columns(columns, pii_keywords)
        naming_violations = check_naming_conventions(columns, naming_regex)
        missing_required = check_required_fields(columns, required_fields)
        null_violations = check_null_thresholds(sample_df, max_null_pct)

        gov_results = {
            "pii_columns": pii_cols,
            "naming_violations": naming_violations,
            "missing_required": missing_required,
            "null_violations": null_violations,
        }

        gov_score = calculate_governance_score(gov_results)
        gov_results["governance_score"] = gov_score

        # Governance penalty feeds into the confidence formula separately,
        # so we report it as detail but set score_penalty=0 to avoid double-counting.
        has_issues = bool(pii_cols or naming_violations or missing_required or null_violations)
        status = "WARN" if has_issues else "PASS"

        return ValidatorResult(
            name=self.name,
            status=status,
            score_penalty=0,  # Governance is weighted separately in confidence formula
            details=gov_results,
            severity="MEDIUM" if has_issues else "INFO",
        )
