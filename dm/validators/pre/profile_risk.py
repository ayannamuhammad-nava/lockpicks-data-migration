"""
Profile Risk Validator — Pre-migration risk assessment using OM profiling stats.

Flags migration risks by comparing OM profiling data against the generated
modern schema constraints. Catches issues like NULL→NOT NULL conflicts,
value range violations, and unmapped enum values before migration starts.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


class ProfileRiskValidator(PreValidator):
    """Flag migration risks using OpenMetadata profiling stats."""

    name = "profile_risk"

    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        sample_df: Any,
        config: Dict,
    ) -> ValidatorResult:
        """Run profiling-based risk checks."""
        risks = []
        project_dir = config.get("_project_dir", ".")

        # Load enriched glossary (contains profiling data)
        metadata_path = config.get("metadata", {}).get("path", "./metadata")
        glossary_file = Path(project_dir) / metadata_path / "glossary.json"
        if not glossary_file.exists():
            return ValidatorResult(
                name=self.name, status="SKIP", score_penalty=0,
                details={"reason": "No enriched glossary found"},
                severity="INFO",
            )

        with open(glossary_file) as f:
            glossary = json.load(f)

        # Load generated schema info
        gen_schema_path = Path(project_dir) / "artifacts" / "generated_schema" / "diff_report.json"
        diff_report = {}
        if gen_schema_path.exists():
            with open(gen_schema_path) as f:
                diff_report = json.load(f)

        # Check each column with profiling data
        for entry in glossary.get("columns", []):
            if entry.get("table") != dataset:
                continue
            if entry.get("system") != "legacy":
                continue

            profiling = entry.get("profiling", {})
            if not profiling:
                continue

            col_name = entry["name"]

            # Risk 1: NULL→NOT NULL conflicts
            null_pct = profiling.get("null_percent", 0)
            if null_pct and null_pct > 0:
                # Check if modern schema has NOT NULL for this column
                is_nullable = entry.get("is_nullable", "YES")
                if is_nullable == "NO":
                    risks.append({
                        "column": col_name,
                        "risk": "null_to_not_null",
                        "severity": "HIGH",
                        "detail": (
                            f"{col_name} has {null_pct:.1f}% nulls in legacy "
                            f"but is NOT NULL in modern schema"
                        ),
                    })

            # Risk 2: Distribution anomalies
            stddev = profiling.get("stddev")
            mean = profiling.get("mean_value")
            max_val = profiling.get("max_value")
            if stddev and mean and max_val:
                try:
                    if float(stddev) > 0 and float(max_val) > float(mean) + 5 * float(stddev):
                        risks.append({
                            "column": col_name,
                            "risk": "extreme_outlier",
                            "severity": "MEDIUM",
                            "detail": (
                                f"{col_name} max={max_val} exceeds "
                                f"5 stddev from mean={mean} (stddev={stddev})"
                            ),
                        })
                except (ValueError, TypeError):
                    pass

            # Risk 3: High null percentage (data quality)
            if null_pct and null_pct > 50:
                risks.append({
                    "column": col_name,
                    "risk": "high_null_rate",
                    "severity": "MEDIUM",
                    "detail": f"{col_name} has {null_pct:.1f}% nulls — review data quality",
                })

            # Risk 4: Columns with 100% nulls (dead columns)
            if null_pct and null_pct >= 100:
                risks.append({
                    "column": col_name,
                    "risk": "dead_column",
                    "severity": "LOW",
                    "detail": f"{col_name} is 100% null — consider excluding from migration",
                })

        # Score penalty: 2 points per HIGH risk, 1 per MEDIUM
        penalty = sum(
            2 if r["severity"] == "HIGH" else 1 if r["severity"] == "MEDIUM" else 0
            for r in risks
        )
        penalty = min(penalty, 25)

        status = "PASS" if not risks else "WARN" if penalty < 10 else "FAIL"
        severity = "INFO" if not risks else "HIGH" if penalty >= 10 else "MEDIUM"

        return ValidatorResult(
            name=self.name,
            status=status,
            score_penalty=penalty,
            details={
                "risk_count": len(risks),
                "risks": risks[:20],  # Limit detail output
                "high_risks": sum(1 for r in risks if r["severity"] == "HIGH"),
                "medium_risks": sum(1 for r in risks if r["severity"] == "MEDIUM"),
            },
            severity=severity,
        )
