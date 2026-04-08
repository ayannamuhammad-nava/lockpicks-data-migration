"""
Data Quality Validator

Runs cross-field anomaly checks provided by plugins via the
dm_data_quality_rules hook. The toolkit ships no built-in rules —
each migration project defines its own via a plugin.
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


class DataQualityValidator(PreValidator):
    """Run plugin-provided cross-field data quality anomaly checks."""

    name = "data_quality"

    def __init__(self, plugin_rules: List[Dict] = None):
        """
        Args:
            plugin_rules: List of rule dicts from dm_data_quality_rules hook.
                Each rule has: name, severity, description, check_fn(df) -> dict|None
        """
        self._plugin_rules = plugin_rules or []

    def run(self, legacy_conn, modern_conn, dataset, sample_df, config) -> ValidatorResult:
        anomalies = []

        for rule in self._plugin_rules:
            try:
                result = rule["check_fn"](sample_df)
                if result is not None:
                    result.setdefault("rule", rule["name"])
                    result.setdefault("severity", rule.get("severity", "MEDIUM"))
                    result.setdefault("description", rule.get("description", ""))
                    anomalies.append(result)
            except Exception as e:
                logger.error(f"Data quality rule '{rule['name']}' failed: {e}")
                anomalies.append({
                    "rule": rule["name"],
                    "severity": "ERROR",
                    "description": f"Rule execution failed: {e}",
                })

        if anomalies:
            logger.warning(f"Found {len(anomalies)} data quality anomaly type(s)")

        return ValidatorResult(
            name=self.name,
            status="WARN" if anomalies else "PASS",
            score_penalty=0,  # Anomalies are advisory, not penalized in score
            details={"anomalies": anomalies, "count": len(anomalies)},
            severity="HIGH" if any(a.get("severity") == "HIGH" for a in anomalies) else "INFO",
        )
