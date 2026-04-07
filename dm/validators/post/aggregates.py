"""Aggregate Validator — compare business aggregate queries between systems."""

import logging
from typing import Any, Dict

import pandas as pd

from dm.validators.base import PostValidator, ValidatorResult

logger = logging.getLogger(__name__)


def _compare_with_tolerance(df1: pd.DataFrame, df2: pd.DataFrame, tolerance: float) -> bool:
    if df1.shape != df2.shape:
        return False
    for col in df1.columns:
        if pd.api.types.is_numeric_dtype(df1[col]):
            if not all(abs(df1[col].fillna(0) - df2[col].fillna(0)) <= tolerance):
                return False
        else:
            if not df1[col].equals(df2[col]):
                return False
    return True


class AggregateValidator(PostValidator):
    """Validate business aggregate queries match between legacy and modern."""

    name = "aggregates"

    def run(self, legacy_conn, modern_conn, dataset, config) -> ValidatorResult:
        agg_config = config.get("validation", config).get("aggregates", {}).get(dataset, [])

        if not agg_config:
            return ValidatorResult(
                name=self.name, status="SKIP", score_penalty=0,
                details={"reason": f"No aggregate checks configured for {dataset}"},
            )

        results = {}
        total_penalty = 0.0

        for agg in agg_config:
            name = agg["name"]
            comparison = agg.get("comparison", "exact")
            tolerance = agg.get("tolerance", 0.01)

            try:
                legacy_result = legacy_conn.execute_query(agg["legacy_query"])
                modern_result = modern_conn.execute_query(agg["modern_query"])

                if comparison == "exact":
                    match = legacy_result.equals(modern_result)
                else:
                    match = _compare_with_tolerance(legacy_result, modern_result, tolerance)

                results[name] = {
                    "legacy": legacy_result.to_dict("records"),
                    "modern": modern_result.to_dict("records"),
                    "match": match,
                }
                if not match:
                    total_penalty += 10

            except Exception as e:
                logger.error(f"Aggregate check '{name}' failed: {e}")
                results[name] = {"error": str(e), "match": False}
                total_penalty += 10

        has_failures = any(
            not r.get("match", True) for r in results.values() if isinstance(r, dict)
        )

        return ValidatorResult(
            name=self.name,
            status="FAIL" if has_failures else "PASS",
            score_penalty=total_penalty,
            details=results,
            severity="MEDIUM" if has_failures else "INFO",
        )
