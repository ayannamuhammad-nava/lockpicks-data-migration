"""
Pandera Schema Validator

Validates sample data against auto-generated Pandera schemas.
"""

import logging
from typing import Any, Dict

import pandas as pd
import pandera as pa

from dm.validators.base import PreValidator, ValidatorResult

logger = logging.getLogger(__name__)


class PanderaValidator(PreValidator):
    """Validate legacy sample data against Pandera schema."""

    name = "pandera_validation"

    def run(self, legacy_conn, modern_conn, dataset, sample_df, config) -> ValidatorResult:
        from dm.discovery.pandera_generator import load_pandera_schema

        try:
            schema = load_pandera_schema("legacy", dataset, config)
            if schema is None:
                return ValidatorResult(
                    name=self.name,
                    status="SKIP",
                    score_penalty=0,
                    details={"reason": "No Pandera schema found"},
                )

            schema.validate(sample_df, lazy=True)
            return ValidatorResult(
                name=self.name,
                status="PASS",
                score_penalty=0,
                details={"errors": []},
            )

        except pa.errors.SchemaErrors as e:
            error_count = len(e.failure_cases)
            errors = [str(err) for err in e.failure_cases.to_dict("records")]
            penalty = min(error_count, 20)
            return ValidatorResult(
                name=self.name,
                status="FAIL",
                score_penalty=penalty,
                details={"errors": errors, "error_count": error_count},
                severity="MEDIUM",
            )

        except Exception as e:
            logger.error(f"Pandera validation error: {e}")
            return ValidatorResult(
                name=self.name,
                status="ERROR",
                score_penalty=0,
                details={"error": str(e)},
            )
