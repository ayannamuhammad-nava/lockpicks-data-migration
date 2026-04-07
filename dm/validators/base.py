"""
Base classes for all DM validators.

Every validation check — built-in or plugin-provided — implements either
PreValidator or PostValidator and returns a ValidatorResult.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pandas as pd


@dataclass
class ValidatorResult:
    """Standard result returned by every validator."""

    name: str
    status: str  # PASS | FAIL | WARN | SKIP | ERROR
    score_penalty: float  # Points to deduct from 100
    details: Dict = field(default_factory=dict)
    severity: str = "INFO"  # INFO | LOW | MEDIUM | HIGH | CRITICAL


class PreValidator(ABC):
    """Base class for pre-migration validation checks."""

    @property
    def name(self) -> str:
        """Human-readable name for this validator."""
        return self.__class__.__name__

    @abstractmethod
    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        sample_df: pd.DataFrame,
        config: Dict,
    ) -> ValidatorResult:
        """Execute this pre-migration check.

        Args:
            legacy_conn: Connector to legacy database.
            modern_conn: Connector to modern database.
            dataset: Table/dataset name being validated.
            sample_df: Sample data from the legacy system.
            config: Full project configuration dict.

        Returns:
            A ValidatorResult with status, penalty, and details.
        """


class PostValidator(ABC):
    """Base class for post-migration validation checks."""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def run(
        self,
        legacy_conn: Any,
        modern_conn: Any,
        dataset: str,
        config: Dict,
    ) -> ValidatorResult:
        """Execute this post-migration check.

        Args:
            legacy_conn: Connector to legacy database.
            modern_conn: Connector to modern database.
            dataset: Table/dataset name being validated.
            config: Full project configuration dict.

        Returns:
            A ValidatorResult with status, penalty, and details.
        """
