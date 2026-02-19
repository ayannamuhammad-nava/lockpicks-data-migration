"""
Unit tests for agents/post_agent.py

Covers: _load_column_mappings, _build_column_pairs, _values_equivalent,
        calculate_integrity_score.
"""
import json
import math

import pytest

from agents.post_agent import (
    _build_column_pairs,
    _load_column_mappings,
    _values_equivalent,
    calculate_integrity_score,
)


# ── Load Column Mappings ──


class TestLoadColumnMappings:
    """Tests for _load_column_mappings."""

    def test_loads_mappings_from_valid_file(self, tmp_path):
        """A well-formed mappings.json should return the 'mappings' list."""
        mappings_file = tmp_path / "mappings.json"
        mappings_data = {
            "mappings": [
                {"table": "users", "source": "uid", "target": "user_id", "type": "renamed"},
                {"table": "users", "source": "old_col", "target": "new_col", "type": "renamed"},
            ]
        }
        mappings_file.write_text(json.dumps(mappings_data))

        result = _load_column_mappings(str(mappings_file))

        assert len(result) == 2
        assert result[0]["source"] == "uid"
        assert result[1]["target"] == "new_col"

    def test_returns_empty_list_when_file_not_found(self, tmp_path):
        """When the mappings file does not exist, an empty list is returned."""
        missing_path = str(tmp_path / "nonexistent.json")

        result = _load_column_mappings(missing_path)

        assert result == []


# ── Build Column Pairs ──


class TestBuildColumnPairs:
    """Tests for _build_column_pairs."""

    def test_maps_via_mappings_and_adds_direct_matches(self):
        """Mapped columns appear first; unmapped columns shared by both sides are appended."""
        legacy_cols = ["uid", "name", "email"]
        modern_cols = ["user_id", "name", "email"]
        mappings = [
            {"table": "users", "source": "uid", "target": "user_id", "type": "renamed"},
        ]

        pairs = _build_column_pairs(legacy_cols, modern_cols, mappings, "users")

        # Mapped pair comes first
        assert pairs[0] == ("uid", "user_id")
        # Direct matches follow
        assert ("name", "name") in pairs
        assert ("email", "email") in pairs
        assert len(pairs) == 3

    def test_no_mappings_only_direct_matches(self):
        """With an empty mappings list, only columns present in both sides are paired."""
        legacy_cols = ["id", "name", "old_field"]
        modern_cols = ["id", "name", "new_field"]
        mappings = []

        pairs = _build_column_pairs(legacy_cols, modern_cols, mappings, "orders")

        assert pairs == [("id", "id"), ("name", "name")]

    def test_skips_removed_columns(self):
        """Mappings with type='removed' should be excluded from the pairs."""
        legacy_cols = ["uid", "deprecated_col", "name"]
        modern_cols = ["user_id", "name"]
        mappings = [
            {"table": "users", "source": "uid", "target": "user_id", "type": "renamed"},
            {"table": "users", "source": "deprecated_col", "target": None, "type": "removed"},
        ]

        pairs = _build_column_pairs(legacy_cols, modern_cols, mappings, "users")

        sources = [p[0] for p in pairs]
        assert "deprecated_col" not in sources
        assert ("uid", "user_id") in pairs
        assert ("name", "name") in pairs
        assert len(pairs) == 2


# ── Values Equivalent ──


class TestValuesEquivalent:
    """Tests for _values_equivalent."""

    def test_both_nan_returns_true(self):
        """Two NaN values should be considered equivalent."""
        assert _values_equivalent(float("nan"), float("nan")) is True

    def test_one_nan_returns_false(self):
        """When only one value is NaN, the comparison should return False."""
        assert _values_equivalent(float("nan"), 42) is False
        assert _values_equivalent("hello", float("nan")) is False

    def test_numeric_within_tolerance_returns_true(self):
        """Numeric values within abs tolerance of 0.001 should be equivalent."""
        assert _values_equivalent(1.0, 1.0005) is True
        assert _values_equivalent(100, 100.0009) is True

    def test_string_comparison_with_whitespace_returns_true(self):
        """String values that differ only by trailing whitespace should be equivalent."""
        assert _values_equivalent("hello", "hello ") is True
        assert _values_equivalent(" world ", " world") is True

    def test_different_values_returns_false(self):
        """Clearly different values should not be equivalent."""
        assert _values_equivalent("apple", "orange") is False
        assert _values_equivalent(1, 2) is False
        assert _values_equivalent(10.0, 10.5) is False


# ── Calculate Integrity Score ──


class TestCalculateIntegrityScore:
    """Tests for calculate_integrity_score."""

    @staticmethod
    def _perfect_results() -> dict:
        """Return a results dict that represents a perfect migration."""
        return {
            "row_count_check": {
                "legacy_count": 1000,
                "modern_count": 1000,
                "match": True,
                "difference": 0,
            },
            "integrity_results": {},
            "sample_comparison": {
                "sample_size": 100,
                "exact_matches": 100,
                "match_rate": 100,
            },
            "checksum_results": {
                "_summary": {
                    "total_columns": 10,
                    "matches": 10,
                    "mismatches": 0,
                }
            },
            "aggregate_validation": {},
        }

    def test_perfect_results_returns_100(self):
        """A fully matching migration should score 100."""
        results = self._perfect_results()

        score = calculate_integrity_score(results)

        assert score == 100.0

    def test_row_count_mismatch_penalty(self):
        """A row count mismatch should reduce the score by min(diff_pct, 30)."""
        results = self._perfect_results()
        # 50 missing out of 1000 = 5% difference -> penalty of 5
        results["row_count_check"] = {
            "legacy_count": 1000,
            "modern_count": 950,
            "match": False,
            "difference": 50,
        }

        score = calculate_integrity_score(results)

        assert score == 95.0

    def test_checksum_mismatch_penalty(self):
        """Checksum mismatches should penalise by min(mismatches * 3, 15)."""
        results = self._perfect_results()
        results["checksum_results"]["_summary"]["mismatches"] = 4

        score = calculate_integrity_score(results)

        # penalty = min(4 * 3, 15) = 12
        assert score == 88.0

    def test_aggregate_mismatch_penalty(self):
        """Each failed aggregate validation should deduct 10 points."""
        results = self._perfect_results()
        results["aggregate_validation"] = {
            "total_revenue": {"match": False},
            "claimant_count": {"match": False},
        }

        score = calculate_integrity_score(results)

        # penalty = 2 * 10 = 20
        assert score == 80.0

    def test_score_floors_at_zero(self):
        """Score should never go below 0 regardless of total penalties."""
        results = self._perfect_results()
        # Row count: 100% diff -> penalty capped at 30
        results["row_count_check"] = {
            "legacy_count": 1000,
            "modern_count": 0,
            "match": False,
            "difference": 1000,
        }
        # Sample match rate: 0% -> penalty = (100 - 0) * 0.3 = 30
        results["sample_comparison"]["match_rate"] = 0
        # Checksum: 5 mismatches -> penalty = min(15, 15) = 15
        results["checksum_results"]["_summary"]["mismatches"] = 5
        # Aggregates: 3 failures -> penalty = 30
        results["aggregate_validation"] = {
            "a": {"match": False},
            "b": {"match": False},
            "c": {"match": False},
        }

        score = calculate_integrity_score(results)

        # total penalties = 30 + 30 + 15 + 30 = 105, floored at 0
        assert score == 0
