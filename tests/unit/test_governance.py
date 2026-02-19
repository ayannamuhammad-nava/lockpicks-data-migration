"""
Unit tests for tools/governance.py

Covers: detect_pii_columns, check_naming_conventions, check_required_fields,
        check_null_thresholds, run_governance_checks, calculate_governance_score,
        generate_governance_report_csv.
"""
import pandas as pd
import pytest

from tools.governance import (
    calculate_governance_score,
    check_naming_conventions,
    check_null_thresholds,
    check_required_fields,
    detect_pii_columns,
    generate_governance_report_csv,
    run_governance_checks,
)


# ── PII Detection ──


class TestDetectPiiColumns:
    """Tests for detect_pii_columns."""

    def test_detects_ssn_and_email_in_claimant_data(self, sample_legacy_claimants_df):
        """Both 'clmt_ssn' and 'clmt_email' should be flagged when those keywords are supplied."""
        pii_keywords = ["ssn", "email"]
        result = detect_pii_columns(sample_legacy_claimants_df, pii_keywords)

        assert "clmt_ssn" in result
        assert "clmt_email" in result
        assert len(result) == 2

    def test_no_pii_in_clean_data(self, sample_claims_df):
        """A DataFrame with no PII-related column names should return an empty list."""
        pii_keywords = ["ssn", "passport", "credit_card"]
        result = detect_pii_columns(sample_claims_df, pii_keywords)

        assert result == []

    def test_case_insensitive_matching(self):
        """Keywords and column names should be matched case-insensitively."""
        df = pd.DataFrame({"SSN_Number": [1], "Passport_Id": [2], "name": [3]})
        pii_keywords = ["ssn", "passport"]
        result = detect_pii_columns(df, pii_keywords)

        assert "SSN_Number" in result
        assert "Passport_Id" in result
        assert "name" not in result


# ── Naming Conventions ──


class TestCheckNamingConventions:
    """Tests for check_naming_conventions."""

    def test_identifies_invalid_column_names(self, df_with_bad_column_names):
        """Columns that do not match snake_case regex should be listed as invalid."""
        columns = df_with_bad_column_names.columns.tolist()
        result = check_naming_conventions(columns, r"^[a-z0-9_]+$")

        assert set(result["invalid"]) == {"BadName", "also-bad", "ALLCAPS", "has spaces"}

    def test_identifies_valid_column_names(self, df_with_bad_column_names):
        """Columns that match the snake_case regex should be listed as valid."""
        columns = df_with_bad_column_names.columns.tolist()
        result = check_naming_conventions(columns, r"^[a-z0-9_]+$")

        assert set(result["valid"]) == {"good_name", "snake_case_123"}


# ── Required Fields ──


class TestCheckRequiredFields:
    """Tests for check_required_fields."""

    def test_all_required_fields_present(self, sample_legacy_claimants_df):
        """When every required field exists, all values should be True."""
        required = ["clmt_id", "clmt_email"]
        result = check_required_fields(sample_legacy_claimants_df, required)

        assert result == {"clmt_id": True, "clmt_email": True}

    def test_some_required_fields_missing(self, sample_legacy_claimants_df):
        """Missing columns should be reported as False."""
        required = ["clmt_id", "nonexistent_field", "another_missing"]
        result = check_required_fields(sample_legacy_claimants_df, required)

        assert result["clmt_id"] is True
        assert result["nonexistent_field"] is False
        assert result["another_missing"] is False

    def test_required_fields_on_empty_df(self, empty_df):
        """An empty DataFrame has no columns, so all required fields should be missing."""
        required = ["clmt_id", "clmt_email"]
        result = check_required_fields(empty_df, required)

        assert result == {"clmt_id": False, "clmt_email": False}


# ── Null Thresholds ──


class TestCheckNullThresholds:
    """Tests for check_null_thresholds."""

    def test_columns_exceeding_threshold(self, df_with_high_nulls):
        """'name' has 70% nulls and 'age' has 30% nulls -- both exceed 20%."""
        result = check_null_thresholds(df_with_high_nulls, max_null_percent=20.0)

        assert result["name"]["exceeds_threshold"] is True
        assert result["name"]["null_pct"] == 70.0
        assert result["age"]["exceeds_threshold"] is True
        assert result["age"]["null_pct"] == 30.0

    def test_columns_within_threshold(self, df_with_high_nulls):
        """'id' has 0% nulls and 'email' has 30% nulls; only 'id' is within 20%."""
        result = check_null_thresholds(df_with_high_nulls, max_null_percent=20.0)

        assert result["id"]["exceeds_threshold"] is False
        assert result["id"]["null_pct"] == 0.0

    def test_empty_df_returns_empty_results(self, empty_df):
        """An empty DataFrame (no columns) should produce an empty result dict."""
        result = check_null_thresholds(empty_df, max_null_percent=20.0)

        assert result == {}


# ── Full Governance Run ──


class TestRunGovernanceChecks:
    """Tests for run_governance_checks."""

    def test_config_integration(self, sample_legacy_claimants_df, governance_config):
        """run_governance_checks should aggregate all sub-check results and a score."""
        results = run_governance_checks(sample_legacy_claimants_df, governance_config)

        # Structural keys
        assert "pii_columns" in results
        assert "naming_check" in results
        assert "required_fields" in results
        assert "null_checks" in results
        assert "governance_score" in results

        # PII: 'clmt_ssn' matches keyword 'ssn'; 'clmt_dob' matches keyword 'dob'
        assert "clmt_ssn" in results["pii_columns"]
        assert "clmt_dob" in results["pii_columns"]

        # Required fields: both present
        assert results["required_fields"]["clmt_id"] is True
        assert results["required_fields"]["clmt_email"] is True

        # Score should be a number between 0 and 100
        assert 0 <= results["governance_score"] <= 100


# ── Score Calculation ──


class TestCalculateGovernanceScore:
    """Tests for calculate_governance_score."""

    def test_perfect_score(self):
        """No violations should yield a score of 100."""
        results = {
            "pii_columns": [],
            "naming_check": {"valid": ["col_a", "col_b"], "invalid": []},
            "required_fields": {"col_a": True, "col_b": True},
            "null_checks": {
                "col_a": {"null_pct": 0.0, "exceeds_threshold": False},
                "col_b": {"null_pct": 5.0, "exceeds_threshold": False},
            },
        }
        df = pd.DataFrame({"col_a": [1], "col_b": [2]})
        score = calculate_governance_score(results, df)

        assert score == 100

    def test_pii_penalty(self):
        """Each PII column should deduct 10 points."""
        results = {
            "pii_columns": ["clmt_ssn", "clmt_email"],
            "naming_check": {"valid": [], "invalid": []},
            "required_fields": {},
            "null_checks": {},
        }
        df = pd.DataFrame()
        score = calculate_governance_score(results, df)

        assert score == 80  # 100 - (2 * 10)

    def test_naming_penalty(self):
        """Each invalid column name should deduct 5 points."""
        results = {
            "pii_columns": [],
            "naming_check": {"valid": [], "invalid": ["BadName", "ALLCAPS"]},
            "required_fields": {},
            "null_checks": {},
        }
        df = pd.DataFrame()
        score = calculate_governance_score(results, df)

        assert score == 90  # 100 - (2 * 5)

    def test_missing_field_penalty(self):
        """Each missing required field should deduct 20 points."""
        results = {
            "pii_columns": [],
            "naming_check": {"valid": [], "invalid": []},
            "required_fields": {"clmt_id": False, "clmt_email": True},
            "null_checks": {},
        }
        df = pd.DataFrame()
        score = calculate_governance_score(results, df)

        assert score == 80  # 100 - (1 * 20)

    def test_null_penalty(self):
        """Each column exceeding the null threshold should deduct 5 points."""
        results = {
            "pii_columns": [],
            "naming_check": {"valid": [], "invalid": []},
            "required_fields": {},
            "null_checks": {
                "name": {"null_pct": 70.0, "exceeds_threshold": True},
                "age": {"null_pct": 30.0, "exceeds_threshold": True},
                "id": {"null_pct": 0.0, "exceeds_threshold": False},
            },
        }
        df = pd.DataFrame()
        score = calculate_governance_score(results, df)

        assert score == 90  # 100 - (2 * 5)

    def test_score_floors_at_zero(self):
        """Score should never go below 0 regardless of total penalties."""
        results = {
            "pii_columns": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"],
            "naming_check": {"valid": [], "invalid": []},
            "required_fields": {},
            "null_checks": {},
        }
        df = pd.DataFrame()
        score = calculate_governance_score(results, df)

        assert score == 0  # 100 - (11 * 10) = -10, floored to 0


# ── CSV Report Generation ──


class TestGenerateGovernanceReportCsv:
    """Tests for generate_governance_report_csv."""

    def test_report_csv_format(self):
        """The CSV report should contain a header and correctly formatted rows."""
        results = {
            "pii_columns": ["clmt_ssn"],
            "naming_check": {"valid": ["good"], "invalid": ["BadName"]},
            "required_fields": {"clmt_id": True, "clmt_email": False},
            "null_checks": {
                "name": {"null_pct": 70.0, "exceeds_threshold": True},
                "id": {"null_pct": 0.0, "exceeds_threshold": False},
            },
        }
        df = pd.DataFrame()

        csv_output = generate_governance_report_csv(results, df)

        lines = csv_output.strip().split("\n")

        # Header
        assert lines[0] == "category,item,status,details"

        # PII row
        assert "PII,clmt_ssn,VIOLATION,Contains PII keywords" in csv_output

        # Naming row
        assert "Naming,BadName,VIOLATION,Invalid naming convention" in csv_output

        # Required field rows
        assert "Required Field,clmt_id,PASS,Present" in csv_output
        assert "Required Field,clmt_email,VIOLATION,Missing" in csv_output

        # Null check rows
        assert "Null Check,name,VIOLATION,70.0% null" in csv_output
        assert "Null Check,id,PASS,0.0% null" in csv_output

        # Should have header + 6 data rows = 7 lines total
        assert len(lines) == 7
