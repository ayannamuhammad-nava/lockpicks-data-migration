"""
Unit tests for tools/metadata_generator.py

Covers: detect_pii, infer_description, _semantic_types_compatible,
        find_column_mapping, generate_mapping_rationale.

All tests exercise pure functions directly -- no database connections required.
"""
import pytest

from tools.metadata_generator import (
    detect_pii,
    find_column_mapping,
    generate_mapping_rationale,
    infer_description,
    _semantic_types_compatible,
)


# ── PII Detection ──


class TestDetectPii:
    """Tests for detect_pii."""

    def test_detects_common_pii_keywords(self):
        """Columns containing ssn, email, phone, dob, credit_card, address,
        or zip should all be flagged as PII."""
        assert detect_pii("ssn") is True
        assert detect_pii("claimant_email") is True
        assert detect_pii("home_phone") is True
        assert detect_pii("clmt_dob") is True
        assert detect_pii("credit_card_number") is True
        assert detect_pii("mailing_address") is True
        assert detect_pii("zip_code") is True

    def test_returns_false_for_clean_column_names(self):
        """Ordinary column names that do not contain PII keywords should not
        be flagged."""
        assert detect_pii("claimant_id") is False
        assert detect_pii("status") is False
        assert detect_pii("created_at") is False
        assert detect_pii("weeks_claimed") is False
        assert detect_pii("is_active") is False

    def test_case_insensitive_matching(self):
        """PII detection should be case-insensitive so that upper-case or
        mixed-case column names are still caught."""
        assert detect_pii("SSN") is True
        assert detect_pii("Email") is True
        assert detect_pii("PHONE_NUMBER") is True
        assert detect_pii("Date_Of_Birth") is True

    def test_detects_cobol_pii_names(self):
        """COBOL-style abbreviated column names containing PII keywords
        should still be flagged."""
        assert detect_pii("cl_ssn") is True
        assert detect_pii("cl_emal") is True
        assert detect_pii("cl_phon") is True
        assert detect_pii("cl_dob") is True
        assert detect_pii("cl_bact") is True
        assert detect_pii("cl_brtn") is True
        assert detect_pii("er_ein") is True


# ── Description Inference ──


class TestInferDescription:
    """Tests for infer_description."""

    def test_ssn_columns_return_high_confidence(self):
        """SSN and clmt_ssn should return the Social Security Number
        description with 0.95 confidence."""
        desc, conf = infer_description("ssn", "varchar", False)
        assert "Social Security Number" in desc
        assert conf == 0.95

        desc2, conf2 = infer_description("clmt_ssn", "varchar", False)
        assert "Social Security Number" in desc2
        assert conf2 == 0.95

    def test_id_columns_return_identifier_description(self):
        """Columns ending in _id should return a 'Unique identifier'
        description with 0.9 confidence."""
        desc, conf = infer_description("claimant_id", "integer", False)
        assert "Unique identifier" in desc
        assert "claimant" in desc
        assert conf == 0.9

    def test_email_and_phone_columns_detected(self):
        """Columns containing 'email' or 'phone' should be recognised with
        0.9 confidence."""
        desc_email, conf_email = infer_description("user_email", "varchar", True)
        assert "Email address" in desc_email
        assert conf_email == 0.9

        desc_phone, conf_phone = infer_description("work_phone", "varchar", True)
        assert "Phone number" in desc_phone
        assert conf_phone == 0.9

    def test_status_columns(self):
        """Columns containing 'status' should be inferred with 0.75
        confidence."""
        desc, conf = infer_description("claim_status", "varchar", True)
        assert "status" in desc.lower()
        assert conf == 0.75

    def test_cobol_ssn_column(self):
        """COBOL-style cl_ssn should return Social Security description
        with 0.95 confidence."""
        desc, conf = infer_description("cl_ssn", "char", False)
        assert "Social Security" in desc
        assert conf == 0.95

    def test_cobol_bank_account(self):
        """COBOL-style cl_bact should return a Bank-related description
        with confidence >= 0.8."""
        desc, conf = infer_description("cl_bact", "char", False)
        assert "Bank" in desc
        assert conf >= 0.8

    def test_unknown_column_returns_low_confidence(self):
        """A column with an unrecognised name and a non-standard type should
        fall back to the generic description with 0.3 confidence."""
        desc, conf = infer_description("xyzzy_flag", "jsonb", True)
        assert conf == 0.3
        # Generic fallback produces a Title Case field description
        assert "field" in desc.lower()


# ── Semantic Type Compatibility ──


class TestSemanticTypesCompatible:
    """Tests for _semantic_types_compatible."""

    def test_same_semantic_types_are_compatible(self):
        """Two columns of the same semantic type (temporal-temporal,
        identifier-identifier) should be compatible."""
        # temporal - temporal
        assert _semantic_types_compatible("created_at", "updated_at") is True
        # identifier - identifier
        assert _semantic_types_compatible("claim_id", "claimant_id") is True
        # name - name
        assert _semantic_types_compatible("first_name", "last_name") is True

    def test_different_semantic_types_are_incompatible(self):
        """Columns with different semantic types should NOT be compatible."""
        # temporal vs identifier
        assert _semantic_types_compatible("created_at", "claim_id") is False
        # status vs location
        assert _semantic_types_compatible("claim_status", "home_city") is False
        # amount vs identifier
        assert _semantic_types_compatible("total_amount", "claim_id") is False

    def test_generic_is_always_compatible(self):
        """A column that resolves to 'generic' (no recognised suffix) should
        be compatible with any other type."""
        # generic source against temporal target
        assert _semantic_types_compatible("foo_bar", "created_at") is True
        # temporal source against generic target
        assert _semantic_types_compatible("created_at", "foo_bar") is True
        # both generic
        assert _semantic_types_compatible("alpha", "beta") is True


# ── Column Mapping ──


class TestFindColumnMapping:
    """Tests for find_column_mapping."""

    def test_exact_match(self):
        """An exact column name match should return confidence 1.0."""
        targets = ["claim_id", "claimant_name", "status"]
        result = find_column_mapping("claim_id", targets)

        assert result is not None
        matched_col, confidence = result
        assert matched_col == "claim_id"
        assert confidence == 1.0

    def test_nm_to_name_matching(self):
        """Legacy '_nm' suffix should map to modern '_name' suffix with high
        confidence (>= 0.85)."""
        targets = ["first_name", "last_name", "claim_id"]
        result = find_column_mapping("clmt_first_nm", targets)

        assert result is not None
        matched_col, confidence = result
        assert matched_col == "first_name"
        assert confidence >= 0.85

    def test_abbreviation_expansion(self):
        """Common abbreviations (pymt -> payment, etc.) should be expanded
        and matched against fully-spelled target columns."""
        targets = ["payment_id", "claimant_name", "benefit_amount"]
        result = find_column_mapping("pymt_id", targets)

        assert result is not None
        matched_col, confidence = result
        assert matched_col == "payment_id"
        assert confidence >= 0.75

    def test_cobol_direct_mapping(self):
        """COBOL-style cl_fnam should map to first_name with confidence
        >= 0.85."""
        targets = ["first_name", "last_name", "claim_id"]
        result = find_column_mapping("cl_fnam", targets)

        assert result is not None
        matched_col, confidence = result
        assert matched_col == "first_name"
        assert confidence >= 0.85

    def test_cobol_recid_to_id(self):
        """COBOL-style cl_recid should map to claimant_id with confidence
        >= 0.75."""
        targets = ["claimant_id", "employer_id"]
        result = find_column_mapping("cl_recid", targets)

        assert result is not None
        matched_col, confidence = result
        assert matched_col == "claimant_id"
        assert confidence >= 0.75

    def test_no_match_returns_none(self):
        """When no target column is similar enough, the function should return
        None."""
        targets = ["claim_id", "claimant_name", "status"]
        result = find_column_mapping("totally_unrelated_xyz", targets)

        assert result is None

    def test_semantic_type_prevents_bad_match(self):
        """clmt_state (location) should NOT match claimant_status (status)
        even if the string similarity is high, because their semantic types
        are incompatible."""
        targets = ["claimant_status", "claimant_id"]
        result = find_column_mapping("clmt_state", targets)

        # Should either return None or match something other than
        # claimant_status -- never map a location to a status.
        if result is not None:
            matched_col, _ = result
            assert matched_col != "claimant_status"


# ── Mapping Rationale ──


class TestGenerateMappingRationale:
    """Tests for generate_mapping_rationale."""

    def test_type_change_rationale(self):
        """Same column name with different types should mention the type
        change."""
        rationale = generate_mapping_rationale(
            "amount", "amount", "numeric", "decimal"
        )
        assert "numeric" in rationale.lower() or "decimal" in rationale.lower()
        assert "type" in rationale.lower() or "changed" in rationale.lower()
