"""
Unit tests for tools/schema_loader.py

Tests cover: compare_schemas, generate_schema_diff_report,
map_pandera_to_sql_type, sql_type_to_pandera_type, and extract_pandera_schema_info.

All tests are pure-logic; no database connection is required.
"""
import pytest
from unittest.mock import MagicMock

from tools.schema_loader import (
    compare_schemas,
    generate_schema_diff_report,
    map_pandera_to_sql_type,
    sql_type_to_pandera_type,
    extract_pandera_schema_info,
)


# ---------------------------------------------------------------------------
# compare_schemas
# ---------------------------------------------------------------------------


class TestCompareSchemas:
    """Tests for compare_schemas(legacy_schema, modern_schema)."""

    def test_finds_missing_columns_and_type_mismatches(
        self, legacy_schema_dict, modern_schema_dict
    ):
        """Detects columns missing on each side, type mismatches, and common columns."""
        result = compare_schemas(legacy_schema_dict, modern_schema_dict)

        # Columns in legacy but not in modern
        assert set(result["missing_in_modern"]) == {
            "cl_recid", "cl_fnam", "cl_lnam", "cl_ssn",
            "cl_dob", "cl_phon", "cl_emal", "cl_stat",
            "cl_rgdt", "cl_dcsd",
        }

        # Columns in modern but not in legacy
        # Note: 'email' is in modern but not legacy (legacy has 'clmt_email')
        assert set(result["missing_in_legacy"]) == {
            "claimant_id", "first_name", "last_name", "ssn_hash",
            "date_of_birth", "phone_number", "email", "claimant_status",
            "registered_at",
        }

        # Common columns present in both schemas (no columns match anymore)
        assert set(result["common_columns"]) == set()

        # No type mismatches since no common columns
        assert result["type_mismatches"] == []

    def test_identical_schemas(self):
        """When both schemas are identical the diff should be empty."""
        schema = {"id": "integer", "name": "text", "active": "boolean"}
        result = compare_schemas(schema, schema)

        assert result["missing_in_modern"] == []
        assert result["missing_in_legacy"] == []
        assert result["type_mismatches"] == []
        assert set(result["common_columns"]) == {"id", "name", "active"}

    def test_empty_schemas(self):
        """Two empty schemas produce an entirely empty diff."""
        result = compare_schemas({}, {})

        assert result["missing_in_modern"] == []
        assert result["missing_in_legacy"] == []
        assert result["type_mismatches"] == []
        assert result["common_columns"] == []

    def test_completely_different_schemas(self):
        """Schemas with zero column overlap."""
        legacy = {"col_a": "integer", "col_b": "text"}
        modern = {"col_x": "boolean", "col_y": "bigint"}
        result = compare_schemas(legacy, modern)

        assert set(result["missing_in_modern"]) == {"col_a", "col_b"}
        assert set(result["missing_in_legacy"]) == {"col_x", "col_y"}
        assert result["type_mismatches"] == []
        assert result["common_columns"] == []


# ---------------------------------------------------------------------------
# generate_schema_diff_report
# ---------------------------------------------------------------------------


class TestGenerateSchemaDiffReport:
    """Tests for generate_schema_diff_report(legacy, modern, table_name)."""

    def test_report_includes_missing_columns_and_type_mismatches(
        self, legacy_schema_dict, modern_schema_dict
    ):
        """Report must mention every missing column."""
        report = generate_schema_diff_report(
            legacy_schema_dict, modern_schema_dict, "claimants"
        )

        # Missing in modern
        assert "cl_recid" in report
        assert "cl_fnam" in report
        assert "cl_ssn" in report
        assert "cl_rgdt" in report

        # New in modern
        assert "claimant_id" in report
        assert "first_name" in report
        assert "ssn_hash" in report
        assert "registered_at" in report

    def test_report_contains_markdown_formatting(
        self, legacy_schema_dict, modern_schema_dict
    ):
        """Report should use markdown headers and bold formatting."""
        report = generate_schema_diff_report(
            legacy_schema_dict, modern_schema_dict, "claimants"
        )

        # Top-level header with table name
        assert "# Schema Diff Report: claimants" in report

        # Section headers
        assert "## Columns Missing in Modern System" in report
        assert "## New Columns in Modern System" in report
        assert "## Summary" in report

        # Bold column names in bullet lists
        assert "**cl_fnam**" in report


# ---------------------------------------------------------------------------
# map_pandera_to_sql_type
# ---------------------------------------------------------------------------


class TestMapPanderaToSqlType:
    """Tests for map_pandera_to_sql_type(pandera_type)."""

    @pytest.mark.parametrize(
        "pandera_type, expected_sql",
        [
            ("int64", "bigint"),
            ("int32", "integer"),
            ("float64", "double precision"),
            ("float32", "real"),
            ("object", "text"),
            ("string", "text"),
            ("bool", "boolean"),
            ("datetime64[ns]", "timestamp"),
        ],
    )
    def test_known_mappings(self, pandera_type, expected_sql):
        """Every known Pandera type maps to the correct SQL type."""
        assert map_pandera_to_sql_type(pandera_type) == expected_sql

    def test_unknown_type_returns_unknown(self):
        """An unrecognised Pandera type should fall back to 'unknown'."""
        assert map_pandera_to_sql_type("complex128") == "unknown"
        assert map_pandera_to_sql_type("") == "unknown"
        assert map_pandera_to_sql_type("category") == "unknown"


# ---------------------------------------------------------------------------
# sql_type_to_pandera_type
# ---------------------------------------------------------------------------


class TestSqlTypeToPanderaType:
    """Tests for sql_type_to_pandera_type(sql_type)."""

    @pytest.mark.parametrize(
        "sql_type, expected_pandera",
        [
            ("integer", "int"),
            ("bigint", "int"),
            ("smallint", "int"),
            ("numeric", "float"),
            ("decimal", "float"),
            ("double precision", "float"),
            ("real", "float"),
            ("character varying", "str"),
            ("varchar", "str"),
            ("character", "str"),
            ("char", "str"),
            ("text", "str"),
            ("boolean", "bool"),
            ("timestamp without time zone", "'datetime64[ns]'"),
            ("timestamp with time zone", "'datetime64[ns]'"),
            ("date", "'datetime64[ns]'"),
            ("time", "str"),
            ("json", "str"),
            ("jsonb", "str"),
            ("uuid", "str"),
        ],
    )
    def test_known_sql_types(self, sql_type, expected_pandera):
        """Every recognised SQL type maps to the correct Pandera type."""
        assert sql_type_to_pandera_type(sql_type) == expected_pandera

    def test_unknown_sql_type_returns_str(self):
        """An unrecognised SQL type should default to 'str'."""
        assert sql_type_to_pandera_type("xml") == "str"
        assert sql_type_to_pandera_type("bytea") == "str"
        assert sql_type_to_pandera_type("custom_enum") == "str"

    def test_case_insensitive_lookup(self):
        """SQL types should be matched case-insensitively."""
        assert sql_type_to_pandera_type("INTEGER") == "int"
        assert sql_type_to_pandera_type("Boolean") == "bool"
        assert sql_type_to_pandera_type("TEXT") == "str"
        assert sql_type_to_pandera_type("BIGINT") == "int"
        assert sql_type_to_pandera_type("Character Varying") == "str"
        assert sql_type_to_pandera_type("TIMESTAMP WITHOUT TIME ZONE") == "'datetime64[ns]'"


# ---------------------------------------------------------------------------
# extract_pandera_schema_info
# ---------------------------------------------------------------------------


class TestExtractPanderaSchemaInfo:
    """Tests for extract_pandera_schema_info(schema)."""

    def test_extracts_columns_from_mock_schema(self):
        """Given a schema object with a columns dict, extract dtype and nullable."""
        # Build a lightweight mock that mirrors a Pandera DataFrameSchema
        col_id = MagicMock()
        col_id.dtype = "int64"
        col_id.nullable = False

        col_name = MagicMock()
        col_name.dtype = "object"
        col_name.nullable = True

        col_active = MagicMock()
        col_active.dtype = "bool"
        col_active.nullable = False

        mock_schema = MagicMock()
        mock_schema.columns = {
            "id": col_id,
            "name": col_name,
            "active": col_active,
        }

        result = extract_pandera_schema_info(mock_schema)

        assert "id" in result
        assert result["id"]["dtype"] == "int64"
        assert result["id"]["nullable"] is False

        assert "name" in result
        assert result["name"]["dtype"] == "object"
        assert result["name"]["nullable"] is True

        assert "active" in result
        assert result["active"]["dtype"] == "bool"
        assert result["active"]["nullable"] is False

    def test_returns_empty_dict_when_no_columns_attr(self):
        """If the schema object lacks a 'columns' attribute, return {}."""
        mock_schema = MagicMock(spec=[])  # spec=[] means no attributes at all
        result = extract_pandera_schema_info(mock_schema)
        assert result == {}

    def test_handles_columns_missing_dtype_and_nullable(self):
        """Columns without dtype or nullable attrs fall back to defaults."""
        col = MagicMock(spec=[])  # no dtype, no nullable

        mock_schema = MagicMock()
        mock_schema.columns = {"mystery_col": col}

        result = extract_pandera_schema_info(mock_schema)

        assert result["mystery_col"]["dtype"] == "unknown"
        assert result["mystery_col"]["nullable"] is True
