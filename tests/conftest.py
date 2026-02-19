"""
Shared test fixtures for the Data Validation Agent test suite.
Scenario: State Department of Labor - Unemployment Insurance System
"""
import pytest
import pandas as pd
import numpy as np
import json
import os
from unittest.mock import MagicMock
from datetime import datetime


# ── Sample DataFrames ──


@pytest.fixture
def sample_legacy_claimants_df():
    """Mirrors the legacy ClaimantsSchema."""
    return pd.DataFrame({
        'cl_recid': [1, 2, 3, 4, 5],
        'cl_fnam': ['James', 'Mary', 'Robert', 'Patricia', '  John  '],
        'cl_lnam': ['Smith', 'Johnson', 'Williams', 'Brown', ' Davis '],
        'cl_ssn': ['123-45-6789', '987-65-4321', '123-45-6789', None, '555-66-7777'],
        'cl_dob': ['1980-01-15', '03/20/1975', None, 'January 10, 1988', '1990-05-01'],
        'cl_phon': ['217-555-1001', '(614) 555-1002', None, '317.555.1004', '704 555 1005'],
        'cl_emal': ['james@email.com', 'mary@email.com', None, 'patricia@email.com', None],
        'cl_adr1': ['123 Main St', '456 Oak Ave', '789 Pine St', '321 Elm St', '654 Cedar Ln'],
        'cl_city': ['Springfield', 'Columbus', 'Jacksonville', 'Indianapolis', 'Charlotte'],
        'cl_st': ['IL', 'OH', 'FL', 'IN', 'NC'],
        'cl_zip': ['62701', '43215', '32099', '46201', '28201'],
        'cl_bact': ['1234567890', '0987654321', '1111111111', '2222222222', '3333333333'],
        'cl_brtn': ['111000025', '111000026', '111000027', '111000028', '111000029'],
        'cl_stat': ['ACTIVE', 'active', 'Active', 'ACT', 'INACTIVE'],
        'cl_rgdt': ['2022-03-15 00:00:00', '2022-06-20 00:00:00', '2023-01-10 00:00:00', '2023-04-05 00:00:00', '2023-05-01 00:00:00'],
        'cl_dcsd': ['N', 'N', 'N', 'N', 'N'],
        'cl_fil1': ['', '', '', '', ''],
    })


@pytest.fixture
def sample_modern_claimants_df():
    """Mirrors the modern ClaimantsSchema (renamed columns, hashed SSN, phone as int)."""
    return pd.DataFrame({
        'claimant_id': [1, 2, 3, 4, 5],
        'first_name': ['James', 'Mary', 'Robert', 'Patricia', 'John'],
        'last_name': ['Smith', 'Johnson', 'Williams', 'Brown', 'Davis'],
        'ssn_hash': ['a1b2c3d4e5f6a7b8', 'c3d4e5f6a7b8c9d0', 'a1b2c3d4e5f6a7b8', None, 'i9j0k1l2m3n4o5p6'],
        'date_of_birth': pd.to_datetime(['1980-01-15', '1975-03-20', None, '1988-01-10', '1990-05-01']),
        'phone_number': [2175551001, 6145551002, 9045551003, 3175551004, 7045551005],
        'email': ['james@email.com', 'mary@email.com', None, 'patricia@email.com', None],
        'address_line1': ['123 Main St', '456 Oak Ave', '789 Pine St', '321 Elm St', '654 Cedar Ln'],
        'city': ['Springfield', 'Columbus', 'Jacksonville', 'Indianapolis', 'Charlotte'],
        'state': ['IL', 'OH', 'FL', 'IN', 'NC'],
        'zip_code': ['62701', '43215', '32099', '46201', '28201'],
        'claimant_status': ['active', 'active', 'active', 'active', 'inactive'],
        'registered_at': pd.to_datetime(['2022-03-15', '2022-06-20', '2023-01-10', '2023-04-05', '2023-05-01']),
        'is_deceased': [False, False, False, False, False],
    })


@pytest.fixture
def sample_claims_df():
    """DataFrame matching the legacy ClaimsSchema."""
    return pd.DataFrame({
        'cm_recid': [101, 102, 103, 104, 105],
        'cm_clmnt': [1, 2, 3, 1, 4],
        'cm_emplr': [1, 2, 3, 1, 4],
        'cm_seprs': ['Laid off - lack of work', 'Company closure', 'Seasonal layoff', 'Reduction in force', 'Contract ended'],
        'cm_fildt': ['2023-04-01', '2023-05-15', '2023-06-01', '2023-07-10', '2023-08-01'],
        'cm_wkamt': [450.00, 380.00, 275.00, 520.00, 325.50],
        'cm_mxamt': [11700.00, 9880.00, 7150.00, 13520.00, 8463.00],
        'cm_totpd': [5400.00, 3040.00, 0.00, 7800.00, 1627.50],
        'cm_wkcnt': [12, 8, 0, 15, 5],
        'cm_stat': ['ACTIVE', 'active', 'PENDING', 'EXHAUSTED', 'Active'],
        'cm_lupdt': ['2023-10-01 00:00:00', '2023-09-15 00:00:00', '2023-06-01 00:00:00', '2023-11-10 00:00:00', '2023-10-01 00:00:00'],
    })


@pytest.fixture
def df_with_high_nulls():
    """DataFrame with high null percentages for threshold testing."""
    return pd.DataFrame({
        'id': range(10),
        'name': ['Alice', None, None, None, 'Eve', None, None, None, None, 'Jane'],
        'email': ['a@b.com', 'b@b.com', None, 'd@b.com', None, 'f@b.com', None, 'h@b.com', 'i@b.com', 'j@b.com'],
        'age': [25, 30, None, 40, 45, None, None, 60, 65, 70],
    })


@pytest.fixture
def df_with_bad_column_names():
    """DataFrame with mixed naming conventions."""
    return pd.DataFrame({
        'good_name': [1],
        'BadName': [2],
        'also-bad': [3],
        'ALLCAPS': [4],
        'has spaces': [5],
        'snake_case_123': [6],
    })


@pytest.fixture
def empty_df():
    """Empty DataFrame for edge case testing."""
    return pd.DataFrame()


# ── Schema Dicts ──


@pytest.fixture
def legacy_schema_dict():
    """Simulates introspect_database_schema output for legacy claimants."""
    return {
        'cl_recid': 'integer',
        'cl_fnam': 'character varying',
        'cl_lnam': 'character varying',
        'cl_ssn': 'character varying',
        'cl_dob': 'character varying',
        'cl_phon': 'character varying',
        'cl_emal': 'character varying',
        'cl_stat': 'character varying',
        'cl_rgdt': 'character',
        'cl_dcsd': 'character',
    }


@pytest.fixture
def modern_schema_dict():
    """Simulates introspect_database_schema output for modern claimants."""
    return {
        'claimant_id': 'integer',
        'first_name': 'character varying',
        'last_name': 'character varying',
        'ssn_hash': 'character varying',
        'date_of_birth': 'date',
        'phone_number': 'bigint',
        'email': 'character varying',
        'claimant_status': 'character varying',
        'registered_at': 'timestamp without time zone',
        'is_deceased': 'boolean',
    }


# ── Mock Database Connections ──


@pytest.fixture
def mock_connection():
    """Returns a MagicMock that behaves like a psycopg2 connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    # Provide encoding for sql.Composed.as_string()
    conn.encoding = 'UTF8'
    return conn


@pytest.fixture
def mock_cursor(mock_connection):
    """Direct access to the cursor mock from mock_connection."""
    return mock_connection.cursor.return_value


# ── Configuration Fixtures ──


@pytest.fixture
def sample_config():
    """Returns a config dict matching config.yaml structure."""
    return {
        'database': {
            'legacy': {
                'host': 'localhost', 'port': 5432,
                'database': 'legacy_db', 'user': 'postgres', 'password': 'postgres'
            },
            'modern': {
                'host': 'localhost', 'port': 5432,
                'database': 'modern_db', 'user': 'postgres', 'password': 'postgres'
            },
        },
        'validation': {
            'sample_size': 1000,
            'governance': {
                'pii_keywords': ['ssn', 'passport', 'credit_card', 'dob', 'social_security', 'drivers_license'],
                'naming_regex': r'^[a-z0-9_]+$',
                'max_null_percent': 20,
                'required_fields': {
                    'claimants': ['cl_recid', 'cl_ssn', 'cl_emal'],
                    'claims': ['cm_recid', 'cm_clmnt', 'cm_fildt'],
                },
            },
        },
        'confidence': {
            'weights': {'structure': 0.4, 'integrity': 0.4, 'governance': 0.2},
            'thresholds': {'green': 90, 'yellow': 70},
        },
        'artifacts': {'base_path': './artifacts'},
    }


@pytest.fixture
def governance_config():
    """Governance-specific config subset."""
    return {
        'pii_keywords': ['ssn', 'passport', 'credit_card', 'dob'],
        'naming_regex': r'^[a-z0-9_]+$',
        'max_null_percent': 20.0,
        'required_fields': ['cl_recid', 'cl_emal'],
    }


# ── Temporary Directories ──


@pytest.fixture
def tmp_artifact_dir(tmp_path):
    """Provides a temporary directory for artifact output."""
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    return str(artifact_dir)


@pytest.fixture
def tmp_metadata_dir(tmp_path):
    """Provides a temp directory with sample glossary.json and mappings.json."""
    meta_dir = tmp_path / "metadata"
    meta_dir.mkdir()

    glossary = {
        "columns": [
            {"name": "cl_recid", "description": "Unique identifier for claimant",
             "system": "legacy", "pii": False, "confidence": 0.9, "table": "claimants"},
            {"name": "cl_emal", "description": "Email address for communication",
             "system": "legacy", "pii": True, "confidence": 0.9, "table": "claimants"},
            {"name": "cl_ssn", "description": "Social Security Number - highly sensitive personal identifier",
             "system": "legacy", "pii": True, "confidence": 1.0, "table": "claimants"},
            {"name": "cl_fnam", "description": "Claimant first name in legacy system",
             "system": "legacy", "pii": False, "confidence": 0.7, "table": "claimants"},
            {"name": "first_name", "description": "Claimant's first name in modern system",
             "system": "modern", "pii": False, "confidence": 0.85, "table": "claimants"},
        ]
    }
    (meta_dir / "glossary.json").write_text(json.dumps(glossary))

    mappings = {
        "mappings": [
            {"source": "cl_fnam", "target": "first_name",
             "rationale": "Renamed for clarity and consistency",
             "confidence": 0.8, "table": "claimants", "type": "rename"},
            {"source": "cl_rgdt", "target": "registered_at",
             "rationale": "Standardized timestamp naming to use _at suffix",
             "confidence": 0.91, "table": "claimants", "type": "rename"},
        ]
    }
    (meta_dir / "mappings.json").write_text(json.dumps(mappings))

    return str(meta_dir)


