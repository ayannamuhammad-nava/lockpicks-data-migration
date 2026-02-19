"""
Unit tests for tools/reporter.py

Covers artifact folder creation, file-saving helpers, formatting utilities,
and Markdown report generators (readiness report, dashboard, reconciliation report).
"""
import json
import os
from datetime import datetime

import pytest

from tools.reporter import (
    create_artifact_folder,
    format_confidence_score,
    generate_readiness_dashboard,
    generate_readiness_report,
    generate_reconciliation_report,
    save_confidence_score,
    save_csv_report,
    save_json_log,
    save_markdown_report,
    save_run_metadata,
)


# ---------------------------------------------------------------------------
# 1. create_artifact_folder
# ---------------------------------------------------------------------------

class TestCreateArtifactFolder:

    def test_creates_directory_and_returns_path_with_run_prefix(self, tmp_artifact_dir):
        """create_artifact_folder creates a directory whose name contains 'run_'."""
        path = create_artifact_folder(base_path=tmp_artifact_dir)

        assert os.path.isdir(path), "Artifact folder should exist on disk"
        assert "run_" in os.path.basename(path), (
            "Folder name should contain the 'run_' prefix"
        )


# ---------------------------------------------------------------------------
# 2. save_markdown_report
# ---------------------------------------------------------------------------

class TestSaveMarkdownReport:

    def test_writes_content_to_file(self, tmp_artifact_dir):
        """save_markdown_report writes the supplied content verbatim."""
        file_path = os.path.join(tmp_artifact_dir, "report.md")
        content = "# Test Report\n\nHello, world!"

        save_markdown_report(content, file_path)

        with open(file_path, "r") as f:
            assert f.read() == content


# ---------------------------------------------------------------------------
# 3. save_csv_report
# ---------------------------------------------------------------------------

class TestSaveCsvReport:

    def test_writes_csv_content_to_file(self, tmp_artifact_dir):
        """save_csv_report writes the supplied CSV string to the target file."""
        file_path = os.path.join(tmp_artifact_dir, "data.csv")
        content = "col_a,col_b\n1,2\n3,4\n"

        save_csv_report(content, file_path)

        with open(file_path, "r") as f:
            assert f.read() == content


# ---------------------------------------------------------------------------
# 4. save_json_log
# ---------------------------------------------------------------------------

class TestSaveJsonLog:

    def test_serializes_dict_and_handles_datetime(self, tmp_artifact_dir):
        """save_json_log writes JSON with indent=2 and converts datetime via default=str."""
        file_path = os.path.join(tmp_artifact_dir, "log.json")
        now = datetime(2026, 2, 15, 10, 30, 0)
        data = {"event": "migration_start", "timestamp": now, "count": 42}

        save_json_log(data, file_path)

        with open(file_path, "r") as f:
            loaded = json.load(f)

        assert loaded["event"] == "migration_start"
        assert loaded["count"] == 42
        # datetime should have been serialised via str()
        assert "2026" in loaded["timestamp"]


# ---------------------------------------------------------------------------
# 5. format_confidence_score
# ---------------------------------------------------------------------------

class TestFormatConfidenceScore:

    @pytest.mark.parametrize(
        "status, expected_emoji",
        [
            ("GREEN", "\U0001f7e2"),   # green circle
            ("YELLOW", "\U0001f7e1"),  # yellow circle
            ("RED", "\U0001f534"),     # red circle
            ("UNKNOWN", "\u26aa"),     # white circle (fallback)
        ],
    )
    def test_returns_correct_emoji_for_status(self, status, expected_emoji):
        """Each known status maps to the right traffic-light emoji; unknown -> white."""
        result = format_confidence_score(95, status)

        assert result.startswith(expected_emoji)
        assert "95/100" in result
        assert status in result


# ---------------------------------------------------------------------------
# 6. generate_readiness_report
# ---------------------------------------------------------------------------

class TestGenerateReadinessReport:

    def test_contains_schema_diff_governance_and_recommendations(self):
        """Report includes schema diff sections, governance findings, and recommendations."""
        schema_diff = {
            "missing_in_modern": ["ssn", "created_date"],
            "type_mismatches": [
                {"column": "phone", "legacy_type": "varchar", "modern_type": "bigint"}
            ],
        }
        governance_results = {
            "pii_columns": ["ssn", "email"],
            "naming_check": {"invalid": ["BadName", "has spaces"]},
        }
        rag_explanations = {
            "ssn": "SSN is no longer stored in the modern system for compliance.",
            "phone": "Phone was converted to bigint to support E.164 format.",
        }
        score = 72

        report = generate_readiness_report(
            schema_diff, governance_results, rag_explanations, score
        )

        # Schema diff sections
        assert "Schema Analysis" in report
        assert "Missing in Modern" in report
        assert "ssn" in report
        assert "Type Mismatches" in report
        assert "phone" in report

        # RAG explanations embedded
        assert "SSN is no longer stored" in report
        assert "E.164" in report

        # Governance findings
        assert "Governance Findings" in report
        assert "Sensitive Data Detection" in report
        assert "Naming" in report
        assert "BadName" in report

        # Recommendations section
        assert "Recommendations" in report
        assert "Schema Mapping" in report
        assert "Data Protection" in report
        assert "Standards Compliance" in report


# ---------------------------------------------------------------------------
# 7 & 8. generate_readiness_dashboard
# ---------------------------------------------------------------------------

class TestGenerateReadinessDashboard:

    _base_schema_diff = {
        "missing_in_modern": ["ssn"],
        "type_mismatches": [
            {"column": "phone", "legacy_type": "varchar", "modern_type": "bigint"}
        ],
    }
    _base_governance = {"pii_columns": ["ssn"]}

    @pytest.mark.parametrize(
        "overall_score, expected_status_fragment",
        [
            (95, "GREEN"),
            (80, "YELLOW"),
            (50, "RED"),
        ],
    )
    def test_traffic_light_status_thresholds(self, overall_score, expected_status_fragment):
        """Dashboard reflects GREEN (>=90), YELLOW (>=70), RED (<70) status."""
        dashboard = generate_readiness_dashboard(
            schema_diff=self._base_schema_diff,
            governance_results=self._base_governance,
            structure_score=85,
            governance_score=80,
            overall_score=overall_score,
        )

        assert expected_status_fragment in dashboard

    def test_contains_key_metrics_risks_and_recommendation(self):
        """Dashboard includes the key-metrics table, top risks, and go/no-go section."""
        dashboard = generate_readiness_dashboard(
            schema_diff=self._base_schema_diff,
            governance_results=self._base_governance,
            structure_score=85,
            governance_score=75,
            overall_score=80,
        )

        # Key Metrics table
        assert "Key Metrics" in dashboard
        assert "Schema Compatibility" in dashboard
        assert "Data Governance" in dashboard
        assert "Overall Confidence" in dashboard

        # Top Risks
        assert "Top 3 Risks" in dashboard
        assert "schema mismatches" in dashboard
        assert "PII fields detected" in dashboard
        assert "data type changes" in dashboard

        # Go/No-Go Recommendation
        assert "Go/No-Go Recommendation" in dashboard


# ---------------------------------------------------------------------------
# 9 & 10. generate_reconciliation_report
# ---------------------------------------------------------------------------

class TestGenerateReconciliationReport:

    _base_row_count = {"legacy_count": 10000, "modern_count": 10000, "match": True}
    _base_checksums = {"claimants": {"match": True}, "claims": {"match": False}}
    _base_integrity = {
        "claims_claimant_fk": {"orphan_count": 0},
        "payments_claim_fk": {"orphan_count": 2, "orphan_sample": [101, 105]},
    }
    _base_sample = {"sample_size": 100, "exact_matches": 98, "discrepancies": 2}

    def test_includes_all_reconciliation_sections(self):
        """Report contains row count, checksums, integrity, and sample comparison."""
        report = generate_reconciliation_report(
            row_count_check=self._base_row_count,
            checksum_results=self._base_checksums,
            integrity_results=self._base_integrity,
            sample_comparison=self._base_sample,
            score=85,
        )

        # Row count verification
        assert "Row Count Verification" in report
        assert "10,000" in report
        assert "Match" in report

        # Checksums
        assert "Data Checksums" in report
        assert "claimants" in report
        assert "claims" in report

        # Referential integrity
        assert "Referential Integrity" in report
        assert "claims_claimant_fk" in report
        assert "payments_claim_fk" in report
        assert "2 orphans" in report

        # Sample comparison
        assert "Sample Comparison" in report
        assert "100" in report
        assert "98" in report
        assert "2" in report

    @pytest.mark.parametrize(
        "score, expected_label",
        [
            (95, "SUCCESS"),
            (80, "REVIEW REQUIRED"),
            (60, "FAILURE"),
        ],
    )
    def test_migration_status_based_on_score(self, score, expected_label):
        """Migration status is SUCCESS (>=90), REVIEW REQUIRED (>=70), or FAILURE (<70)."""
        report = generate_reconciliation_report(
            row_count_check=self._base_row_count,
            checksum_results=self._base_checksums,
            integrity_results={"fk_check": {"orphan_count": 0}},
            sample_comparison=self._base_sample,
            score=score,
        )

        assert expected_label in report


# ---------------------------------------------------------------------------
# 13. save_confidence_score
# ---------------------------------------------------------------------------

class TestSaveConfidenceScore:

    def test_writes_file_with_score_and_status(self, tmp_artifact_dir):
        """save_confidence_score writes a file that contains both the score and status."""
        file_path = os.path.join(tmp_artifact_dir, "confidence.txt")

        save_confidence_score(88, "GREEN", file_path)

        with open(file_path, "r") as f:
            content = f.read()

        assert "88/100" in content
        assert "GREEN" in content
        assert "Score: 88/100" in content
        assert "Status: GREEN" in content
        assert "Timestamp:" in content


# ---------------------------------------------------------------------------
# 14. save_run_metadata
# ---------------------------------------------------------------------------

class TestSaveRunMetadata:

    def test_adds_generated_at_field(self, tmp_artifact_dir):
        """save_run_metadata injects a generated_at timestamp into the metadata."""
        file_path = os.path.join(tmp_artifact_dir, "metadata.json")
        metadata = {"run_id": "abc-123", "tables": ["claimants", "claims"]}

        save_run_metadata(metadata, file_path)

        with open(file_path, "r") as f:
            loaded = json.load(f)

        assert "generated_at" in loaded
        # The value should be an ISO-format timestamp string
        datetime.fromisoformat(loaded["generated_at"])
        # Original fields are preserved
        assert loaded["run_id"] == "abc-123"
        assert loaded["tables"] == ["claimants", "claims"]
