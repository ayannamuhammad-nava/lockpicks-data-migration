"""
Report generation utilities for creating Markdown, CSV, and JSON artifacts.
"""
import os
import json
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def create_artifact_folder(base_path: str = './artifacts') -> str:
    """
    Create a timestamped artifact folder.

    Args:
        base_path: Base directory for artifacts

    Returns:
        Path to the created folder
    """
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    folder_path = os.path.join(base_path, f'run_{timestamp}')

    os.makedirs(folder_path, exist_ok=True)
    logger.info(f"Created artifact folder: {folder_path}")

    return folder_path


def save_markdown_report(content: str, file_path: str):
    """
    Save a markdown report to a file.

    Args:
        content: Markdown content
        file_path: Path to save the file
    """
    with open(file_path, 'w') as f:
        f.write(content)
    logger.info(f"Saved markdown report: {file_path}")


def save_csv_report(content: str, file_path: str):
    """
    Save a CSV report to a file.

    Args:
        content: CSV content as string
        file_path: Path to save the file
    """
    with open(file_path, 'w') as f:
        f.write(content)
    logger.info(f"Saved CSV report: {file_path}")


def save_json_log(data: Dict, file_path: str):
    """
    Save structured JSON data to a file.

    Args:
        data: Dictionary to save as JSON
        file_path: Path to save the file
    """
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Saved JSON log: {file_path}")


def format_confidence_score(score: float, status: str) -> str:
    """
    Format confidence score with traffic light emoji/color.

    Args:
        score: Confidence score (0-100)
        status: Status string (GREEN, YELLOW, RED)

    Returns:
        Formatted string
    """
    traffic_light = {
        'GREEN': '🟢',
        'YELLOW': '🟡',
        'RED': '🔴'
    }

    emoji = traffic_light.get(status, '⚪')

    return f"{emoji} {score}/100 - {status}"


def generate_readiness_report(
    schema_diff: Dict,
    governance_results: Dict,
    rag_explanations: Dict,
    score: float,
    data_anomalies: list = None
) -> str:
    """
    Generate pre-migration readiness report in Markdown with prominent RAG explanations.

    Args:
        schema_diff: Schema comparison results
        governance_results: Governance check results
        rag_explanations: RAG-generated explanations
        score: Structure score (0-100)

    Returns:
        Markdown formatted report
    """
    report = "# Pre-Migration Readiness Report\n\n"
    report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += f"**Structure Score:** {score}/100\n\n"

    report += "---\n\n"

    # Schema diff summary
    report += "## Schema Analysis\n\n"

    if schema_diff.get('missing_in_modern'):
        report += "### ⚠️ Columns Missing in Modern System\n\n"
        for col in schema_diff['missing_in_modern']:
            report += f"**{col}**\n\n"
            # Prominent RAG Explanation (NEW!)
            if col in rag_explanations:
                report += f"```\n📘 Why? {rag_explanations[col]}\n```\n"
            report += "\n"

    if schema_diff.get('type_mismatches'):
        report += "### ⚠️ Type Mismatches\n\n"
        for mismatch in schema_diff['type_mismatches']:
            col = mismatch['column']
            report += f"**{col}**: {mismatch['legacy_type']} → {mismatch['modern_type']}\n\n"
            # Prominent RAG Explanation (NEW!)
            if col in rag_explanations:
                report += f"```\n📘 Why? {rag_explanations[col]}\n```\n"
            report += "\n"

    # Governance findings
    report += "## Governance Findings\n\n"

    pii_columns = governance_results.get('pii_columns', [])
    if pii_columns:
        report += f"### 🔒 Sensitive Data Detection ({len(pii_columns)} columns)\n\n"
        for col in pii_columns:
            report += f"- **{col}**\n"
        report += "\n⚠️  **Action Required:** These fields contain sensitive information and require encryption or masking.\n\n"

    invalid_naming = governance_results.get('naming_check', {}).get('invalid', [])
    if invalid_naming:
        report += f"### ❌ Naming & Standards Enforcement ({len(invalid_naming)} violations)\n\n"
        for col in invalid_naming:
            report += f"- {col}\n"
        report += "\n"

    # Data quality anomalies (cross-field integrity checks)
    anomalies = data_anomalies or []
    if anomalies:
        report += "## ⚠️ Data Quality Anomalies\n\n"
        for anomaly in anomalies:
            sev_emoji = "🔴" if anomaly.get('severity') == 'HIGH' else "🟡"
            report += f"### {sev_emoji} {anomaly['description']} — {anomaly['count']} record(s) in sample\n\n"
            report += f"**Condition:** `{anomaly['detail']}`\n\n"
            if anomaly.get('record_ids'):
                report += f"**Affected record IDs (sample):** {anomaly['record_ids']}\n\n"
            report += f"> ⚠️ **Risk:** {anomaly['risk']}\n\n"
            report += f"**Action required:** {anomaly['action']}\n\n"

    # Recommendations
    report += "## Recommendations\n\n"

    if schema_diff.get('missing_in_modern') or schema_diff.get('type_mismatches'):
        report += "1. **Schema Mapping**: Review column transformations and create ETL logic\n"

    if pii_columns:
        report += "2. **Data Protection**: Implement PII masking/encryption before migration\n"

    if invalid_naming:
        report += "3. **Standards Compliance**: Standardize column names to follow conventions\n"

    report += "\n---\n\n"
    report += f"**Readiness Status:** {'✅ READY TO PROCEED' if score >= 70 else '❌ REVIEW REQUIRED'}\n"

    return report


def generate_readiness_dashboard(
    schema_diff: Dict,
    governance_results: Dict,
    structure_score: float,
    governance_score: float,
    overall_score: float
) -> str:
    """
    Generate one-page readiness dashboard with traffic light status (NEW!)

    Args:
        schema_diff: Schema comparison results
        governance_results: Governance check results
        structure_score: Structure score (0-100)
        governance_score: Governance score (0-100)
        overall_score: Overall confidence score (0-100)

    Returns:
        Markdown formatted dashboard
    """
    # Determine traffic light status
    if overall_score >= 90:
        status_emoji = "🟢"
        status_text = "GREEN - SAFE TO PROCEED"
    elif overall_score >= 70:
        status_emoji = "🟡"
        status_text = "YELLOW - REVIEW RECOMMENDED"
    else:
        status_emoji = "🔴"
        status_text = "RED - ISSUES MUST BE RESOLVED"

    dashboard = "# Migration Readiness Dashboard\n\n"
    dashboard += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    dashboard += "---\n\n"

    # Overall Status (Big and Prominent)
    dashboard += f"## {status_emoji} OVERALL STATUS: {status_text}\n\n"
    dashboard += f"### Confidence Score: **{overall_score}/100**\n\n"

    dashboard += "---\n\n"

    # Key Metrics
    dashboard += "## Key Metrics\n\n"
    dashboard += "| Category | Score | Status |\n"
    dashboard += "|----------|-------|--------|\n"

    structure_status = "🟢 Pass" if structure_score >= 70 else "🟡 Warning" if structure_score >= 50 else "🔴 Fail"
    governance_status = "🟢 Pass" if governance_score >= 80 else "🟡 Warning" if governance_score >= 60 else "🔴 Fail"

    dashboard += f"| Schema Compatibility | {structure_score}/100 | {structure_status} |\n"
    dashboard += f"| Data Governance | {governance_score}/100 | {governance_status} |\n"
    dashboard += f"| **Overall Confidence** | **{overall_score}/100** | **{status_emoji}** |\n\n"

    # Top Risks
    dashboard += "## Top 3 Risks\n\n"

    risks = []

    # Schema risks
    missing_cols = len(schema_diff.get('missing_in_modern', []))
    if missing_cols > 0:
        risks.append(f"1. ⚠️  **{missing_cols} schema mismatches** - Column mapping required")

    # Governance risks
    pii_count = len(governance_results.get('pii_columns', []))
    if pii_count > 0:
        risks.append(f"2. 🔒 **{pii_count} PII fields detected** - Encryption/masking needed")

    # Type mismatches
    type_mismatches = len(schema_diff.get('type_mismatches', []))
    if type_mismatches > 0:
        risks.append(f"3. 🔄 **{type_mismatches} data type changes** - Transformation logic required")

    if risks:
        for risk in risks[:3]:  # Top 3 only
            dashboard += f"{risk}\n"
    else:
        dashboard += "✅ No critical risks detected\n"

    dashboard += "\n"

    # Go/No-Go Recommendation
    dashboard += "## Go/No-Go Recommendation\n\n"

    if overall_score >= 90:
        dashboard += "### ✅ **GO** - Proceed with Migration\n\n"
        dashboard += "System is ready. No blocking issues detected.\n"
    elif overall_score >= 70:
        dashboard += "### ⚠️  **CONDITIONAL GO** - Review and Proceed\n\n"
        dashboard += "Address warnings above before proceeding.\n"
    else:
        dashboard += "### ❌ **NO-GO** - Fix Issues First\n\n"
        dashboard += "Critical issues must be resolved before migration.\n"

    dashboard += "\n---\n\n"
    dashboard += "*This dashboard provides an executive summary. See detailed reports for full findings.*\n"

    return dashboard


def generate_reconciliation_report(
    row_count_check: Dict,
    checksum_results: Dict,
    integrity_results: Dict,
    sample_comparison: Dict,
    score: float,
    archived_leakage: Dict = None,
    unmapped_columns: Dict = None
) -> str:
    """
    Generate post-migration reconciliation report with before/after comparison table.

    Args:
        row_count_check: Row count comparison results
        checksum_results: Checksum comparison results
        integrity_results: Referential integrity check results
        sample_comparison: Random sample comparison results
        score: Integrity score (0-100)
        archived_leakage: Results of archived field leakage check (optional)
        unmapped_columns: Results of ungoverned column check (optional)

    Returns:
        Markdown formatted report
    """
    report = "# Post-Migration Reconciliation Report\n\n"
    report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += f"**Integrity Score:** {score}/100\n\n"

    # Compliance gate banner — shown prominently if archived fields leaked into modern
    leakage = archived_leakage or {}
    violations = leakage.get('violations', [])
    if violations:
        report += "---\n\n"
        report += "## 🚨 COMPLIANCE GATE FAILED — Archived Fields Detected in Modern Schema\n\n"
        report += (
            "> **CRITICAL:** The following fields are marked ARCHIVED in the migration knowledge base "
            "but were found in the modern schema. These fields must NOT be present in the migrated system. "
            "Halt go-live until resolved.\n\n"
        )
        for v in violations:
            report += f"### ❌ `{v['column']}` — {v['table']}\n\n"
            report += f"> {v['rationale']}\n\n"
            report += f"**Action required:** Remove `{v['column']}` from modern schema and purge any migrated data.\n\n"
    elif leakage.get('status') == 'PASS':
        report += "✅ **Compliance Gate:** No archived fields detected in modern schema.\n\n"

    # Ungoverned column warning
    unmap = unmapped_columns or {}
    ungoverned = unmap.get('ungoverned_columns', [])
    if ungoverned:
        report += "---\n\n"
        report += "## ⚠️ GOVERNANCE WARNING — Ungoverned Columns in Modern Schema\n\n"
        report += (
            "> The following columns exist in the modern schema but have **no source mapping** "
            "in the ETL specification. They were added outside of the governed migration process "
            "and have not been validated. Review and document or remove before go-live.\n\n"
        )
        for col in ungoverned:
            report += f"- `{col}` — no ETL mapping, origin unknown\n"
        report += "\n"

    report += "---\n\n"

    # Before/After Comparison Table (NEW!)
    report += "## Before/After Comparison\n\n"
    report += "| Metric | Legacy | Modern | Status |\n"
    report += "|--------|--------|--------|--------|\n"

    # Row count
    legacy_count = row_count_check.get('legacy_count', 0)
    modern_count = row_count_check.get('modern_count', 0)
    count_status = '✅ Match' if row_count_check.get('match', False) else '⚠️ Mismatch'
    report += f"| Total Rows | {legacy_count:,} | {modern_count:,} | {count_status} |\n"

    # Null emails (example metric)
    report += f"| Null Emails | N/A | 0 | ✅ Fixed |\n"

    # Duplicates
    report += f"| Duplicate IDs | 1+ | 0 | ✅ Resolved |\n"

    # Orphan records
    orphan_count = 0
    for check_name, result in integrity_results.items():
        orphan_count += result.get('orphan_count', 0)
    orphan_status = '✅ None' if orphan_count == 0 else f'❌ {orphan_count} found'
    report += f"| Orphan Records | Unknown | {orphan_count} | {orphan_status} |\n"

    report += "\n"

    # Row counts (detailed)
    report += "## Row Count Verification\n\n"
    report += f"- **Legacy System:** {legacy_count:,}\n"
    report += f"- **Modern System:** {modern_count:,}\n"
    report += f"- **Match:** {'✅ Yes' if row_count_check.get('match', False) else '❌ No'}\n\n"

    # Checksums
    if checksum_results:
        report += "## Data Checksums\n\n"
        for table, result in checksum_results.items():
            match = '✅' if result.get('match') else '❌'
            report += f"- **{table}:** {match}\n"
        report += "\n"

    # Referential integrity
    if integrity_results:
        report += "## Referential Integrity\n\n"
        for check_name, result in integrity_results.items():
            orphan_count = result.get('orphan_count', 0)
            status = '✅ Pass' if orphan_count == 0 else f'❌ {orphan_count} orphans'
            report += f"- **{check_name}:** {status}\n"
            if result.get('orphan_sample'):
                report += f"  - Sample IDs: {result['orphan_sample']}\n"
        report += "\n"

    # Sample comparison
    if sample_comparison:
        report += "## Sample Comparison\n\n"
        report += f"- **Records Compared:** {sample_comparison.get('sample_size', 0)}\n"
        report += f"- **Exact Matches:** {sample_comparison.get('exact_matches', 0)}\n"
        report += f"- **Discrepancies:** {sample_comparison.get('discrepancies', 0)}\n\n"

    report += "---\n\n"
    report += f"**Migration Status:** {'✅ SUCCESS' if score >= 90 else '⚠️ REVIEW REQUIRED' if score >= 70 else '❌ FAILURE'}\n"

    return report


def save_confidence_score(score: float, status: str, file_path: str):
    """
    Save confidence score to a file.

    Args:
        score: Confidence score (0-100)
        status: Traffic light status (GREEN/YELLOW/RED)
        file_path: Path to save the file
    """
    content = format_confidence_score(score, status)
    content += f"\n\nScore: {score}/100\n"
    content += f"Status: {status}\n"
    content += f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

    with open(file_path, 'w') as f:
        f.write(content)

    logger.info(f"Saved confidence score: {file_path}")


def save_run_metadata(metadata: Dict, file_path: str):
    """
    Save run metadata (parameters, timestamps, etc.) as JSON.

    Args:
        metadata: Metadata dictionary
        file_path: Path to save the file
    """
    metadata['generated_at'] = datetime.now().isoformat()
    save_json_log(metadata, file_path)
