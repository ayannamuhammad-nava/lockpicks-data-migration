"""
Pre-Migration Validation Agent
Detects structural and governance risks before migration begins.
"""
import json
import logging
from pathlib import Path
from typing import Dict
from tools import db_utils, sampler, schema_loader, governance, rag_tool
import pandera as pa

logger = logging.getLogger(__name__)


def run_pre_migration_validation(
    legacy_conn,
    modern_conn,
    dataset: str,
    sample_size: int,
    config: Dict
) -> Dict:
    """
    Run pre-migration validation checks.

    Args:
        legacy_conn: Legacy database connection
        modern_conn: Modern database connection
        dataset: Dataset name (e.g., 'claimants')
        sample_size: Number of rows to sample for validation
        config: Configuration dict

    Returns:
        Dict with validation results and structure score
    """
    logger.info(f"Starting pre-migration validation for {dataset}")

    results = {
        'dataset': dataset,
        'sample_size': sample_size
    }

    # 1. Schema comparison
    logger.info("Comparing schemas...")
    legacy_schema_db = schema_loader.introspect_database_schema(legacy_conn, dataset)
    modern_schema_db = schema_loader.introspect_database_schema(modern_conn, dataset)

    schema_diff = schema_loader.compare_schemas(legacy_schema_db, modern_schema_db)
    results['schema_diff'] = schema_diff

    # 2. Generate schema diff report
    schema_diff_report = schema_loader.generate_schema_diff_report(
        legacy_schema_db,
        modern_schema_db,
        dataset
    )
    results['schema_diff_report'] = schema_diff_report

    # 3. Sample data from legacy system
    logger.info(f"Sampling {sample_size} rows from legacy {dataset}...")
    sample_df = sampler.sample_table(legacy_conn, dataset, sample_size)
    results['sample_row_count'] = len(sample_df)

    # 4. Validate with Pandera schema (if available)
    logger.info("Validating with Pandera schema...")
    try:
        legacy_pandera_schema = schema_loader.load_pandera_schema('legacy', dataset)

        if legacy_pandera_schema:
            try:
                legacy_pandera_schema.validate(sample_df, lazy=True)
                results['pandera_validation'] = 'PASS'
                results['pandera_errors'] = []
            except pa.errors.SchemaErrors as e:
                results['pandera_validation'] = 'FAIL'
                results['pandera_errors'] = [str(err) for err in e.failure_cases.to_dict('records')]
                logger.warning(f"Pandera validation failed: {len(e.failure_cases)} errors")
        else:
            results['pandera_validation'] = 'SKIP'
            results['pandera_errors'] = []
    except Exception as e:
        logger.error(f"Pandera validation error: {e}")
        results['pandera_validation'] = 'ERROR'
        results['pandera_errors'] = [str(e)]

    # 5. Governance checks
    logger.info("Running governance checks...")
    gov_config = config.get('validation', {}).get('governance', {})

    # Get required fields for this dataset
    dataset_required_fields = gov_config.get('required_fields', {}).get(dataset, [])
    gov_config_for_dataset = {
        **gov_config,
        'required_fields': dataset_required_fields
    }

    gov_results = governance.run_governance_checks(sample_df, gov_config_for_dataset)
    results['governance'] = gov_results

    # 5b. Cross-field data quality anomalies
    logger.info("Checking cross-field data quality anomalies...")
    data_anomalies = check_data_quality_anomalies(sample_df, dataset)
    results['data_anomalies'] = data_anomalies
    if data_anomalies:
        logger.warning(f"Found {len(data_anomalies)} data quality anomaly type(s) in legacy sample")

    # 6. RAG explanations for schema differences
    logger.info("Generating RAG explanations...")
    rag_instance = rag_tool.RAGTool()
    rag_explanations = rag_instance.enrich_schema_diff(schema_diff)
    results['rag_explanations'] = rag_explanations

    # 7. Calculate structure score
    structure_score = calculate_structure_score(schema_diff, gov_results, results, dataset=dataset)
    results['structure_score'] = structure_score

    logger.info(f"Pre-migration validation complete. Structure score: {structure_score}/100")

    return results


def check_data_quality_anomalies(sample_df, dataset: str) -> list:
    """
    Run dataset-specific cross-field integrity checks on the legacy sample.

    Returns a list of anomaly dicts, each with:
      rule, severity, description, detail, count, record_ids, risk, action
    """
    anomalies = []

    if dataset == 'claimants':
        # Deceased claimant still carrying an active status
        if 'cl_dcsd' in sample_df.columns and 'cl_stat' in sample_df.columns:
            deceased_mask = sample_df['cl_dcsd'].astype(str).str.strip() == 'Y'
            active_mask = sample_df['cl_stat'].astype(str).str.strip().str.upper().isin(['ACTIVE', 'ACT'])
            bad_rows = sample_df[deceased_mask & active_mask]
            if not bad_rows.empty:
                ids = bad_rows['cl_recid'].tolist() if 'cl_recid' in bad_rows.columns else []
                anomalies.append({
                    'rule': 'deceased_active',
                    'severity': 'HIGH',
                    'description': 'Deceased claimant with active benefit status',
                    'detail': "cl_dcsd = 'Y'  AND  cl_stat IN ('ACTIVE', 'ACT')",
                    'count': len(bad_rows),
                    'record_ids': ids[:5],
                    'risk': (
                        'Active benefits may be disbursed to deceased claimants — '
                        'potential fraud exposure and audit finding. '
                        'Migrating these records as-is carries the anomaly into production.'
                    ),
                    'action': (
                        'Close or suspend affected records before migration. '
                        'Refer to benefits fraud review team.'
                    ),
                })

    return anomalies


def _load_column_mapping_types(dataset: str) -> Dict[str, str]:
    """
    Load mappings.json and return {source_col: mapping_type} for the given table.
    Used by calculate_structure_score to distinguish renamed/archived/removed columns.
    """
    mappings_path = Path(__file__).parent.parent / "metadata" / "mappings.json"
    if not mappings_path.exists():
        return {}
    try:
        data = json.loads(mappings_path.read_text())
        return {
            m["source"].lower(): m.get("type", "removed")
            for m in data.get("mappings", [])
            if m.get("table", "").lower() == dataset.lower()
        }
    except Exception:
        return {}


def calculate_structure_score(
    schema_diff: Dict,
    gov_results: Dict,
    validation_results: Dict,
    dataset: str = ""
) -> float:
    """
    Calculate structure/readiness score (0-100).

    Penalises columns based on knowledge-base coverage:
      - rename / transform  → 0  (ETL path is documented)
      - archived            → 1  (intentionally excluded, compliance process known)
      - removed / unknown   → 4  (no modern equivalent, ETL must investigate)

    Governance is handled separately in the confidence formula and is NOT
    double-counted here.

    Args:
        schema_diff: Schema comparison results
        gov_results: Governance check results
        validation_results: All validation results
        dataset: Table name used to load the correct column mappings

    Returns:
        Score from 0-100
    """
    penalties = 0.0

    # Load knowledge-base mapping types for this table
    mapping_types = _load_column_mapping_types(dataset) if dataset else {}

    # Per-column penalty based on knowledge-base coverage
    PENALTY = {
        "rename":    0,   # known rename — ETL is straightforward
        "transform": 0,   # known transform — ETL logic documented
        "archived":  1,   # intentionally excluded for compliance — low risk
        "removed":   4,   # no modern equivalent — ETL must investigate
    }

    for col in schema_diff.get('missing_in_modern', []):
        col_type = mapping_types.get(col.lower(), "removed")
        penalties += PENALTY.get(col_type, 4)

    # Type mismatch penalty (independent of mapping coverage)
    type_mismatches = len(schema_diff.get('type_mismatches', []))
    penalties += type_mismatches * 5

    # Pandera validation penalties
    if validation_results.get('pandera_validation') == 'FAIL':
        error_count = len(validation_results.get('pandera_errors', []))
        penalties += min(error_count, 20)  # Max 20 points penalty

    # Calculate final score
    score = max(0, 100 - penalties)

    return round(score, 2)
